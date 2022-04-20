// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Elasticsearch.Net;
using Elasticsearch.Net.Specification.IndicesApi;
using MySqlConnector;
using Nest;
using StatsdClient;

namespace osu.ElasticIndexer
{
    public class SoloScoreIndexer : IIndexer
    {
        class Metadata
        {
            public bool IsAliased { get; set; }

            public long LastId { get; set; }
            public string RealName { get; set; }
            public long? ResetQueueTo { get; set; }
            public bool Ready { get; set; }
            public string Schema { get; set; }
            public DateTimeOffset? UpdatedAt { get; set; }

            public Metadata(string indexName, string schema)
            {
                RealName = indexName;
                Schema = schema;
            }

            public Metadata(IndexName indexName, IndexState indexState)
            {
                RealName = indexName.Name;

                var meta = indexState.Mappings.Meta;
                LastId = Convert.ToInt64(meta["last_id"]);
                ResetQueueTo = meta.ContainsKey("reset_queue_to") ? Convert.ToInt64(meta["reset_queue_to"]) : null;
                Schema = (string) meta["schema"];
                Ready = (bool) meta["ready"];
                UpdatedAt = meta.ContainsKey("updated_at") ? DateTimeOffset.Parse((string) meta["updated_at"]) : null;
            }

            public void Save()
            {
                AppSettings.ELASTIC_CLIENT.Map<SoloScoreIndexer>(mappings => mappings.Meta(
                    m => m
                        .Add("last_id", LastId)
                        .Add("reset_queue_to", ResetQueueTo)
                        .Add("schema", Schema)
                        .Add("ready", Ready)
                        .Add("updated_at", DateTimeOffset.UtcNow)
                ).Index(RealName));
            }

            // TODO: should probably create whole object
            private void UpdateWith(IndexState indexState)
            {
                var meta = indexState.Mappings.Meta;
                LastId = Convert.ToInt64(meta["last_id"]);
                ResetQueueTo = meta.ContainsKey("reset_queue_to") ? Convert.ToInt64(meta["reset_queue_to"]) : null;
                Schema = (string) meta["schema"];
                Ready = (bool) meta["ready"];
                UpdatedAt = meta.ContainsKey("updated_at") ? DateTimeOffset.Parse((string) meta["updated_at"]) : null;
            }
        }

        public event EventHandler<IndexCompletedArgs> IndexCompleted = delegate { };

        public string Name { get; set; }
        public long? ResumeFrom { get; set; }
        public string Suffix { get; set; }

        // use shared instance to avoid socket leakage.
        private readonly ElasticClient elasticClient = AppSettings.ELASTIC_CLIENT;

        private BulkIndexingDispatcher<SoloScore> dispatcher;

        private Metadata metadata;

        public void Run()
        {
            if (!checkIfReady()) return;

            this.metadata = findOrCreateIndex(Name);

            checkIndexState();

            var indexCompletedArgs = new IndexCompletedArgs
            {
                Alias = Name,
                Index = metadata.RealName,
                StartedAt = DateTime.Now
            };

            dispatcher = new BulkIndexingDispatcher<SoloScore>(metadata.RealName);

            if (AppSettings.IsRebuild)
                dispatcher.BatchWithLastIdCompleted += handleBatchWithLastIdCompleted;

            try
            {
                var readerTask = databaseReaderTask(metadata.LastId);
                dispatcher.Run();
                readerTask.Wait();

                indexCompletedArgs.Count = readerTask.Result;
                indexCompletedArgs.CompletedAt = DateTime.Now;

                // when preparing for schema changes, the alias update
                // should be done by process waiting for the ready signal.
                if (AppSettings.IsRebuild)
                    if (AppSettings.IsPrepMode) {
                        metadata.Ready = true;
                        metadata.Save();
                    }
                    else
                        updateAlias(Name, metadata.RealName);

                IndexCompleted(this, indexCompletedArgs);
            }
            catch (AggregateException ae)
            {
                ae.Handle(handleAggregateException);
            }

            // Local function exception handler.
            bool handleAggregateException(Exception ex)
            {
                if (!(ex is InvalidOperationException)) return false;

                Console.Error.WriteLine(ex.Message);
                if (ex.InnerException != null)
                    Console.Error.WriteLine(ex.InnerException.Message);

                return true;
            }

            void handleBatchWithLastIdCompleted(object sender, long lastId)
            {
                metadata.LastId = lastId;
                // metadata.UpdatedAt = DateTimeOffset.UtcNow;
                metadata.Save();
            }
        }

        /// <summary>
        /// Checks if the indexer should wait for the next pass or continue.
        /// </summary>
        /// <returns>true if ready; false, otherwise.</returns>
        private bool checkIfReady()
        {
            if (AppSettings.IsRebuild || getIndicesForCurrentVersion(Name).Count > 0)
                return true;

            Console.WriteLine($"`{Name}` for version {AppSettings.Schema} is not ready...");
            return false;
        }

        /// <summary>
        /// Self contained database reader task. Reads the database by cursoring through records
        /// and adding chunks into readBuffer.
        /// </summary>
        /// <param name="resumeFrom">The cursor value to resume from;
        /// use null to resume from the last known value.</param>
        /// <returns>The database reader task.</returns>
        private Task<long> databaseReaderTask(long resumeFrom) => Task.Factory.StartNew(() =>
            {
                long count = 0;

                while (true)
                {
                    try
                    {
                        Console.WriteLine($"Rebuild from {resumeFrom}...");
                        var chunks = Model.Chunk<SoloScore>(AppSettings.ChunkSize, resumeFrom);
                        foreach (var chunk in chunks)
                        {
                            // var scores = SoloScore.FetchByScoreIds(chunk.Select(x => x.score_id).AsList());
                            var scores = chunk.Where(x => x.ShouldIndex).AsList();
                            // TODO: investigate fetching country directly in scores query.
                            var users = User.FetchUserMappings(scores);
                            foreach (var score in scores)
                            {
                                score.country_code = users[score.UserId].country_acronym;
                            }

                            dispatcher.Enqueue(scores);
                            count += chunk.Count;
                            // update resumeFrom in this scope to allow resuming from connection errors.
                            resumeFrom = chunk.Last().CursorValue;
                        }

                        break;
                    }
                    catch (DbException ex)
                    {
                        Console.Error.WriteLine(ex.Message);
                        Task.Delay(1000).Wait();
                    }
                }

                dispatcher.EnqueueEnd();
                Console.WriteLine($"Finished reading database {count} records.");

                return count;
            }, TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach);

        /// <summary>
        /// Attempts to find the matching index or creates a new one.
        /// </summary>
        /// <param name="name">name of the index alias.</param>
        /// <returns>Name of index found or created and any existing alias.</returns>
        private Metadata findOrCreateIndex(string name)
        {
            Console.WriteLine();

            var indices = getIndicesForCurrentVersion(name);

            if (indices.Count > 0 && !AppSettings.IsNew)
            {
                // 3 cases are handled:
                // 1. Index was already aliased and has tracking information; likely resuming from a completed job.
                var (indexName, indexState) = indices.FirstOrDefault(entry => entry.Value.Aliases.ContainsKey(name));
                if (indexName != null)
                {
                    Console.WriteLine($"Using aliased `{indexName}`.");

                    return new Metadata(indexName, indexState) { IsAliased = true };
                }

                // 2. Index has not been aliased and has tracking information;
                // likely resuming from an incomplete job or waiting to switch over.
                // TODO: throw if there's more than one? or take lastest one.
                (indexName, indexState) = indices.First();
                Console.WriteLine($"Using non-aliased `{indexName}`.");

                return new Metadata(indexName, indexState);
            }

            if (indices.Count == 0 && AppSettings.IsWatching)
                throw new Exception("no existing index found");

            // 3. Not aliased and no tracking information; likely starting from scratch
            var suffix = Suffix ?? DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss");
            var index = $"{name}_{suffix}";

            Console.WriteLine($"Creating `{index}` for `{name}`.");

            // create by supplying the json file instead of the attributed class because we're not
            // mapping every field but still want everything for _source.
            var json = File.ReadAllText(Path.GetFullPath("schemas/solo_scores.json"));
            elasticClient.LowLevel.Indices.Create<DynamicResponse>(
                index,
                json,
                new CreateIndexRequestParameters() { WaitForActiveShards = "all" }
            );
            var metadata = new Metadata(index, AppSettings.Schema);
            metadata.Save();

            return metadata;

            // TODO: cases not covered should throw an Exception (aliased but not tracked, etc).
        }

        private void checkIndexState()
        {
            // TODO: all this needs cleaning.
            if (!AppSettings.IsRebuild && !metadata.IsAliased)
                updateAlias(Name, metadata.RealName);

            metadata.LastId = ResumeFrom ?? metadata.LastId;

            if (metadata.Schema != AppSettings.Schema)
                // A switchover is probably happening, so signal that this mode should be skipped.
                throw new VersionMismatchException($"`{Name}` found version {metadata.Schema}, expecting {AppSettings.Schema}");

            if (AppSettings.IsRebuild)
            {
                // Save the position the score processing queue should be reset to if rebuilding an index.
                // If there is already an existing value, the processor is probabaly resuming from a previous run,
                // so we likely want to preserve that value.
                if (!metadata.ResetQueueTo.HasValue)
                {
                    // TODO: set ResetQueueTo to first unprocessed item.
                }
            }
            else
            {
                // process queue reset if any.
                if (metadata.ResetQueueTo.HasValue)
                {
                    Console.WriteLine($"Resetting queue_id > {metadata.ResetQueueTo}");
                    // TODO: reset queue to metadata.ResetQueueTo.Value
                    metadata.ResetQueueTo = null;
                }
            }
        }

        private List<KeyValuePair<IndexName, IndexState>> getIndicesForCurrentVersion(string name)
        {
            return elasticClient.Indices.Get($"{name}_*").Indices
                .Where(entry => (string) entry.Value.Mappings.Meta?["schema"] == AppSettings.Schema)
                .ToList();
        }

        private Dictionary<uint, dynamic> getUsers(IEnumerable<uint> userIds)
        {
            // get users
            using (var dbConnection = new MySqlConnection(AppSettings.ConnectionString))
            {
                dbConnection.Open();
                return dbConnection.Query<dynamic>($"select user_id, country_acronym from phpbb_users where user_id in @userIds", new { userIds })
                    .ToDictionary(u => (uint) u.user_id);
            }
        }

        private void updateAlias(string alias, string index, bool close = true)
        {
            // TODO: updating alias should mark the index as ready since it's switching over.
            Console.WriteLine($"Updating `{alias}` alias to `{index}`...");

            var aliasDescriptor = new BulkAliasDescriptor();
            var oldIndices = elasticClient.GetIndicesPointingToAlias(alias);

            foreach (var oldIndex in oldIndices)
                aliasDescriptor.Remove(d => d.Alias(alias).Index(oldIndex));

            aliasDescriptor.Add(d => d.Alias(alias).Index(index));
            elasticClient.Indices.BulkAlias(aliasDescriptor);

            // cleanup
            if (!close) return;
            foreach (var toClose in oldIndices.Where(x => x != index))
            {
                Console.WriteLine($"Closing {toClose}");
                elasticClient.Indices.Close(toClose);
            }
        }
    }
}
