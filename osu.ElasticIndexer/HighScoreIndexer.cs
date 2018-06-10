// Copyright (c) 2007-2018 ppy Pty Ltd <contact@ppy.sh>.
// Licensed under the MIT Licence - https://raw.githubusercontent.com/ppy/osu-elastic-indexer/master/LICENCE

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using MySql.Data.MySqlClient;
using Nest;

namespace osu.ElasticIndexer
{
    public class HighScoreIndexer<T> : IIndexer where T : Model
    {
        public string Name { get; set; }
        public long? ResumeFrom { get; set; }
        public string Suffix { get; set; }

        private readonly ElasticClient elasticClient;

        private string indexName;

        public HighScoreIndexer()
        {
            elasticClient = new ElasticClient
            (
                new ConnectionSettings(new Uri(AppSettings.ElasticsearchHost))
            );
        }

        public void Run()
        {
            indexName = findOrCreateIndex(Name);

            // find out if we should be resuming
            var resumeFrom = ResumeFrom ?? IndexMeta.GetByName(indexName)?.LastId;

            Console.WriteLine();
            Console.WriteLine($"{typeof(T)}, index `{indexName}`, chunkSize `{AppSettings.ChunkSize}`, resume `{resumeFrom}`");
            Console.WriteLine();

            var start = DateTime.Now;

            using (var dbConnection = new MySqlConnection(AppSettings.ConnectionString))
            {
                dbConnection.Open();

                // TODO: retry needs to be added on timeout
                var chunks = Model.Chunk<T>(dbConnection, AppSettings.ChunkSize, resumeFrom);

                Parallel.ForEach(chunks, chunk =>
                {
                    consumeChunk(chunk);

                    var count = chunk.Count;
                    var timeTaken = DateTime.Now - start;
                    Console.WriteLine($"{count} records took {timeTaken}");
                    if (count > 0) Console.WriteLine($"{count / timeTaken.TotalSeconds} records/s");
                });
            }

            updateAlias(Name, indexName);
        }

        private void consumeChunk(List<T> chunk)
        {
            while (true)
            {
                try
                {
                    var bulkDescriptor = new BulkDescriptor().Index(indexName).IndexMany(chunk);

                    var result = elasticClient.Bulk(bulkDescriptor);

                    throwOnFailure(result);

                    // TODO: Less blind-fire update.
                    // I feel like this is in the wrong place...
                    IndexMeta.Update(new IndexMeta
                    {
                        Index = indexName,
                        Alias = Name,
                        LastId = chunk.Last().CursorValue,
                        UpdatedAt = DateTimeOffset.UtcNow
                    });
                }
                catch (Exception e)
                {
                    Console.WriteLine($"retrying chunk with lastId {chunk.Last().CursorValue} (failed with: {e})");
                    Thread.Sleep(10000);
                    continue;
                }

                break;
            }
        }

        /// <summary>
        /// Attemps to find the matching index or creates a new one.
        /// </summary>
        /// <param name="name">Name of the alias to find the matching index for.</param>
        /// <returns>Name of index found or created.</returns>
        private string findOrCreateIndex(string name)
        {
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine($"Find or create index for `{name}`...");
            var metas = IndexMeta.GetByAlias(name).ToList();
            var indices = elasticClient.GetIndicesPointingToAlias(name);

            string index = metas.FirstOrDefault(m => indices.Contains(m.Index))?.Index;
            // 3 cases are handled:
            // 1. Index was already aliased and has tracking information; likely resuming from a completed job.
            if (index != null)
            {
                Console.WriteLine($"Found matching aliased index `{index}`.");
                return index;
            }

            // 2. Index has not been aliased and has tracking information; likely resuming from an imcomplete job.
            index = metas.FirstOrDefault()?.Index;
            if (index != null)
            {
                Console.WriteLine($"Found previous index `{index}`.");
                return index;
            }

            // 3. Not aliased and no tracking information; likely starting from scratch
            var suffix = Suffix ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
            index = $"{name}_{suffix}";

            Console.WriteLine($"Creating `{index}` for `{name}`.");
            // create by supplying the json file instead of the attributed class because we're not
            // mapping every field but still want everything for _source.
            var json = File.ReadAllText(Path.GetFullPath("schemas/high_scores.json"));
            elasticClient.LowLevel.IndicesCreate<DynamicResponse>(index, json);

            return index;

            // TODO: cases not covered should throw an Exception (aliased but not tracked, etc).
        }

        private void throwOnFailure(IBulkResponse response)
        {
            if (response.ItemsWithErrors.Any(item => item.Status == 429))
                throw new Exception("Server returned 429");
        }

        private void updateAlias(string alias, string index)
        {
            Console.WriteLine($"Updating `{alias}` alias to `{index}`...");

            var aliasDescriptor = new BulkAliasDescriptor();
            var oldIndices = elasticClient.GetIndicesPointingToAlias(alias);

            foreach (var oldIndex in oldIndices)
                aliasDescriptor.Remove(d => d.Alias(alias).Index(oldIndex));

            aliasDescriptor.Add(d => d.Alias(alias).Index(index));

            Console.WriteLine(elasticClient.Alias(aliasDescriptor));

            // TODO: cleanup unaliased indices.
        }
    }
}
