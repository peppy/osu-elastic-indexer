// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Threading;
using System.Threading.Tasks;
using Nest;

namespace osu.ElasticIndexer
{
    public class SoloScoreIndexer
    {
        // TODO: maybe have a fixed name?
        public string Name { get; set; } = IndexHelper.INDEX_NAME;
        public long? ResumeFrom { get; set; }

        // use shared instance to avoid socket leakage.
        private readonly ElasticClient elasticClient = AppSettings.ELASTIC_CLIENT;

        private BulkIndexingDispatcher<SoloScore>? dispatcher;

        private Metadata? metadata;

        public void Run()
        {
            metadata = IndexHelper.FindOrCreateIndex(Name);
            if (metadata == null)
            {
                Console.WriteLine($"No metadata found for `{Name}` for version {AppSettings.Schema}...");
                return;
            }

            dispatcher = new BulkIndexingDispatcher<SoloScore>(metadata.RealName);

            try
            {
                // TODO: processor needs to check if index is closed instead of spinning

                // TODO: dispatcher should be the separate task?
                // or fix 0 length queue buffer on start?

                // read from queue
                var cts = new CancellationTokenSource();
                var queueTask = Task.Factory.StartNew(() =>
                    {
                        new Processor(dispatcher).Run(cts.Token);
                    });

                // Run() should block.
                dispatcher.Run();
                // something caused the dispatcher to bail out, e.g. index closed.
                Console.WriteLine("stopping indexer...");
                cts.Cancel();
                // FIXME: better shutdown (currently queue processer throws exception).
                queueTask.Wait();
                Console.WriteLine("indexer stopped.");
            }
            catch (AggregateException ae)
            {
                Console.WriteLine(ae);
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
        }
    }
}
