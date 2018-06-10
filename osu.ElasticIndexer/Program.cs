// Copyright (c) 2007-2018 ppy Pty Ltd <contact@ppy.sh>.
// Licensed under the MIT Licence - https://raw.githubusercontent.com/ppy/osu-elastic-indexer/master/LICENCE

using System;
using System.Globalization;
using System.Threading;
using Dapper;

namespace osu.ElasticIndexer
{
    public class Program
    {
        public void Run()
        {
            if (AppSettings.IsWatching)
                Console.WriteLine($"Running in watch mode with {AppSettings.PollingInterval}ms poll.");

            if (AppSettings.IsWatching)
            {
                while (true)
                {
                    // When running in watch mode, the indexer should be told to resume from the
                    // last known saved point instead of the configured value.
                    RunLoop(AppSettings.ResumeFrom);
                    Thread.Sleep(AppSettings.PollingInterval);
                }
            }
            else
            {
                RunLoop();
            }
        }

        public void RunLoop(long? resumeFrom = null)
        {
            var suffix = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();

            foreach (var mode in AppSettings.Modes)
            {
                var modeTitleCase = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(mode);
                var className = $"{typeof(HighScore).Namespace}.HighScore{modeTitleCase}";

                Type indexerType = typeof(HighScoreIndexer<>)
                    .MakeGenericType(Type.GetType(className, true));

                var indexer = (IIndexer) Activator.CreateInstance(indexerType);
                indexer.Suffix = suffix;
                indexer.Name = $"{AppSettings.Prefix}high_scores_{mode}";
                indexer.ResumeFrom = resumeFrom;
                indexer.Run();
            }
        }

        public static void Main()
        {
            DefaultTypeMap.MatchNamesWithUnderscores = true;
            new Program().Run();
        }
    }
}
