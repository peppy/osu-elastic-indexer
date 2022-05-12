// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Collections.Generic;
using System.Threading;
using McMaster.Extensions.CommandLineUtils;

namespace osu.ElasticIndexer.Commands
{
    [Command("fake", Description = "Pumps fake scores through the queue")]
    public class PumpFakeScores
    {
        protected readonly Processor<SoloScore> Queue = new Processor<SoloScore>();

        [Option("--delay", Description = "Delay in milliseconds between generating chunks")]
        public int Delay { get; set; }

        public int OnExecute(CancellationToken cancellationToken)
        {
            long counter = 0;

            var chunk = new List<SoloScore>(AppSettings.ChunkSize);

            while (!cancellationToken.IsCancellationRequested)
            {
                chunk.Add(
                    new SoloScore()
                    {
                        // TODO: better random data
                        Data = @"{
                            ""mods"": [],
                            ""rank"": ""D"",
                            ""passed"": true,
                            ""user_id"": 475001,
                            ""accuracy"": 0.32538631346578367,
                            ""build_id"": 3985,
                            ""ended_at"": ""2022-03-21T07:03:04+00:00"",
                            ""max_combo"": 14,
                            ""beatmap_id"": 876349,
                            ""ruleset_id"": 0,
                            ""started_at"": null,
                            ""statistics"": {
                                ""ok"": 7,
                                ""meh"": 0,
                                ""good"": 0,
                                ""miss"": 46,
                                ""great"": 22,
                                ""perfect"": 0,
                                ""ignore_hit"": 1,
                                ""ignore_miss"": 5,
                                ""large_tick_hit"": 2,
                                ""small_tick_hit"": 1,
                                ""large_tick_miss"": 1,
                                ""small_tick_miss"": 5
                            },
                            ""total_score"": 0
                        }",
                        Id = ++counter,
                        preserve = true
                    }
                );

                if (chunk.Count >= AppSettings.ChunkSize)
                {
                    Console.WriteLine($"pushing {chunk.Count} fake scores to {Processor<SoloScore>.QueueName}, id: {counter}");
                    Queue.PushToQueue(new ScoreItem(chunk));
                    chunk = new List<SoloScore>(AppSettings.ChunkSize);

                    if (Delay > 0)
                        Thread.Sleep(Delay);
                }
            }

            return 0;
        }
    }
}
