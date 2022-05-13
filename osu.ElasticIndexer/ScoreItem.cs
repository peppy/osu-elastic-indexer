// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using osu.Server.QueueProcessor;

namespace osu.ElasticIndexer
{
    public class ScoreItem : QueueItem
    {
        public SoloScore Score { get; private set; }

        public ScoreItem(SoloScore score)
        {
            Score = score;
        }
    }
}
