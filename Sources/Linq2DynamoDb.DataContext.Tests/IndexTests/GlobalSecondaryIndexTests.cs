using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.IndexTests
{

    public class GameScores : EntityBase
    {
        public string UserId { get; set; }
        public string GameTitle { get; set; }
        public int TopScore { get; set; }
        public DateTime TopScoreDateTime { get; set; }
        public int Wins { get; set; }
        public int Losses { get; set; }
    }


    public static class GameScoresQueries
    {
        public static IEnumerable<GameScores> GetInitialRecords()
        {
            return new[]
            {
                new GameScores{UserId = "101", GameTitle = "Galaxy Invaders",   TopScore = 5842,    TopScoreDateTime = new DateTime(2013, 09, 15, 17, 24, 31),  Wins = 21,  Losses = 72 },
                new GameScores{UserId = "101", GameTitle = "Meteor Blasters",   TopScore = 1000,    TopScoreDateTime = new DateTime(2013, 10, 22, 23, 18, 01),  Wins = 12,  Losses = 3  },
                new GameScores{UserId = "101", GameTitle = "Starship X",        TopScore = 24,      TopScoreDateTime = new DateTime(2013, 08, 31, 13, 14, 21),  Wins = 4,   Losses = 9  },

                new GameScores{UserId = "102", GameTitle = "Alien Adventures",  TopScore = 192,     TopScoreDateTime = new DateTime(2013, 07, 12, 11, 07, 56),  Wins = 32,  Losses = 192},
                new GameScores{UserId = "102", GameTitle = "Galaxy Invaders",   TopScore = 0,       TopScoreDateTime = new DateTime(2013, 09, 18, 07, 33, 42),  Wins = 0,   Losses = 5  },

                new GameScores{UserId = "103", GameTitle = "Attack Ships",      TopScore = 3,       TopScoreDateTime = new DateTime(2013, 10, 19, 01, 13, 24),  Wins = 1,   Losses = 8  },
                new GameScores{UserId = "103", GameTitle = "Galaxy Invaders",   TopScore = 2317,    TopScoreDateTime = new DateTime(2013, 09, 11, 06, 53, 00),  Wins = 40,  Losses = 3  },
                new GameScores{UserId = "103", GameTitle = "Meteor Blasters",   TopScore = 723,     TopScoreDateTime = new DateTime(2013, 10, 19, 01, 13, 24),  Wins = 22,  Losses = 12 },
                new GameScores{UserId = "103", GameTitle = "Starship X",        TopScore = 42,      TopScoreDateTime = new DateTime(2013, 07, 11, 06, 53, 00),  Wins = 4,   Losses = 19 },

                // even though most of properties are not specified here, these entities will appear in all indexes (because of default property values)
                new GameScores{UserId = "400", GameTitle = "Comet Quest"                                                                                                                },
                new GameScores{UserId = "400", GameTitle = "Starship X"                                                                                                                 }
            };
        }

        public static IQueryable<GameScores> QueryByHashKey(this IQueryable<GameScores> table)
        {
            return
                from t in table
                where
                    t.UserId == "101"
                select t;
        }

        public static IQueryable<GameScores> QueryByWins(this IQueryable<GameScores> table)
        {
            return
                from t in table
                where
                    t.Wins == 4
                select t;
        }

        public static IQueryable<GameScores> QueryByGameTitleAndTopScore1(this IQueryable<GameScores> table)
        {
            return
                from t in table
                where
                    t.GameTitle == "Starship X"
                    &&
                    t.TopScore > 30
                select t;
        }

        public static IQueryable<GameScores> QueryByGameTitleAndTopScore2(this IQueryable<GameScores> table)
        {
            return
                from t in table
                where
                    t.GameTitle == "Galaxy Invaders"
                    &&
                    t.TopScore > 10000
                select t;
        }


        public static IQueryable<GameScores> QueryByGameTitle(this IQueryable<GameScores> table)
        {
            return
                from t in table
                where
                    t.GameTitle == "Starship X"
                select t;
        }

        public static bool IsEqualTo(this GameScores thisThread, GameScores thatThread)
        {
            return
                thisThread.UserId == thatThread.UserId
                &&
                thisThread.GameTitle == thatThread.GameTitle
                &&
                thisThread.TopScore == thatThread.TopScore
                &&
                thisThread.TopScoreDateTime == thatThread.TopScoreDateTime
                &&
                thisThread.Wins == thatThread.Wins
                &&
                thisThread.Losses == thatThread.Losses
            ;
        }

        public static bool IsEqualTo(this IEnumerable<GameScores> thisArray, IEnumerable<GameScores> thatArray)
        {
            var thatList = thatArray.ToList();

            foreach (var thisEntity in thisArray)
            {
                var thatEntity = thatList.FirstOrDefault(t => t.IsEqualTo(thisEntity));
                if (thatEntity == null)
                {
                    return false;
                }
                thatList.Remove(thatEntity);
            }

            return thatList.Count == 0;
        }
    }

    [TestFixture]
    [Category(TestCategories.Slow)]
    public class GlobalSecondaryIndexTests : DataContextTestBase
    {
        private DataContext NoIndexContext { get; set; }
        private DataContext OneIndexContext { get; set; }
        private DataContext TwoIndexContext { get; set; }

        private DataTable<GameScores> NoIndexThreadTable { get; set; }
        private DataTable<GameScores> OneIndexThreadTable { get; set; }
        private DataTable<GameScores> TwoIndexThreadTable { get; set; }

        private bool _queryOperationUsed;
        private bool _indexQueryOperationUsed;
        private string _indexNameUsed;

        private void ClearFlags()
        {
            this._queryOperationUsed = false;
            this._indexQueryOperationUsed = false;
            this._indexNameUsed = string.Empty;
        }

        public override void SetUp()
        {
            var noIndexTablePrefix = typeof(GlobalSecondaryIndexTests).Name + Guid.NewGuid();
            var oneIndexTablePrefix = typeof(GlobalSecondaryIndexTests).Name + Guid.NewGuid();
            var twoIndexTablePrefix = typeof(GlobalSecondaryIndexTests).Name + Guid.NewGuid();

            this.ClearFlags();

            TestConfiguration.GetDataContext(noIndexTablePrefix).CreateTableIfNotExists
            (
                new CreateTableArgs<GameScores>
                (
                    // hash key
                    score => score.UserId,
                    // range key
                    score => score.GameTitle,

                    // initial values
                    GameScoresQueries.GetInitialRecords
                )
            );

            TestConfiguration.GetDataContext(oneIndexTablePrefix).CreateTableIfNotExists
            (
                new CreateTableArgs<GameScores>
                (
                    // hash key
                    score => score.UserId,
                    // range key
                    score => score.GameTitle,

                    // local secondary indexes,
                    null,

                    // global secondary indexes,
                    new GlobalSecondaryIndexDefinitions<GameScores>
                    (
                        score => new GlobalSecondaryIndexDefinition{HashKeyField = score.GameTitle, RangeKeyField = score.TopScore}
                    ),

                    // initial values
                    GameScoresQueries.GetInitialRecords
                )
            );

            TestConfiguration.GetDataContext(twoIndexTablePrefix).CreateTableIfNotExists
            (
                new CreateTableArgs<GameScores>
                (
                    // hash key
                    score => score.UserId,
                    // range key
                    score => score.GameTitle,

                    // local secondary indexes,
                    null,

                    // global secondary indexes,
                    new GlobalSecondaryIndexDefinitions<GameScores>
                    (
                        score => new GlobalSecondaryIndexDefinition { HashKeyField = score.GameTitle, RangeKeyField = score.TopScore },
                        score => new GlobalSecondaryIndexDefinition { HashKeyField = score.Wins }
                    ),

                    // initial values
                    GameScoresQueries.GetInitialRecords
                )
            );

            this.NoIndexContext = TestConfiguration.GetDataContext(noIndexTablePrefix);
            this.OneIndexContext = TestConfiguration.GetDataContext(oneIndexTablePrefix);
            this.TwoIndexContext = TestConfiguration.GetDataContext(twoIndexTablePrefix);

            this.NoIndexContext.OnLog += this.Context_OnLog;
            this.OneIndexContext.OnLog += this.Context_OnLog;
            this.TwoIndexContext.OnLog += this.Context_OnLog;

            this.NoIndexThreadTable = this.NoIndexContext.GetTable<GameScores>();
            this.OneIndexThreadTable = this.OneIndexContext.GetTable<GameScores>();
            this.TwoIndexThreadTable = this.TwoIndexContext.GetTable<GameScores>();
        }


        private void Context_OnLog(string msg)
        {
            // getting information about what type of operation was used from log
            if (msg.Contains("DynamoDb query:"))
            {
                this._queryOperationUsed = true;
            }
            if (msg.Contains("DynamoDb index query:"))
            {
                this._indexQueryOperationUsed = true;

                int indexNamePos = msg.IndexOf("Index name: ", StringComparison.InvariantCulture);
                if (indexNamePos >= 0)
                {
                    this._indexNameUsed = msg.Substring(indexNamePos + 12);
                }
            }
        }

        public override void TearDown()
        {
            try
            {
                this.NoIndexContext.DeleteTable<GameScores>();
                this.OneIndexContext.DeleteTable<GameScores>();
                this.TwoIndexContext.DeleteTable<GameScores>();
            }
            catch (ResourceNotFoundException)
            {
            }
        }


        private void TestAllThreeTables(Func<IQueryable<GameScores>, IQueryable<GameScores>> query, bool noIndexTableShouldBeQueried, bool oneIndexTableShouldBeQueried, string firstIndexName, string secondIndexName)
        {
            var result1 = query(this.NoIndexThreadTable).ToArray();
            Assert.AreEqual(noIndexTableShouldBeQueried, this._queryOperationUsed);
            Assert.IsFalse(this._indexQueryOperationUsed);

            this.ClearFlags();

            var result2 = query(this.OneIndexThreadTable).ToArray();
            if (string.IsNullOrEmpty(firstIndexName))
            {
                Assert.AreEqual(oneIndexTableShouldBeQueried, this._queryOperationUsed);
                Assert.IsFalse(this._indexQueryOperationUsed);
            }
            else
            {
                Assert.IsFalse(this._queryOperationUsed);
                Assert.IsTrue(this._indexQueryOperationUsed);
                Assert.AreEqual(this._indexNameUsed, firstIndexName);
            }

            this.ClearFlags();

            var result3 = query(this.TwoIndexThreadTable).ToArray();
            if (string.IsNullOrEmpty(secondIndexName))
            {
                Assert.IsTrue(this._queryOperationUsed);
                Assert.IsFalse(this._indexQueryOperationUsed);
            }
            else
            {
                Assert.IsFalse(this._queryOperationUsed);
                Assert.IsTrue(this._indexQueryOperationUsed);
                Assert.AreEqual(this._indexNameUsed, secondIndexName);
            }

            Assert.IsTrue(result1.IsEqualTo(result2));
            Assert.IsTrue(result2.IsEqualTo(result3));
        }

        [Test]
        public void DataContext_QueryByHashReturnsEqualResults()
        {
            this.TestAllThreeTables(GameScoresQueries.QueryByHashKey, true, true, string.Empty, string.Empty);
        }

        [Test]
        public void DataContext_QueryByWinsReturnsEqualResults()
        {
            this.TestAllThreeTables(GameScoresQueries.QueryByWins, false, false, string.Empty, "WinsIndex");
        }

        [Test]
        public void DataContext_QueryByGameTitleAndTopScore1ReturnsEqualResults()
        {
            this.TestAllThreeTables(GameScoresQueries.QueryByGameTitleAndTopScore1, false, true, "GameTitleTopScoreIndex", "GameTitleTopScoreIndex");
        }

        [Test]
        public void DataContext_QueryByGameTitleAndTopScore2ReturnsEqualResults()
        {
            this.TestAllThreeTables(GameScoresQueries.QueryByGameTitleAndTopScore2, false, true, "GameTitleTopScoreIndex", "GameTitleTopScoreIndex");
        }

        [Test]
        public void DataContext_QueryByGameTitleReturnsEqualResults()
        {
            this.TestAllThreeTables(GameScoresQueries.QueryByGameTitle, false, true, "GameTitleTopScoreIndex", "GameTitleTopScoreIndex");
        }
    }
}
