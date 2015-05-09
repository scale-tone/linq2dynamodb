using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.IndexTests
{
    public class ForumThread : EntityBase
    {
        public string ForumName { get; set; }
        public string Subject { get; set; }
        public DateTime LastPostDateTime { get; set; }
        public int Replies { get; set; }

        public bool IsArchived { get; set; }
    }

	public static class ForumQueries
	{
	    public static IEnumerable<ForumThread> GetInitialRecords()
		{
			return new[]
            {
                new ForumThread {ForumName = "S3",		    Subject = "Not Working",		LastPostDateTime = new DateTime(2001, 1, 11), Replies = 100, IsArchived = true  },
                new ForumThread {ForumName = "S3",		    Subject = "Working",			LastPostDateTime = new DateTime(2002, 1, 12), Replies = 200, IsArchived = false },
                new ForumThread {ForumName = "S3",		    Subject = "Not Functioning",	LastPostDateTime = new DateTime(2003, 2, 13), Replies = 300, IsArchived = true  },
                new ForumThread {ForumName = "DynamoDb",	Subject = "Working",			LastPostDateTime = new DateTime(2004, 2, 14), Replies = 400, IsArchived = false },
                new ForumThread {ForumName = "DynamoDb",	Subject = "Not Working",		LastPostDateTime = new DateTime(2005, 3, 15), Replies = 500, IsArchived = true  },
                new ForumThread {ForumName = "DynamoDb",	Subject = "Not Functioning",	LastPostDateTime = new DateTime(2006, 3, 16), Replies = 600, IsArchived = false }
            };
		}

		public static IQueryable<ForumThread> QueryByHashKey(this IQueryable<ForumThread> table)
		{
			return
				from t in table
				where
					t.ForumName == "S3"
				select t;
		}

		public static IQueryable<ForumThread> QueryByHashAndRangeKey(this IQueryable<ForumThread> table)
		{
			return
				from t in table
				where 
					t.ForumName == "S3"
					&&
					t.Subject.StartsWith("Not")
				select t;
		}

        public static IQueryable<ForumThread> QueryByHashAndRangeKeysAndPostDate1(this IQueryable<ForumThread> table)
        {
            return
                from t in table
                where
                    t.ForumName == "S3"
                    &&
                    t.Subject.StartsWith("Not")
                    &&
                    t.LastPostDateTime > new DateTime(2001, 1, 10)
                select t;
        }

        public static IQueryable<ForumThread> QueryByHashAndRangeKeysAndPostDate2(this IQueryable<ForumThread> table)
        {
            return
                from t in table
                where
                    t.ForumName == "S3"
                    &&
                    t.Subject.StartsWith("Not")
                    &&
                    t.LastPostDateTime > new DateTime(2003, 2, 13)
                select t;
        }


        public static IQueryable<ForumThread> QueryByHashKeyAndPostDate1(this IQueryable<ForumThread> table)
        {
            return
                from t in table
                where
                    t.ForumName == "S3"
                    &&
                    t.LastPostDateTime > new DateTime(2001, 1, 10)
                select t;
        }

        public static IQueryable<ForumThread> QueryByHashKeyAndPostDate2(this IQueryable<ForumThread> table)
        {
            return
                from t in table
                where
                    t.ForumName == "S3"
                    &&
                    t.LastPostDateTime > new DateTime(2003, 2, 13)
                select t;
        }

        public static IQueryable<ForumThread> QueryByHashKeyAndReplies1(this IQueryable<ForumThread> table)
        {
            return
                from t in table
                where
                    t.ForumName == "DynamoDb"
                    &&
                    t.Replies < 600
                select t;
        }

        public static IQueryable<ForumThread> QueryByHashKeyAndReplies2(this IQueryable<ForumThread> table)
        {
            return
                from t in table
                where
                    t.ForumName == "DynamoDb"
                    &&
                    t.Replies < 400
                select t;
        }


        public static IQueryable<ForumThread> QueryByHashKeyLastPostDateTimeAndIsArchived(this IQueryable<ForumThread> table)
        {
            return
                from t in table
                where
                    t.ForumName == "DynamoDb"
                    &&
                    t.LastPostDateTime > new DateTime(2004, 2, 14)
                    &&
                    t.IsArchived == true
                select t;
        }


        public static bool IsEqualTo(this ForumThread thisThread, ForumThread thatThread)
        {
            return
                thisThread.ForumName == thatThread.ForumName
                &&
                thisThread.Subject == thatThread.Subject
                &&
                thisThread.LastPostDateTime == thatThread.LastPostDateTime
                &&
                thisThread.Replies == thatThread.Replies
                &&
                thisThread.IsArchived == thatThread.IsArchived
            ;
        }

        public static bool IsEqualTo(this IEnumerable<ForumThread> thisArray, IEnumerable<ForumThread> thatArray)
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
    public class LocalSecondaryIndexTests : DataContextTestBase
    {
	    private DataContext NoIndexContext { get; set; }
		private DataContext OneIndexContext { get; set; }
        private DataContext TwoIndexContext { get; set; }

		private DataTable<ForumThread> NoIndexThreadTable { get; set; }
		private DataTable<ForumThread> OneIndexThreadTable { get; set; }
        private DataTable<ForumThread> TwoIndexThreadTable { get; set; }

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
            var noIndexTablePrefix = typeof(LocalSecondaryIndexTests).Name + Guid.NewGuid();
			var oneIndexTablePrefix = typeof(LocalSecondaryIndexTests).Name + Guid.NewGuid();
            var twoIndexTablePrefix = typeof(LocalSecondaryIndexTests).Name + Guid.NewGuid();

			this.ClearFlags();
            
            TestConfiguration.GetDataContext(noIndexTablePrefix).CreateTableIfNotExists
			(
				new CreateTableArgs<ForumThread>
				(
					// hash key
					forum => forum.ForumName,
					// range key
					forum => forum.Subject,

					// initial values
					ForumQueries.GetInitialRecords
				)
			);

            TestConfiguration.GetDataContext(oneIndexTablePrefix).CreateTableIfNotExists
			(
				new CreateTableArgs<ForumThread>
				(
					// hash key
					forum => forum.ForumName,
					// range key
					forum => forum.Subject,

					// local secondary indexes
					new LocalSecondaryIndexDefinitions<ForumThread>
					(
						forum => forum.LastPostDateTime
					),

					// initial values
					ForumQueries.GetInitialRecords
				)
			);

            TestConfiguration.GetDataContext(twoIndexTablePrefix).CreateTableIfNotExists
            (
                new CreateTableArgs<ForumThread>
                (
                    // hash key
                    forum => forum.ForumName,
                    // range key
                    forum => forum.Subject,

                    // local secondary indexes
                    new LocalSecondaryIndexDefinitions<ForumThread>
                    (
                        forum => forum.LastPostDateTime,
                        forum => forum.Replies
                    ),

                    // initial values
                    ForumQueries.GetInitialRecords
                )
            );

            this.NoIndexContext = TestConfiguration.GetDataContext(noIndexTablePrefix);
            this.OneIndexContext = TestConfiguration.GetDataContext(oneIndexTablePrefix);
            this.TwoIndexContext = TestConfiguration.GetDataContext(twoIndexTablePrefix);

            this.NoIndexContext.OnLog += this.Context_OnLog;
            this.OneIndexContext.OnLog += this.Context_OnLog;
            this.TwoIndexContext.OnLog += this.Context_OnLog;

			this.NoIndexThreadTable = this.NoIndexContext.GetTable<ForumThread>();
			this.OneIndexThreadTable = this.OneIndexContext.GetTable<ForumThread>();
            this.TwoIndexThreadTable = this.TwoIndexContext.GetTable<ForumThread>();
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
				this.NoIndexContext.DeleteTable<ForumThread>();
				this.OneIndexContext.DeleteTable<ForumThread>();
                this.TwoIndexContext.DeleteTable<ForumThread>();
            }
            catch (ResourceNotFoundException)
            {
            }
        }

        private void TestAllThreeTables(Func<IQueryable<ForumThread>, IQueryable<ForumThread>> query, bool noIndexTableShouldBeQueried, bool oneIndexTableShouldBeQueried, string firstIndexName, string secondIndexName)
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
            this.TestAllThreeTables(ForumQueries.QueryByHashKey, true, true, string.Empty, string.Empty);
        }

		[Test]
		public void DataContext_QueryByHashAndRangeKeyReturnsEqualResults()
		{
            this.TestAllThreeTables(ForumQueries.QueryByHashAndRangeKey, true, true, string.Empty, string.Empty);
        }

        [Test]
        public void DataContext_QueryByHashAndRangeKeysAndPostDateReturnsEqualResults1()
        {
            this.TestAllThreeTables(ForumQueries.QueryByHashAndRangeKeysAndPostDate1, true, true, string.Empty, string.Empty);
        }

        [Test]
        public void DataContext_QueryByHashAndRangeKeysAndPostDateReturnsEqualResults2()
        {
            this.TestAllThreeTables(ForumQueries.QueryByHashAndRangeKeysAndPostDate2, true, true, string.Empty, string.Empty);
        }

        [Test]
        public void DataContext_QueryByHashKeyAndPostDateReturnsEqualResults1()
        {
            this.TestAllThreeTables(ForumQueries.QueryByHashKeyAndPostDate1, false, true, "LastPostDateTimeIndex", "LastPostDateTimeIndex");
        }

        [Test]
        public void DataContext_QueryByHashKeyAndPostDateReturnsEqualResults2()
        {
            this.TestAllThreeTables(ForumQueries.QueryByHashKeyAndPostDate2, false, true, "LastPostDateTimeIndex", "LastPostDateTimeIndex");
        }

        [Test]
        public void DataContext_QueryByHashKeyAndRepliesReturnsEqualResults1()
        {
            this.TestAllThreeTables(ForumQueries.QueryByHashKeyAndReplies1, false, false, string.Empty, "RepliesIndex");
        }

        [Test]
        public void DataContext_QueryByHashKeyAndRepliesReturnsEqualResults2()
        {
            this.TestAllThreeTables(ForumQueries.QueryByHashKeyAndReplies2, false, false, string.Empty, "RepliesIndex");
        }

        [Test]
        public void DataContext_QueryByHashKeyLastPostDateTimeAndIsArchivedReturnsEqualResults()
        {
            this.TestAllThreeTables(ForumQueries.QueryByHashKeyLastPostDateTimeAndIsArchived, false, true, "LastPostDateTimeIndex", "LastPostDateTimeIndex");
        }
    }
}
