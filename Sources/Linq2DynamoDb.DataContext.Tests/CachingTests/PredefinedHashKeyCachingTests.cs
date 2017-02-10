using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Amazon.DynamoDBv2.DataModel;
using Enyim.Caching;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.Caching.MemcacheD;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.CachingTests
{
    [DynamoDBTable("FullEntity")]
	class UserEntity : EntityBase
	{
		public string RangeKey { get; set; }
		public int IntField { get; set; }
		public DateTime DateTimeField { get; set; }
	}

	class FullEntity : UserEntity
	{
		public Guid HashKey { get; set; }
	}


	[TestFixture]
	class PredefinedHashKeyCachingTests
	{
        private static readonly string tablePrefix = "abc";

        // ReSharper disable InconsistentNaming
        private static MemcachedClient cacheClient;

		private static readonly Guid userId1 = Guid.NewGuid();
		private static readonly Guid userId2 = Guid.NewGuid();
        private static readonly Guid userId3 = Guid.NewGuid();

		private static readonly string rangeKey1 = "Erat nostro ut est, at perfecto pertinacia eum.";
		private static readonly string rangeKey2 = "Cum te iusto delicata reformidans.";
		private static readonly string rangeKey3 = "Ex interesset scribentur mei, quo alii facete liberavisse ei.";
		private static readonly string rangeKey4 = "Dico invidunt ut qui. Vel tincidunt adolescens ea, no homero noster verterem sit, ad deseruisse ullamcorper vel.";
		private static readonly string rangeKey5 = "Nulla liber sed in. Ei eos brute volumus deserunt.";

		private static readonly DateTime dtNow = DateTime.Now;

		private static readonly List<int> intRange = new List<int>{3,4,5};

		private static readonly FullEntity[] initialEntities = new []
		{
			new FullEntity
			{
				HashKey = userId1,
				RangeKey = rangeKey1,
				DateTimeField = dtNow - TimeSpan.FromDays(10),
				IntField = 1
			},
			new FullEntity
			{
				HashKey = userId1,
				RangeKey = rangeKey2,
				DateTimeField = dtNow,
				IntField = 2
			},
			new FullEntity
			{
				HashKey = userId1,
				RangeKey = rangeKey3,
				DateTimeField = dtNow + TimeSpan.FromDays(10),
				IntField = 3
			},
			new FullEntity
			{
				HashKey = userId2,
				RangeKey = rangeKey5,
				DateTimeField = dtNow - TimeSpan.FromDays(100500),
				IntField = 4
			},
			new FullEntity
			{
				HashKey = userId2,
				RangeKey = rangeKey4,
				DateTimeField = dtNow,
				IntField = 5
			},
			new FullEntity
			{
				HashKey = userId2,
				RangeKey = rangeKey3,
				DateTimeField = dtNow + TimeSpan.FromDays(100500),
				IntField = 6
			},
		};


		[SetUp]
		public static void ClassInit()
        {
            MemcachedController.StartIfRequired();
			cacheClient = new MemcachedClient();
            var ctx = TestConfiguration.GetDataContext(tablePrefix);

			ctx.CreateTableIfNotExists
			(
				new CreateTableArgs<FullEntity>
				(
					5, 5,
					en => en.HashKey,
					en => en.RangeKey,
					null,
					() => initialEntities
				)
			);
		}

		[TearDown]
		public static void ClassClean()
        {
            MemcachedController.Stop();
            var ctx = TestConfiguration.GetDataContext(tablePrefix);
            ctx.DeleteTable<FullEntity>();
        }

        private volatile int _cacheHitCount;
		private void OnCacheHit()
		{
			this._cacheHitCount++;
		}


		[Test]
		public void DataContext_Caching_UserSpecificIndexUpdatedByFullIndex()
		{
			cacheClient.FlushAll();
			this._cacheHitCount = 0;

            var ctx = TestConfiguration.GetDataContext(tablePrefix);

			var fullTable = ctx.GetTable<FullEntity>(() =>
				{
					var cache = new EnyimTableCache(cacheClient, TimeSpan.MaxValue);
					cache.OnLog += s => Debug.WriteLine("{0} FullEntityCache: {1}", DateTime.Now, s);
				    cache.OnHit += this.OnCacheHit;
					return cache;
				});

			var userSpecificTable1 = ctx.GetTable<UserEntity>
			(
				userId1,
				() =>
				{
					var cache = new EnyimTableCache(cacheClient, TimeSpan.MaxValue);
					cache.OnLog += s => Debug.WriteLine("{0} UserEntityCache1: {1}", DateTime.Now, s);
                    cache.OnHit += this.OnCacheHit;
                    return cache;
				}
			);

            var userSpecificTable2 = ctx.GetTable<UserEntity>
            (
                userId2,
                () =>
                {
                    var cache = new EnyimTableCache(cacheClient, TimeSpan.MaxValue);
                    cache.OnLog += s => Debug.WriteLine("{0} UserEntityCache2: {1}", DateTime.Now, s);
                    cache.OnHit += this.OnCacheHit;
                    return cache;
                }
            );

			var fullQuery = from en in fullTable where en.DateTimeField >= dtNow && (intRange.Contains(en.IntField)) select en;
			var userSpecificQuery1 = from en in userSpecificTable1 where en.DateTimeField >= dtNow && (intRange.Contains(en.IntField)) select en;
            var userSpecificQuery2 = from en in userSpecificTable2 where en.DateTimeField < dtNow && en.IntField < 3 select en;

			// loading indexes - they should be put to cache
			Assert.AreEqual(2, fullQuery.AsEnumerable().Count(), "Failed to load full query from table");
			Assert.AreEqual(1, userSpecificQuery1.AsEnumerable().Count(), "Failed to load user-specific query1 from table");
            Assert.AreEqual(0, userSpecificQuery2.AsEnumerable().Count(), "The user-specific query2 should return 0 entities");
            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");

			// now loading from cache
			Assert.AreEqual(2, fullQuery.AsEnumerable().Count(), "Failed to load full query from cache");
			Assert.AreEqual(1, userSpecificQuery1.AsEnumerable().Count(), "Failed to load user-specific query from cache");
            Assert.AreEqual(0, userSpecificQuery2.AsEnumerable().Count(), "The user-specific query2 should return 0 entities");
            Assert.AreEqual(3, this._cacheHitCount, "The queries were not loaded from cache");

            // now adding 2 new entities - they should appear in both indexes
            fullTable.InsertOnSubmit
            (
                new FullEntity()
                {
                    HashKey = userId1,
                    RangeKey = rangeKey5,
                    DateTimeField = dtNow + TimeSpan.FromDays(1000),
                    IntField = 5
                }
            );
            fullTable.InsertOnSubmit
            (
                new FullEntity()
                {
                    HashKey = userId1,
                    RangeKey = rangeKey4,
                    DateTimeField = dtNow + TimeSpan.FromDays(9999),
                    IntField = 4
                }
            );
            ctx.SubmitChanges();

		    this._cacheHitCount = 0;

            // now loading from cache
            Assert.AreEqual(4, fullQuery.AsEnumerable().Count(), "Failed to load full query from cache");
            Assert.AreEqual(3, userSpecificQuery1.AsEnumerable().Count(), "Failed to load user-specific query from cache");
            Assert.AreEqual(0, userSpecificQuery2.AsEnumerable().Count(), "The user-specific query2 should return 0 entities");
            Assert.AreEqual(3, this._cacheHitCount, "The queries were not loaded from cache");


            // now removing a couple of entities

            fullTable.RemoveOnSubmit(new FullEntity() { HashKey = userId1, RangeKey = rangeKey1 });
            fullTable.RemoveOnSubmit(new FullEntity() { HashKey = userId1, RangeKey = rangeKey3 });
            ctx.SubmitChanges();

            this._cacheHitCount = 0;

            // now loading from cache
            Assert.AreEqual(3, fullQuery.AsEnumerable().Count(), "Failed to load full query from cache");
            Assert.AreEqual(2, userSpecificQuery1.AsEnumerable().Count(), "Failed to load user-specific query from cache");
            Assert.AreEqual(0, userSpecificQuery2.AsEnumerable().Count(), "The user-specific query2 should return 0 entities");
            Assert.AreEqual(3, this._cacheHitCount, "The queries were not loaded from cache");

            // now loading from DynamoDb again
            cacheClient.FlushAll();
            this._cacheHitCount = 0;

            Assert.AreEqual(3, fullQuery.AsEnumerable().Count(), "Failed to load full query from cache");
            Assert.AreEqual(2, userSpecificQuery1.AsEnumerable().Count(), "Failed to load user-specific query from cache");
            Assert.AreEqual(0, userSpecificQuery2.AsEnumerable().Count(), "The user-specific query2 should return 0 entities");
            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");
        } 
	}
}
