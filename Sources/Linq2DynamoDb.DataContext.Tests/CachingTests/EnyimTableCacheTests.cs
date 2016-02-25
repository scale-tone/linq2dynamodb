using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Enyim.Caching;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using Linq2DynamoDb.DataContext.Utils;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.CachingTests
{
    public class EnyimTableCacheTests : TableCacheTestsBase
    {

        // ReSharper disable InconsistentNaming
        private MemcachedClient CacheClient { get; set; }

        public override void SetUp()
        {
            this.CacheClient = new MemcachedClient();
            this.CacheClient.FlushAll();

            this.TableCache = new EnyimTableCache(this.CacheClient, TimeSpan.FromDays(1));

            base.SetUp();
        }

        [TestFixtureSetUp]
        public static void ClassInit()
        {
            MemcachedController.StartIfRequired();
        }

        [TestFixtureTearDown]
        public static void ClassClean()
        {
            MemcachedController.Stop();
        }


        [Test]
        public void TableCache_IndexIsDiscardedIfBeingCreatedTwiceInParallel()
        {
            // creating an empty index
            var conditions = new SearchConditions();
            conditions.AddCondition("PublishYear", new SearchCondition(ScanOperator.IsNull));

            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            Assert.AreEqual(this._cacheHitCount, 0);
            Assert.AreNotEqual(this._cacheMissCount, 0);


            // creating an index and interrupting it's creation
            using (this.TableCache.StartCreatingIndex(conditions))
            {
                this.TableCache.StartCreatingIndex(conditions);
            }

            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            Assert.AreEqual(this._cacheHitCount, 0);
            Assert.AreNotEqual(this._cacheMissCount, 0);

            // one more time
            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            Assert.AreEqual(this._cacheHitCount, 0);
            Assert.AreNotEqual(this._cacheMissCount, 0);
        }

        protected override void DropIndexEntityFromCache(string indexKey)
        {
            indexKey = ("Books" + indexKey).ToBase64();
            bool success = this.CacheClient.Remove(indexKey);
            Assert.IsTrue(success, "The index wasn't dropped from cache. Check the key format.");
        }
    }
}
