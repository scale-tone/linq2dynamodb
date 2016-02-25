using System;
using System.Linq;
using Enyim.Caching;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.Caching.MemcacheD;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.CachingTests
{
    public class StringFieldComparisonTests : DataContextTestBase
    {
        // ReSharper disable InconsistentNaming
        private MemcachedClient CacheClient { get; set; }

        protected EnyimTableCache TableCache { get; set; }

        private volatile int _cacheHitCount;

        [TestFixtureSetUp]
        public static void ExtraClassInit()
        {
            MemcachedController.StartIfRequired();
        }

        [TestFixtureTearDown]
        public static void ExtraClassClean()
        {
            MemcachedController.Stop();
        }

        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();

            this.CacheClient = new MemcachedClient();

            this.CacheClient.FlushAll();
            this._cacheHitCount = 0;

            this.TableCache = GetEnyimTableCache(this.CacheClient);
            this.TableCache.OnHit += () => this._cacheHitCount++;
            this.TableCache.OnLog += s => Logger.Debug(s);

            BooksHelper.CreateBook("AA");
            BooksHelper.CreateBook("AB");
            BooksHelper.CreateBook("ABC");
            BooksHelper.CreateBook("ACD");
            BooksHelper.CreateBook("AZ");
        }

        public override void TearDown()
        {
            BooksHelper.CleanSession();
        }

        [Test]
        public void DataContext_Caching_StringComparisonOperator_GreaterThanWorksEqually()
        {
            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            var query = bookTable.Where(b => b.Name.CompareTo("AB") > 0);

            // DynamoDb should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);
            // Cache should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);

            // adding a book
            bookTable.InsertOnSubmit(new Book {Name = "AY"});
            Context.SubmitChanges();

            // DynamoDb should return 4 records
            Assert.AreEqual(4, query.ToArray().Length);
            // Cache should return 4 records
            Assert.AreEqual(4, query.ToArray().Length);

            // removing that book
            bookTable.RemoveOnSubmit(new Book { Name = "AY" });
            Context.SubmitChanges();

            // DynamoDb should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);
            // Cache should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);
        }


        [Test]
        public void DataContext_Caching_StringComparisonOperator_LessThanOrEqualWorksEqually()
        {
            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            var query = bookTable.Where(b => b.Name.CompareTo("ABC") <= 0);

            // DynamoDb should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);
            // Cache should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);

            // adding a book
            bookTable.InsertOnSubmit(new Book { Name = "AY" });
            Context.SubmitChanges();

            // DynamoDb should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);
            // Cache should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);

            // removing that book
            bookTable.RemoveOnSubmit(new Book { Name = "AY" });
            Context.SubmitChanges();

            // DynamoDb should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);
            // Cache should return 3 records
            Assert.AreEqual(3, query.ToArray().Length);
        }


        [Test]
        public void DataContext_Caching_StringComparisonOperator_GreaterThanOrEqualWorksEqually()
        {
            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            var query = bookTable.Where(b => b.Name.CompareTo("B") >= 0);

            // DynamoDb should return 0 records
            Assert.AreEqual(0, query.ToArray().Length);
            // Cache should return 0 records
            Assert.AreEqual(0, query.ToArray().Length);

            // adding a book
            bookTable.InsertOnSubmit(new Book { Name = "B" });
            Context.SubmitChanges();

            // DynamoDb should return 1 records
            Assert.AreEqual(1, query.ToArray().Length);
            // Cache should return 1 records
            Assert.AreEqual(1, query.ToArray().Length);

            // removing that book
            bookTable.RemoveOnSubmit(new Book { Name = "B" });
            Context.SubmitChanges();

            // DynamoDb should return 0 records
            Assert.AreEqual(0, query.ToArray().Length);
            // Cache should return 0 records
            Assert.AreEqual(0, query.ToArray().Length);
        }


        private static EnyimTableCache GetEnyimTableCache(MemcachedClient memcachedClient)
        {
            return new EnyimTableCache(memcachedClient, TimeSpan.FromDays(1));
        }
    }
}
