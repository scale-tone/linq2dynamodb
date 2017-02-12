using System;
using System.Collections.Generic;
using System.Linq;
using Enyim.Caching;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.Caching.MemcacheD;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.CachingTests
{
    public class BatchGetTests : DataContextTestBase
    {
        // ReSharper disable InconsistentNaming
        private MemcachedClient CacheClient { get; set; }

        private volatile int _cacheHitCount;
        private volatile bool _batchGetUsed;

        private DataContext HashKeyContext { get; set; }
        private DataTable<Book> HashKeyTable { get; set; }

        private DataContext HashAndRangeKeyContext { get; set; }
        private DataTable<Book> HashAndRangeKeyTable { get; set; }

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


        private void Context_OnLog(string msg)
        {
            // getting information about what type of operation was used from log
            if (msg.Contains("DynamoDb batch get:"))
            {
                this._batchGetUsed = true;
            }
        }

        public override void SetUp()
        {
            string hashKeyTablePrefix = typeof (BatchGetTests).Name + Guid.NewGuid();
            string hashAndRangeKeyTablePrefix = typeof(BatchGetTests).Name + Guid.NewGuid();

            TestConfiguration.GetDataContext(hashKeyTablePrefix).CreateTableIfNotExists
            (
                new CreateTableArgs<Book>
                (
                    book => book.Name,
                    null,
                    () => new[]
                    {
                        BooksHelper.CreateBook("C++ for Dummies", persistToDynamoDb:false),
                        BooksHelper.CreateBook("C# for Dummies", persistToDynamoDb:false),
                        BooksHelper.CreateBook("TurboPascal for Dummies", persistToDynamoDb:false)
                    }
                )
            );

            TestConfiguration.GetDataContext(hashAndRangeKeyTablePrefix).CreateTableIfNotExists
            (
                new CreateTableArgs<Book>
                (
                    book => book.Author,
                    book => book.Name,
                    () => new[]
                    {
                        BooksHelper.CreateBook(author: "Alan Turing", name: "C# for Dummies", persistToDynamoDb:false),
                        BooksHelper.CreateBook(author: "Alan Turing", name: "Visual Basic for Dummies", persistToDynamoDb:false),

                        BooksHelper.CreateBook(author: "Bjarne Stroustrup", name: "C++ for Dummies", persistToDynamoDb:false),
                        BooksHelper.CreateBook(author: "Bjarne Stroustrup", name: "C# for Dummies", persistToDynamoDb:false),
                        BooksHelper.CreateBook(author: "Bjarne Stroustrup", name: "TurboPascal for Dummies", persistToDynamoDb:false)
                    }
                )
            );

            this.HashKeyContext = TestConfiguration.GetDataContext(hashKeyTablePrefix);
            this.HashKeyContext.OnLog += this.Context_OnLog;

            this.HashAndRangeKeyContext = TestConfiguration.GetDataContext(hashAndRangeKeyTablePrefix);
            this.HashAndRangeKeyContext.OnLog += this.Context_OnLog;

            this.CacheClient = new MemcachedClient();

            this.CacheClient.FlushAll();
            this._cacheHitCount = 0;

            this.HashKeyTable = this.HashKeyContext.GetTable<Book>
            (
                () => 
                {
                    var cache = GetEnyimTableCache(this.CacheClient);
                    cache.OnHit += () => this._cacheHitCount++;
                    cache.OnLog += s => Logger.Debug(s);
                    return cache;
                }
            );
            this.HashAndRangeKeyTable = this.HashAndRangeKeyContext.GetTable<Book>
            (
                () =>
                {
                    var cache = GetEnyimTableCache(this.CacheClient);
                    cache.OnHit += () => this._cacheHitCount++;
                    cache.OnLog += s => Logger.Debug(s);
                    return cache;
                }
            );
        }

        public override void TearDown()
        {
            this.HashKeyContext.DeleteTable<Book>();
            this.HashAndRangeKeyContext.DeleteTable<Book>();

            BooksHelper.CleanSession();
        }

        [Test]
        public void DataContext_HashKeyTable_BatchGetOperationIsUsed()
        {
            var hashKeys = new List<string> {"C# for Dummies", "TurboPascal for Dummies"};

            // first from table
            var result1 = this.HashKeyTable.Where(b => hashKeys.Contains(b.Name)).ToDictionary(b => b.Name);

            Assert.IsTrue(this._batchGetUsed);
            this._batchGetUsed = false;

            // then from cache
            var result2 = this.HashKeyTable.Where(b => hashKeys.Contains(b.Name)).ToListAsync().Result.ToDictionary(b => b.Name);

            Assert.IsFalse(this._batchGetUsed);
            Assert.AreEqual(1, this._cacheHitCount);

            // now checking, that two books were returned
            foreach (string hashKey in hashKeys)
            {
                Assert.IsTrue(result1.Remove(hashKey));
                Assert.IsTrue(result2.Remove(hashKey));
            } 
            Assert.AreEqual(0, result1.Count);
            Assert.AreEqual(0, result2.Count);
        }


        [Test]
        public void DataContext_HashAndRangeKeyTable_BatchGetOperationIsUsed()
        {
            const string hashKey = "Bjarne Stroustrup";
            var rangeKeys = new List<string> { "C# for Dummies", "TurboPascal for Dummies" };

            // first from table
            var result1 = this.HashAndRangeKeyTable.Where(b => b.Author == hashKey && rangeKeys.Contains(b.Name)).ToListAsync().Result.ToDictionary(b => b.Name);

            Assert.IsTrue(this._batchGetUsed);
            this._batchGetUsed = false;

            // then from cache
            var result2 = this.HashAndRangeKeyTable.Where(b => b.Author == hashKey && rangeKeys.Contains(b.Name)).ToDictionary(b => b.Name);

            Assert.IsFalse(this._batchGetUsed);
            Assert.AreEqual(1, this._cacheHitCount);

            // now checking, that two books were returned
            foreach (string rangeKey in rangeKeys)
            {
                Assert.IsTrue(result1.Remove(rangeKey));
                Assert.IsTrue(result2.Remove(rangeKey));
            }
            Assert.AreEqual(0, result1.Count);
            Assert.AreEqual(0, result2.Count);
        }

        private static EnyimTableCache GetEnyimTableCache(MemcachedClient memcachedClient)
        {
            return new EnyimTableCache(memcachedClient, TimeSpan.FromDays(1));
        }
    }
}
