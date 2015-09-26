using System;
using System.Collections.Generic;
using System.Linq;
using Enyim.Caching;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.CachingTests
{
    [TestFixture]
    public class BasicCachingTests : DataContextTestBase
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
        }

        public override void TearDown()
        {
        }

        [Test]
        public void DataContext_Caching_FullTableScanLoadedFromCache()
        {
            BooksHelper.CreateBook();

            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            int booksCount = 0;
            foreach (var book in bookTable)
            {
                Logger.DebugFormat("Found book named '{0}' during DynamoDb full scan", book.Name);
                booksCount++;
            }

            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");

            int booksCountFromCache = 0;
            foreach (var book in bookTable)
            {
                Logger.DebugFormat("Found book named '{0}' during second full scan (cached data is expected to be used)", book.Name);
                booksCountFromCache++;
            }

            Assert.AreNotEqual(0, this._cacheHitCount, "Query result wasn't loaded from cache");
            Assert.AreEqual(booksCount, booksCountFromCache, "The number of books in cache differs from the number of books in DynamoDB");

            this._cacheHitCount = 0;
            booksCountFromCache = 0;

            foreach (var book in bookTable)
            {
                Logger.DebugFormat("Found book named '{0}' during third full scan (cached data is expected to be used)", book.Name);
                booksCountFromCache++;
            }

            Assert.AreNotEqual(0, this._cacheHitCount, "Query result wasn't loaded from cache on second try");
            Assert.AreEqual(booksCount, booksCountFromCache, "The number of books in cache differs from the number of books in DynamoDB");
        }

        [Test]
        public void DataContext_Caching_ComplexObjectFieldLoadedFromCache()
        {
            var createdBook = BooksHelper.CreateBook(publisher: new Book.PublisherDto { Title = "O’Reilly Media", Address = "Sebastopol, CA" });

            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            foreach (var book in bookTable)
            {
                Assert.AreEqual(book.Publisher.ToString(), createdBook.Publisher.ToString(), "Complex object field failed wasn't loaded from table");
            }

            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");

            foreach (var book in bookTable)
            {
                Assert.AreEqual(book.Publisher.ToString(), createdBook.Publisher.ToString(), "Complex object field failed wasn't loaded from cache");
            }

            Assert.AreNotEqual(0, this._cacheHitCount, "Query result wasn't loaded from cache");
        }

        [Test]
        public void DataContext_Caching_ComplexObjectListFieldLoadedFromCache()
        {
            var createdBook = BooksHelper.CreateBook(reviews: new List<Book.ReviewDto> { new Book.ReviewDto { Author = "Beavis", Text = "Cool" }, new Book.ReviewDto { Author = "Butt-head", Text = "This sucks!" } });

            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            foreach (var book in bookTable)
            {
                Assert.AreEqual(book.ReviewsList.Single(r => r.Author == "Beavis").ToString(), createdBook.ReviewsList.Single(r => r.Author == "Beavis").ToString(), "Complex object list field failed wasn't loaded from table");
                Assert.AreEqual(book.ReviewsList.Single(r => r.Author == "Butt-head").ToString(), createdBook.ReviewsList.Single(r => r.Author == "Butt-head").ToString(), "Complex object list field failed wasn't loaded from table");
            }

            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");

            foreach (var book in bookTable)
            {
                Assert.AreEqual(book.ReviewsList.Single(r => r.Author == "Beavis").ToString(), createdBook.ReviewsList.Single(r => r.Author == "Beavis").ToString(), "Complex object list field failed wasn't loaded from table");
                Assert.AreEqual(book.ReviewsList.Single(r => r.Author == "Butt-head").ToString(), createdBook.ReviewsList.Single(r => r.Author == "Butt-head").ToString(), "Complex object list field failed wasn't loaded from table");
            }

            Assert.AreNotEqual(0, this._cacheHitCount, "Query result wasn't loaded from cache");
        }

        [Test]
        public void DataContext_Caching_ProjectionLoadedFromCacheWithFullEntities()
        {
            this.DataContext_Caching_ProjectionLoadedFromCacheBase(true);
        }

        [Test]
        public void DataContext_Caching_ProjectionLoadedFromCache()
        {
            this.DataContext_Caching_ProjectionLoadedFromCacheBase(false);
        }

        [Test]
        public void DataContext_Caching_ProjectionsDroppedFromCacheUponModifications()
        {
            BooksHelper.CreateBook(
                name: "Sherlock Holmes:A Study in Scarlet",
                author: "Arthur Conan Doyle",
                publishYear: 1887,
                userFeedbackRating:
                Book.Stars.Diamond);
            BooksHelper.CreateBook(
                name: "Sherlock Holmes:The Sign of the Four",
                author: "Arthur Conan Doyle",
                publishYear: 1890,
                userFeedbackRating: Book.Stars.Platinum);
            BooksHelper.CreateBook(
                name: "The adventures of Shamrock Jolnes",
                author: "O.Henry",
                publishYear: 1911,
                userFeedbackRating: Book.Stars.Bronze);
            var houndOfBaskervillesBook = BooksHelper.CreateBook(
                name: "Sherlock Holmes:The Hound of the Baskervilles",
                author: "Arthur Conan Doyle",
                publishYear: 1902,
                userFeedbackRating: Book.Stars.Diamond);
            
            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            var authorsOfBooksAboutHolmes = (from book in bookTable
                                             where book.Name.Contains("Holmes")
                                             select book.Author).Distinct();

            var highestFeedbackBookNames = from book in bookTable
                                           where book.UserFeedbackRating == Book.Stars.Diamond
                                           select book.Name;

            // first loading from DynamoDb
            Assert.AreEqual(1, authorsOfBooksAboutHolmes.AsEnumerable().Count(), "There should be only one author of books about Sherlock Holmes!");
            Assert.AreEqual(2, highestFeedbackBookNames.AsEnumerable().Count(), "There should be 2 highest-ranked books");
            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");

            this._cacheHitCount = 0;

            // now loading from cache
            Assert.AreEqual(1, authorsOfBooksAboutHolmes.AsEnumerable().Count(), "There should be only one author of books about Sherlock Holmes!");
            Assert.AreEqual(2, highestFeedbackBookNames.AsEnumerable().Count(), "There should be 2 highest-ranked books");
            Assert.AreEqual(2, this._cacheHitCount, "Projection indexes missing in cache");

            this._cacheHitCount = 0;

            // now removing a book
            bookTable.RemoveOnSubmit(houndOfBaskervillesBook);
            Context.SubmitChanges();

            // now all projection indexes should be dropped and data should be loaded from DynamoDb
            Assert.AreEqual(1, authorsOfBooksAboutHolmes.AsEnumerable().Count(), "There should be only one author of books about Sherlock Holmes!");
            Assert.AreEqual(1, highestFeedbackBookNames.AsEnumerable().Count(), "There should be 1 highest-ranked book");
            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");

            this._cacheHitCount = 0;

            // now adding a book
            var adventureOfEmptyHouseBook = BooksHelper.CreateBook(
                name: "Sherlock Holmes:The Adventure of the Empty House",
                author: "Arthur Conan Doyle",
                publishYear: 1903,
                userFeedbackRating: Book.Stars.Silver,
                persistToDynamoDb: false);

            bookTable.InsertOnSubmit(adventureOfEmptyHouseBook);
            Context.SubmitChanges();

            // now only one of projection indexes should be dropped
            Assert.AreEqual(1, authorsOfBooksAboutHolmes.AsEnumerable().Count(), "There should be only one author of books about Sherlock Holmes!");
            Assert.AreEqual(1, highestFeedbackBookNames.AsEnumerable().Count(), "There should be 1 highest-ranked book");
            Assert.AreEqual(1, this._cacheHitCount, "Projection indexes missing in cache");

            // now modifying an entity
            var bookToBeChanged = bookTable.Find(adventureOfEmptyHouseBook.Name, adventureOfEmptyHouseBook.PublishYear);
            bookToBeChanged.UserFeedbackRating = Book.Stars.Diamond;
            Context.SubmitChanges();

            this._cacheHitCount = 0;

            // now checking that the book was modified in cache
            var changedBook = bookTable.Find(adventureOfEmptyHouseBook.Name, adventureOfEmptyHouseBook.PublishYear);
            Assert.AreEqual(Book.Stars.Diamond, changedBook.UserFeedbackRating, "Book was not updated");
            Assert.AreEqual(1, this._cacheHitCount, "Cache was not used during book re-query");

            this._cacheHitCount = 0;

            // now all projection indexes should be dropped and data should be loaded from DynamoDb
            Assert.AreEqual(1, authorsOfBooksAboutHolmes.AsEnumerable().Count(), "There should be only one author of books about Sherlock Holmes!");
            Assert.AreEqual(2, highestFeedbackBookNames.AsEnumerable().Count(), "There should be 2 highest-ranked books");
            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");
            
            // removing the book one more time
            bookTable.RemoveOnSubmit(adventureOfEmptyHouseBook);
            Context.SubmitChanges();

            // now all projection indexes should be dropped and data should be loaded from DynamoDb
            Assert.AreEqual(1, authorsOfBooksAboutHolmes.AsEnumerable().Count(), "There should be only one author of books about Sherlock Holmes!");
            Assert.AreEqual(1, highestFeedbackBookNames.AsEnumerable().Count(), "There should be 1 highest-ranked book");
            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");
        }

        private void DataContext_Caching_ProjectionLoadedFromCacheBase(bool loadFullEntitiesBefore)
        {
            BooksHelper.CreateBook(author: "Shakespeare");
            BooksHelper.CreateBook(author: "William Shakespeare");
            BooksHelper.CreateBook(author: "Shakespeare William");
            BooksHelper.CreateBook(author: "Chakespare");

            var bookTable = Context.GetTable<Book>(() => this.TableCache);

            int shakespeareBooksCount = 0;

            if (loadFullEntitiesBefore)
            {
                foreach (var book in bookTable.Where(b => b.Author.StartsWith("Shakespeare")))
                {
                    Logger.DebugFormat("Found book from author: {0}", book.Author);
                    shakespeareBooksCount++;
                }
            }
            else
            {
                foreach (var book in bookTable.Where(b => b.Author.StartsWith("Shakespeare")).Select(b => new { b.Name, b.Author }))
                {
                    Logger.DebugFormat("Found book from author: {0}", book.Author);
                    shakespeareBooksCount++;
                }
            }

            Assert.AreEqual(0, this._cacheHitCount, "Cache wasn't flushed for some strange reason");

            int shakespeareBooksCountFromCache = 0;
            foreach (var book in from b in bookTable
                     where b.Author.StartsWith("Shakespeare")
                     select new { b.Name, b.Author })
            {
                Logger.DebugFormat("Found book from author: {0}", book.Author);
                shakespeareBooksCountFromCache++;
            }

            Assert.AreNotEqual(0, this._cacheHitCount, "Query result wasn't loaded from cache");
            Assert.AreEqual(shakespeareBooksCount, shakespeareBooksCountFromCache, "The number of books in cache differs from the number of books in DynamoDB");

            shakespeareBooksCountFromCache = 0;
            this._cacheHitCount = 0;
            foreach (var book in from b in bookTable
                     where b.Author.StartsWith("Shakespeare")
                     select new { b.Name, b.Author })
            {
                Logger.DebugFormat("Found book from author: {0}", book.Author);
                shakespeareBooksCountFromCache++;
            }

            Assert.AreNotEqual(0, this._cacheHitCount, "Query result wasn't loaded from cache");
            Assert.AreEqual(shakespeareBooksCount, shakespeareBooksCountFromCache, "The number of books in cache differs from the number of books in DynamoDB");
        }

        private static EnyimTableCache GetEnyimTableCache(MemcachedClient memcachedClient)
        {
            return new EnyimTableCache(memcachedClient, TimeSpan.FromDays(1));
        }
    }
}
