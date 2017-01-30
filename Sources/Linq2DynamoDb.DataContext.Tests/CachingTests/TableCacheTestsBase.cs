using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using Linq2DynamoDb.DataContext.Utils;
using NUnit.Framework;
using ILog = log4net.ILog;
using LogManager = log4net.LogManager;

namespace Linq2DynamoDb.DataContext.Tests.CachingTests
{
    public abstract class TableCacheTestsBase
    {
        protected static readonly ILog Logger = LogManager.GetLogger(typeof(DataContextTestBase));
        protected ITableCache TableCache { get; set; }

        protected volatile int _cacheHitCount;
        protected volatile int _cacheMissCount;

        protected readonly Func<object, Document> ToDocumentConverter = DynamoDbConversionUtils.ToDocumentConverter(typeof(Entities.Book));
        protected readonly BooksComparer BookComparer = new BooksComparer();

        [SetUp]
        public virtual void SetUp()
        {
            this.TableCache.OnHit += () => this._cacheHitCount++;
            this.TableCache.OnMiss += () => this._cacheMissCount++;
            this.TableCache.OnLog += s => Logger.Debug(s);

            this.TableCache.Initialize("Books", typeof(Book), null);
        }

        [Test]
        public void TableCache_SingleEntitiesSavedAndLoaded()
        {
            // filling the cache in
            var books = new Dictionary<EntityKey, Book>
            {
                {new EntityKey(1, Guid.NewGuid()),  BooksHelper.CreateBook(persistToDynamoDb: false)},
                {new EntityKey(Guid.NewGuid(), "2"), BooksHelper.CreateBook(persistToDynamoDb: false)},
                {new EntityKey(DateTime.Now, 1.23456), BooksHelper.CreateBook(persistToDynamoDb: false)}
            };

            // some garnish
            this.TableCache.PutSingleLoadedEntity(new EntityKey(0, 0), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

            foreach (var book in books)
            {
                // adding twice
                this.TableCache.PutSingleLoadedEntity(book.Key, this.ToDocumentConverter(book.Value));
                this.TableCache.PutSingleLoadedEntity(book.Key, this.ToDocumentConverter(book.Value));
            }

            // some garnish
            this.TableCache.PutSingleLoadedEntity(new EntityKey("AAAA", "BBBB"), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));


            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            // reading twice
            foreach (var book in books)
            {
                var bookFromCache = (Book)this.TableCache.GetSingleEntity(book.Key).ToObject(typeof(Book));
                Assert.IsTrue(this.BookComparer.Equals(book.Value, bookFromCache));
            }
            foreach (var book in books)
            {
                var bookFromCache = (Book)this.TableCache.GetSingleEntity(book.Key).ToObject(typeof(Book));
                Assert.IsTrue(this.BookComparer.Equals(book.Value, bookFromCache));
            }

            Assert.AreEqual(this._cacheHitCount, books.Count*2);
            Assert.AreEqual(this._cacheMissCount, 0);
        }

        [Test]
        public void TableCache_SingleEntityIsNotLoadedAfterBeingRemoved()
        {
            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            var key = new EntityKey(Guid.NewGuid(), Guid.NewGuid());

            Assert.IsNull(this.TableCache.GetSingleEntity(key));

            // trying twice
            this.TableCache.PutSingleLoadedEntity(key, this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
            this.TableCache.RemoveEntities(new [] {key});
            Assert.IsNull(this.TableCache.GetSingleEntity(key));

            this.TableCache.PutSingleLoadedEntity(key, this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
            this.TableCache.RemoveEntities(new[] { key });
            this.TableCache.RemoveEntities(new[] { key });
            Assert.IsNull(this.TableCache.GetSingleEntity(key));

            Assert.AreEqual(this._cacheHitCount, 0);
            Assert.AreEqual(this._cacheMissCount, 3);
        }

        [Test]
        public void TableCache_IndexIsCreatedAndThenLoaded()
        {
            var indexBooks = new Dictionary<EntityKey, Book>
            {
                {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), BooksHelper.CreateBook(persistToDynamoDb: false)},
                {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), BooksHelper.CreateBook(persistToDynamoDb: false)},
                {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), BooksHelper.CreateBook(persistToDynamoDb: false)}
            };

            var conditions = new SearchConditions();

            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                // checking that the index is not accessible during creation
                Assert.IsNull(this.TableCache.GetEntities(conditions, null, "Name", true));

                foreach (var book in indexBooks)
                {
                    indexCreator.AddEntityToIndex(book.Key, this.ToDocumentConverter(book.Value));
                }
            }

            var originalBooks = indexBooks.Values.OrderByDescending(b => b.Name);
            


            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            var loadedBooks1 = this.TableCache.GetEntities(conditions, null, "Name", true).Select(d => (Book)d.ToObject(typeof(Book)));
            this.DeepCompareBookCollections(originalBooks, loadedBooks1);

            Assert.AreNotEqual(this._cacheHitCount, 0);
            Assert.AreEqual(this._cacheMissCount, 0);


            
            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            var loadedBooks2 = this.TableCache.GetEntities(conditions, null, "Name", true).Select(d => (Book)d.ToObject(typeof(Book)));
            this.DeepCompareBookCollections(originalBooks, loadedBooks2);

            Assert.AreNotEqual(this._cacheHitCount, 0);
            Assert.AreEqual(this._cacheMissCount, 0);
            


            // trying one more time
            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                foreach (var book in indexBooks)
                {
                    indexCreator.AddEntityToIndex(book.Key, this.ToDocumentConverter(book.Value));
                }
            }


            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            var loadedBooks3 = this.TableCache.GetEntities(conditions, null, "Name", true).Select(d => (Book)d.ToObject(typeof(Book)));
            this.DeepCompareBookCollections(originalBooks, loadedBooks3);

            Assert.AreNotEqual(this._cacheHitCount, 0);
            Assert.AreEqual(this._cacheMissCount, 0);
        }

        [Test]
        public void TableCache_IndexIsRecreated()
        {
            var conditions = new SearchConditions();

            // creating index

            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
            }

            var indexBooks = new Dictionary<EntityKey, Book>();
            indexBooks[new EntityKey(Guid.NewGuid(), Guid.NewGuid())] = BooksHelper.CreateBook(persistToDynamoDb: false);
            indexBooks[new EntityKey(Guid.NewGuid(), Guid.NewGuid())] = BooksHelper.CreateBook(persistToDynamoDb: false);
            indexBooks[new EntityKey(Guid.NewGuid(), Guid.NewGuid())] = BooksHelper.CreateBook(persistToDynamoDb: false);

            // re-creating index
            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                foreach (var book in indexBooks)
                {
                    indexCreator.AddEntityToIndex(book.Key, this.ToDocumentConverter(book.Value));
                }
            }

            // checking, that index contains only the last three books
            var originalBooks = indexBooks.Values.OrderByDescending(b => b.Name);
            var loadedBooks = this.TableCache.GetEntities(conditions, null, "Name", true).Select(d => (Book)d.ToObject(typeof(Book)));
            this.DeepCompareBookCollections(originalBooks, loadedBooks);
        }

        [Test]
        public void TableCache_EmptyIndexIsCreatedAndThenLoaded()
        {

            // some garnish
            this.TableCache.PutSingleLoadedEntity(new EntityKey(0, 0), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

            // creating an empty index
            var conditions = new SearchConditions();
            using (this.TableCache.StartCreatingIndex(conditions))
            {
            }

            // some garnish
            this.TableCache.PutSingleLoadedEntity(new EntityKey(1, 1), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));


            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            Assert.AreEqual(this.TableCache.GetEntities(conditions, null, null, false).Count(), 0);
            Assert.AreNotEqual(this._cacheHitCount, 0);
            Assert.AreEqual(this._cacheMissCount, 0);


            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            Assert.AreEqual(this.TableCache.GetEntities(conditions, null, null, false).Count(), 0);
            Assert.AreNotEqual(this._cacheHitCount, 0);
            Assert.AreEqual(this._cacheMissCount, 0);
        }

        [Test]
        public void TableCache_IndexIsDiscardedIfModifiedDuringCreation()
        {
            // creating an empty index
            var conditions = new SearchConditions();

            this._cacheHitCount = 0;
            this._cacheMissCount = 0;

            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            Assert.AreEqual(this._cacheHitCount, 0);
            Assert.AreNotEqual(this._cacheMissCount, 0);

            // creating an index and modifying it during creation
            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

                // modifying the index
                this.TableCache.UpdateCacheAndIndexes
                (
                    new Dictionary<EntityKey, Document>
                    {
                        {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false))}
                    },
                    new Dictionary<EntityKey, Document>(),
                    new EntityKey[0]
                );

                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
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

        [Test]
        public void TableCache_IndexIsDiscardedIfEntityIsMissing()
        {
            EntityKey randomBookKey = null;
            int bookCount = 5;
            int randomBookIndex = new Random().Next(0, bookCount-1); 
            var conditions = new SearchConditions();

            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                for (int i = 0; i < bookCount; i++)
                {
                    var bookKey = new EntityKey(Guid.NewGuid(), Guid.NewGuid());
                    indexCreator.AddEntityToIndex(bookKey, this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

                    if (i == randomBookIndex)
                    {
                        randomBookKey = bookKey;
                    }
                }
            }

            // dropping an entity from cache
            this.DropEntityFromCache(randomBookKey);

            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
        }


        [Test]
        public void TableCache_IndexIsModifiedSuccessfully()
        {
            // creating two filters
            var allEntitiesFilter = new SearchConditions();
            var greaterThanFilter = new SearchConditions();
            greaterThanFilter.AddCondition("Author", new SearchCondition(ScanOperator.Equal, "Mark Twain".ToDynamoDbEntry(typeof(string))));
            greaterThanFilter.AddCondition("LastRentTime", new SearchCondition(ScanOperator.GreaterThan, DateTime.Parse("2010-01-31").ToDynamoDbEntry(typeof(DateTime))));

            // creating two empty indexes
            using (this.TableCache.StartCreatingIndex(greaterThanFilter))
            {
            }
            using (this.TableCache.StartCreatingIndex(allEntitiesFilter))
            {
            }

            // checking that indexes are empty
            Assert.AreEqual(0, this.TableCache.GetEntities(allEntitiesFilter, null, null, false).Count());
            Assert.AreEqual(0, this.TableCache.GetEntities(greaterThanFilter, null, null, false).Count());

            // adding a book outside the search filter
            var book1 = BooksHelper.CreateBook(author: "Mark Twain", lastRentTime: DateTime.MinValue, persistToDynamoDb: false);
            var bookKey1 = new EntityKey(Guid.NewGuid(), Guid.NewGuid());

            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>
                {
                    {bookKey1, this.ToDocumentConverter(book1)}
                },
                new Dictionary<EntityKey, Document>(),
                new EntityKey[0]
            );

            // checking index sizes
            Assert.AreEqual(0, this.TableCache.GetEntities(greaterThanFilter, null, null, false).Count());
            Assert.AreEqual(1, this.TableCache.GetEntities(allEntitiesFilter, null, null, false).Count());


            // adding a book inside the search filter
            var book2 = BooksHelper.CreateBook(author: "Mark Twain", lastRentTime: DateTime.Now, persistToDynamoDb: false);
            var bookKey2 = new EntityKey(Guid.NewGuid(), Guid.NewGuid());

            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>
                {
                    {bookKey2, this.ToDocumentConverter(book2)}
                },
                new Dictionary<EntityKey, Document>(),
                new EntityKey[0]
            );

            // checking index sizes
            Assert.AreEqual(1, this.TableCache.GetEntities(greaterThanFilter, null, null, false).Count());
            Assert.AreEqual(2, this.TableCache.GetEntities(allEntitiesFilter, null, null, false).Count());


            // adding one more book inside the search filter
            var book3 = BooksHelper.CreateBook(author: "Mark Twain", lastRentTime: DateTime.Now, persistToDynamoDb: false);
            var bookKey3 = new EntityKey(Guid.NewGuid(), Guid.NewGuid());

            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>
                {
                    {bookKey3, this.ToDocumentConverter(book3)}
                },
                new Dictionary<EntityKey, Document>(),
                new EntityKey[0]
            );

            // checking index sizes
            Assert.AreEqual(2, this.TableCache.GetEntities(greaterThanFilter, null, null, false).Count());
            Assert.AreEqual(3, this.TableCache.GetEntities(allEntitiesFilter, null, null, false).Count());

            // modifying an entity
            book3.LastRentTime = DateTime.Parse("2009-01-01");

            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>(),
                new Dictionary<EntityKey, Document>
                {
                    {bookKey3, this.ToDocumentConverter(book3)}
                },
                new EntityKey[0]
            );

            // checking index sizes
            Assert.AreEqual(1, this.TableCache.GetEntities(greaterThanFilter, null, null, false).Count());
            Assert.AreEqual(3, this.TableCache.GetEntities(allEntitiesFilter, null, null, false).Count());

            // removing one entity
            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>(),
                new Dictionary<EntityKey, Document>(),
                new [] { bookKey1 }
            );

            // checking index sizes
            Assert.AreEqual(1, this.TableCache.GetEntities(greaterThanFilter, null, null, false).Count());
            Assert.AreEqual(2, this.TableCache.GetEntities(allEntitiesFilter, null, null, false).Count());


            // removing other two entities
            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>(),
                new Dictionary<EntityKey, Document>(),
                new [] { bookKey2, bookKey3 }
            );

            // checking index sizes
            Assert.AreEqual(0, this.TableCache.GetEntities(greaterThanFilter, null, null, false).Count());
            Assert.AreEqual(0, this.TableCache.GetEntities(allEntitiesFilter, null, null, false).Count());
        }



        [Test]
        public void TableCache_EntityModificationIsReflectedInIndexes()
        {
            var book = BooksHelper.CreateBook(numPages: 150, persistToDynamoDb: false);
            var bookKey = new EntityKey(Guid.NewGuid(), Guid.NewGuid());

            // creating two filters
            var filter1 = new SearchConditions();
            filter1.AddCondition("NumPages", new SearchCondition(ScanOperator.GreaterThan, 100.ToDynamoDbEntry(typeof(int))));
            var filter2 = new SearchConditions();
            filter2.AddCondition("NumPages", new SearchCondition(ScanOperator.LessThanOrEqual, 100.ToDynamoDbEntry(typeof(int))));

            // creating two indexes
            using (var indexCreator = this.TableCache.StartCreatingIndex(filter1))
            {
                indexCreator.AddEntityToIndex(bookKey, this.ToDocumentConverter(book));
            }
            using (this.TableCache.StartCreatingIndex(filter2))
            {
            }

            // now modifying the book so, that it should disappear in index1 and appear in index2
            book.NumPages = 50;
            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>(),
                new Dictionary<EntityKey, Document>
                {
                    {bookKey, this.ToDocumentConverter(book)}
                },
                new EntityKey[0]
            );

            // checking index sizes
            Assert.AreEqual(0, this.TableCache.GetEntities(filter1, null, null, false).Count());
            Assert.AreEqual(1, this.TableCache.GetEntities(filter2, null, null, false).Count());
        }

        [Test]
        public void TableCache_IndexCreationCancelledIfIndexIsModifiedInParallel()
        {
            var conditions = new SearchConditions();
            conditions.AddCondition("Author", new SearchCondition(ScanOperator.NotEqual, Guid.NewGuid().ToString().ToDynamoDbEntry(typeof(string))));

            // there should be no index yet
            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));

            // creating an index
            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

                // now updating the same index
                this.TableCache.UpdateCacheAndIndexes
                (
                    new Dictionary<EntityKey, Document>
                    {
                        {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false))}
                    },
                    new Dictionary<EntityKey, Document>(),
                    new EntityKey[] {}
                );

                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
            }

            // now the index should be dropped
            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
        }


        [Test]
        public void TableCache_IndexDoesNotResurrectAfterExpiring()
        {
            var conditions = new SearchConditions();
            conditions.AddCondition("Author", new SearchCondition(ScanOperator.NotEqual, Guid.NewGuid().ToString().ToDynamoDbEntry(typeof(string))));

            // there should be no index yet
            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));

            // creating an index
            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
            }

            this.DropIndexEntityFromCache(conditions.Key);

            // now updating the same index
            this.TableCache.UpdateCacheAndIndexes
            (
                new Dictionary<EntityKey, Document>
                {
                        {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false))}
                },
                new Dictionary<EntityKey, Document>(),
                new EntityKey[] { }
            );

            // the index should still be dropped
            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
        }

        [Test]
        public void TableCache_IndexIsNotAccessibleWhileBeingCreated()
        {
            var conditions = new SearchConditions();

            // creating an index
            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                // the index should not be accessible
                Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));

                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

                // the index should not be accessible
                Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));

                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

                // the index should not be accessible
                Assert.IsNull(this.TableCache.GetEntities(conditions, null, null, false));
            }

            // now the index should contain 2 items
            Assert.AreEqual(2, this.TableCache.GetEntities(conditions, null, null, false).Count());
        }


        [Test]
        public void TableCache_WhenIndexesCreatedInParallelTheLastOneIsSaved()
        {
            var indexBooks = new Dictionary<EntityKey, Book>
            {
                {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), BooksHelper.CreateBook(persistToDynamoDb: false)},
                {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), BooksHelper.CreateBook(persistToDynamoDb: false)},
                {new EntityKey(Guid.NewGuid(), Guid.NewGuid()), BooksHelper.CreateBook(persistToDynamoDb: false)}
            };

            // creating indexes in parallel

            var conditions = new SearchConditions();
            using (var indexCreator = this.TableCache.StartCreatingIndex(conditions))
            {
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

                using (var indexCreator2 = this.TableCache.StartCreatingIndex(conditions))
                {
                    indexCreator2.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
                    indexCreator2.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
                }

                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));

                using (var indexCreator2 = this.TableCache.StartCreatingIndex(conditions))
                {
                    foreach (var book in indexBooks)
                    {
                        indexCreator2.AddEntityToIndex(book.Key, this.ToDocumentConverter(book.Value));
                    }
                }

                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false)));
            }

            // checking, that index contains only the last three books
            var originalBooks = indexBooks.Values.OrderByDescending(b => b.Name);
            var loadedBooks = this.TableCache.GetEntities(conditions, null, "Name", true).Select(d => (Book)d.ToObject(typeof(Book)));
            this.DeepCompareBookCollections(originalBooks, loadedBooks);
        }

        [Test]
        public void TableCache_LocalChangesAreNeverOverwrittenWhenCreatingAnIndex()
        {
            var bookKey1 = new EntityKey(1, 1);
            var book1 = BooksHelper.CreateBook(author: "Mark Twain", numPages: 1, persistToDynamoDb: false);
            var bookKey2 = new EntityKey(2, 2);
            var book2 = BooksHelper.CreateBook(author: "Mark Twain", numPages: 2, persistToDynamoDb: false);
            var book21 = BooksHelper.CreateBook(author: "Mark Twain", numPages: 21, persistToDynamoDb: false);
            var bookKey3 = new EntityKey(3, 3);
            var book3 = BooksHelper.CreateBook(author: "Mark Twain", numPages: 3, persistToDynamoDb: false);

            // creating and filling one index with a filter
            var index1 = new SearchConditions();
            index1.AddCondition("Author", new SearchCondition(ScanOperator.Equal, "Mark Twain".ToDynamoDbEntry(typeof(string))));
            using (var indexCreator = this.TableCache.StartCreatingIndex(index1))
            {
                indexCreator.AddEntityToIndex(bookKey1, this.ToDocumentConverter(book1));
                indexCreator.AddEntityToIndex(bookKey2, this.ToDocumentConverter(book2));
            }

            // now start creating another index
            var index2 = new SearchConditions();
            using (var indexCreator = this.TableCache.StartCreatingIndex(index2))
            {
                // loading some garnish
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(author: "Mark Twain", persistToDynamoDb: false)));
                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(author: "Mark Twain", persistToDynamoDb: false)));

                // in parallel modifying existing index
                this.TableCache.UpdateCacheAndIndexes
                (
                    new Dictionary<EntityKey, Document>
                    {
                        { bookKey3, this.ToDocumentConverter(book3) }
                    },
                    new Dictionary<EntityKey, Document>
                    {
                        { bookKey2, this.ToDocumentConverter(book21) }
                    },
                    new [] { bookKey1 }
                );

                indexCreator.AddEntityToIndex(new EntityKey(Guid.NewGuid(), Guid.NewGuid()), this.ToDocumentConverter(BooksHelper.CreateBook(author: "Mark Twain", persistToDynamoDb: false)));

                // loading the same books to the second index - these should be discarded
                indexCreator.AddEntityToIndex(bookKey2, this.ToDocumentConverter(book2));
                indexCreator.AddEntityToIndex(bookKey1, this.ToDocumentConverter(book1));
            }

            // the second index shouldn't be created
            Assert.IsNull(this.TableCache.GetEntities(index2, null, null, false));

            // the first index should now contain book3 and book21
            var expectedBooks = new[] {book3, book21};
            var loadedBooks = this.TableCache.GetEntities(index1, null, "NumPages", false).Select(d => (Book)d.ToObject(typeof(Book)));

            this.DeepCompareBookCollections(expectedBooks, loadedBooks);
        }

        [Test]
        public void TableCache_ManyLargeIndexesAreCreatedAndUpdated()
        {
            const int IndexCount = 10;
            const int IndexSize = 100;
            const int FilterSize = 10;

            var filters = new Dictionary<SearchConditions, List<EntityKey>>();
            var dt = DateTime.Parse("1601-01-01");

            for (int i = 0; i < IndexCount; i++)
            {
                var filter = new SearchConditions();

                filter.AddCondition("Name", new SearchCondition(ScanOperator.Equal, i.ToString().ToDynamoDbEntry(typeof(string))));

                for (int j = 0; j < FilterSize; j++)
                {
                    dt = dt + TimeSpan.FromSeconds(i);
                    filter.AddCondition("LastRentTime", new SearchCondition(ScanOperator.GreaterThan, dt.ToDynamoDbEntry(typeof(DateTime))));
                }

                var entityKeys = new List<EntityKey>();
                for (int j = 0; j < IndexSize; j++)
                {
                    entityKeys.Add(new EntityKey(Guid.NewGuid(), Guid.NewGuid()));
                }

                filters[filter] = entityKeys;
            }

            Parallel.ForEach(filters, kv =>
            {
                var doc = this.ToDocumentConverter(BooksHelper.CreateBook(persistToDynamoDb: false));

                var filter = kv.Key;
                using (var indexCreator = this.TableCache.StartCreatingIndex(filter))
                {
                    foreach (var k in kv.Value)
                    {
                        indexCreator.AddEntityToIndex(k, doc);
                    }
                }
            });
 
            // now sequentially removing all the entities except one in each index
            foreach (var kv in filters)
            {
                foreach (var k in kv.Value.Skip(1))
                {
                    this.TableCache.UpdateCacheAndIndexes
                    (
                        new Dictionary<EntityKey, Document>(),
                        new Dictionary<EntityKey, Document>(),
                        new[] { k }
                    );
                }
            }

            // now checking, that each index contains only one entity
            Parallel.ForEach(filters, kv =>
            {
                var loadedBooks = this.TableCache.GetEntities(kv.Key, null, null, true);

                Assert.AreEqual(1, loadedBooks.Count());
            });
        }


        [Test]
        public void TableCache_TableLockIsAcquired()
        {
            string lockKey1 = Guid.NewGuid().ToString();
            string lockKey2 = Guid.NewGuid().ToString();

            this.TableCache.AcquireTableLock(lockKey1, TimeSpan.FromSeconds(5)).Dispose();

            // acquiring another lock in parallel
            var lock2 = this.TableCache.AcquireTableLock(lockKey2, TimeSpan.FromSeconds(5));

            // now the previous lock is disposed, and we should be able to acquire it again
            this.TableCache.AcquireTableLock(lockKey1, TimeSpan.FromSeconds(5)).Dispose();

            lock2.Dispose();
        }

        protected virtual void DropEntityFromCache(EntityKey key)
        {
            this.TableCache.RemoveEntities(new[] { key });
        }

        protected abstract void DropIndexEntityFromCache(string indexKey);

        protected void DeepCompareBookCollections(IEnumerable<Book> coll1, IEnumerable<Book> coll2)
        {
            var array1 = coll1.ToArray();
            var array2 = coll2.ToArray();

            Assert.AreEqual(array1.Length, array2.Length);

            for (int i = 0; i < array1.Length; i++)
            {
                Assert.IsTrue(this.BookComparer.Equals(array1[i], array2[i]));
            }
        }
    }
}
