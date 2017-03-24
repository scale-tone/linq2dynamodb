using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests.Versioning 
{
    [TestFixture]
    class EntityVersioningTests : DataContextTestBase
    {
        public override void SetUp() 
        {
        }

        public override void TearDown() 
        {
        }

        [Test]
        [ExpectedException(typeof(AggregateException))]
        public void DataContext_UpdateEntity_Does_OptimisticLocking() 
        {
            var contextA = TestConfiguration.GetDataContext();
            var contextB = TestConfiguration.GetDataContext();

            var originalBook = BooksHelper.CreateBook(popularityRating: Book.Popularity.Low, rentingHistory: null);
            var booksTableA = contextA.GetTable<Book>();
            var booksTableB = contextB.GetTable<Book>();

            // Read the same entry from the database into two contexts
            var retrievedBookA = booksTableA.Find(originalBook.Name, originalBook.PublishYear);
            var retrievedBookB = booksTableB.Find(originalBook.Name, originalBook.PublishYear);

            // Mutate a property on instance A and persist
            retrievedBookA.PopularityRating = Book.Popularity.Average;
            contextA.SubmitChanges();

            // Mutate a property on instance B (unaware of changes to A)
            retrievedBookB.RentingHistory = new List<string> { "history element" };
            contextB.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(AggregateException))]
        public void DataContext_AddEntity_DoesNotOverwrite_ExistingVersionedEntity() 
        {
            var contextA = TestConfiguration.GetDataContext();
            var contextB = TestConfiguration.GetDataContext();

            var tableA = contextA.GetTable<Book>();
            var tableB = contextB.GetTable<Book>();

            var bookA = BooksHelper.CreateBook(name: "A Tale of Two Books", publishYear: 0, persistToDynamoDb: false);
            var bookB = BooksHelper.CreateBook(name: "A Tale of Two Books", publishYear: 0, persistToDynamoDb: false);


            tableA.InsertOnSubmit(bookA);
            contextA.SubmitChanges();

            tableB.InsertOnSubmit(bookB);
            contextB.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(AggregateException))]
        public void DataContext_RemoveEntity_RespectsVersionConstraint() 
        {
            var book = BooksHelper.CreateBook(numPages: 5, persistToDynamoDb: false);

            var contextA = TestConfiguration.GetDataContext();
            var contextB = TestConfiguration.GetDataContext();

            var tableA = contextA.GetTable<Book>();
            var tableB = contextB.GetTable<Book>();

            // Insert the book in Context A
            tableA.InsertOnSubmit(book);
            contextA.SubmitChanges();

            // Find and modify the book in Context B
            var retrievedBook = tableB.Find(book.Name, book.PublishYear);
            retrievedBook.NumPages = 10;
            contextB.SubmitChanges();

            // Try to delete the book from Context A
            tableA.RemoveOnSubmit(book);
            contextA.SubmitChanges();
        }
    }
}
