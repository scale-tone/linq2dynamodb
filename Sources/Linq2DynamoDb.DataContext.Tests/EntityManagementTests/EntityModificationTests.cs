using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests
{
    [TestFixture]
    public class EntityModificationTests : DataContextTestBase
    {
        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();
        }

        public override void TearDown()
        {
        }

        [Test]
        public void DataContext_EntityModification_UpdatesRecordWithNewValues()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            book.PopularityRating = Book.Popularity.High;
            this.Context.SubmitChanges();

            var storedBook = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating, "Record was not updated");
        }

        [Test]
        public void DataContext_EntityModification_UpdateRecordWithNewArray() 
        {
            var book = BooksHelper.CreateBook(rentingHistory: null, persistToDynamoDb: false);
            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBook = booksTable.Find(book.Name, book.PublishYear);

            storedBook.RentingHistory = new List<string>() { "non-empty array" };
            this.Context.SubmitChanges();

            var storedBookAfterModification = booksTable.Find(book.Name, book.PublishYear);
            
            CollectionAssert.AreEquivalent(storedBook.RentingHistory, storedBookAfterModification.RentingHistory);
        }
        
        [Test]
        public void DataContext_UpdateEntity_UpdatesRecordWhenOldRecordIsNull()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average);

            var booksTable = this.Context.GetTable<Book>();

            book.PopularityRating = Book.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(book, null);

            var storedBook = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating, "Record was not updated");
        }

        [Test]
        public void DataContext_UpdateEntity_UpdatesRecordWhenOldRecordDoesNotMatchNewRecord()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average);

            var booksTable = this.Context.GetTable<Book>();
            var storedBook = booksTable.Find(book.Name, book.PublishYear);

            storedBook.PopularityRating = Book.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(storedBook, book);

            var updatedBook = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(storedBook.PopularityRating, updatedBook.PopularityRating, "Record was not updated");
        }
    }
}
