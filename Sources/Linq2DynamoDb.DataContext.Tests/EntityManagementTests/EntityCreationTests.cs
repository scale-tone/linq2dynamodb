using System;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests
{
    [TestFixture]
    public class EntityCreationTests : DataContextTestBase
    {
        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();
        }

        public override void TearDown()
        {
        }

        [Test]
        public void DataContext_EntityCreation_PersistsRecordToDynamoDb()
        {
            var book = BooksHelper.CreateBook(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBook = booksTable.Find(book.Name, book.PublishYear);
            Assert.IsNotNull(storedBook);
        }

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations, which by default replaces existing entities")]
        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenEntityAlreadyExistsInDynamoDbButWasNeverQueriedInCurrentContext()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average);

            book.PopularityRating = Book.Popularity.High;

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenTryingToAddSameEntityTwice()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            book.PopularityRating = Book.Popularity.High;

            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenEntityPreviouslyStoredInDynamoDbWasQueriedInCurrentContext()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.Find(book.Name, book.PublishYear);

            book.PopularityRating = Book.Popularity.High;

            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }
    }
}
