using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using System.Collections.Generic;

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
        public void DataContext_EntityModification_UpdatesManyEntities()
        {
            var bookRev1 = BooksHelper.CreateBook(publishYear: 0);

            for(int i = 0; i < 200; i++)
            {
                BooksHelper.CreateBook(bookRev1.Name, i);
            }

            var query = Context.GetTable<Book>().Where(b => b.Name == bookRev1.Name);

            foreach(var b in query)
            {
                b.Author = "scale-tone";
            }

            Context.SubmitChanges();
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

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations with default 'replace' behavior")]
        [Test]
        public void DataContext_EntityModification_UpdateShouldNotAffectFieldsModifiedFromOutside()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            // Update record from outside of DataTable
            BooksHelper.CreateBook(book.Name, book.PublishYear, numPages: 15);

            book.PopularityRating = Book.Popularity.High;
            this.Context.SubmitChanges();

            var storedBook = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBook.PopularityRating, "Record was not updated");
            Assert.AreEqual(book.NumPages, 15, "Update has erased changes from outside");
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
