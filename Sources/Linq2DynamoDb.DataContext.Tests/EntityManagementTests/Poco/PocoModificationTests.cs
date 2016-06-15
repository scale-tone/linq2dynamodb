using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;
using System.Collections.Generic;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests.Poco
{
    [TestFixture]
    public class PocoModificationTests : DataContextTestBase
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
            var book = BookPocosHelper.CreateBookPoco(popularityRating: BookPoco.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            book.PopularityRating = BookPoco.Popularity.High;
            this.Context.SubmitChanges();

            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBookPoco.PopularityRating, "Record was not updated");
        }

        [Test]
        public void DataContext_EntityModification_UpdateRecordWithNewArray()
        {
            var book = BookPocosHelper.CreateBookPoco(rentingHistory: null, persistToDynamoDb: false);
            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);

            storedBookPoco.RentingHistory = new List<string>() { "non-empty array" };
            this.Context.SubmitChanges();

            var storedBookPocoAfterModification = booksTable.Find(book.Name, book.PublishYear);

            CollectionAssert.AreEquivalent(storedBookPoco.RentingHistory, storedBookPocoAfterModification.RentingHistory);
        }

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations with default 'replace' behavior")]
        [Test]
        public void DataContext_EntityModification_UpdateShouldNotAffectFieldsModifiedFromOutside()
        {
            var book = BookPocosHelper.CreateBookPoco(popularityRating: BookPoco.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            // Update record from outside of DataTable
            BookPocosHelper.CreateBookPoco(book.Name, book.PublishYear, numPages: 15);

            book.PopularityRating = BookPoco.Popularity.High;
            this.Context.SubmitChanges();

            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBookPoco.PopularityRating, "Record was not updated");
            Assert.AreEqual(book.NumPages, 15, "Update has erased changes from outside");
        }

        [Test]
        public void DataContext_UpdateEntity_UpdatesRecordWhenOldRecordIsNull()
        {
            var book = BookPocosHelper.CreateBookPoco(popularityRating: BookPoco.Popularity.Average);

            var booksTable = this.Context.GetTable<BookPoco>();

            book.PopularityRating = BookPoco.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(book, null);

            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.PopularityRating, storedBookPoco.PopularityRating, "Record was not updated");
        }

        [Test]
        public void DataContext_UpdateEntity_UpdatesRecordWhenOldRecordDoesNotMatchNewRecord()
        {
            var book = BookPocosHelper.CreateBookPoco(popularityRating: BookPoco.Popularity.Average);

            var booksTable = this.Context.GetTable<BookPoco>();
            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);

            storedBookPoco.PopularityRating = BookPoco.Popularity.High;
            ((ITableCudOperations)booksTable).UpdateEntity(storedBookPoco, book);

            var updatedBookPoco = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(storedBookPoco.PopularityRating, updatedBookPoco.PopularityRating, "Record was not updated");
        }
    }
}
