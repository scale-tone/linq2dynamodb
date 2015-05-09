using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests
{
    [TestFixture]
    public class EntityRemovalTests : DataContextTestBase
    {
        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();
        }

        public override void TearDown()
        {
        }

        [Test]
        public void DataContext_EntityRemoval_RemovesExistingRecordFromDynamoDb()
        {
            var book = BooksHelper.CreateBook();

            var booksTable = this.Context.GetTable<Book>();
            booksTable.RemoveOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBooksCount = booksTable.Count(storedBook => storedBook.Name == book.Name);
            Assert.AreEqual(0, storedBooksCount, "Record was not deleted");
        }

        [Test]
        public void DataContext_EntityRemoval_DoesNotThrowAnyExceptionsIfRecordToRemoveDoesNotExist()
        {
            var book = BooksHelper.CreateBook(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.RemoveOnSubmit(book);
            this.Context.SubmitChanges();
        }
    }
}
