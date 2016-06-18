using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests.Poco
{
    [TestFixture]
    public class PocoRemovalTests : DataContextTestBase
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
            var book = BookPocosHelper.CreateBookPoco();

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.RemoveOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBookPocosCount = booksTable.Count(storedBookPoco => storedBookPoco.Name == book.Name);
            Assert.AreEqual(0, storedBookPocosCount, "Record was not deleted");
        }

        [Test]
        public void DataContext_EntityRemoval_DoesNotThrowAnyExceptionsIfRecordToRemoveDoesNotExist()
        {
            var book = BookPocosHelper.CreateBookPoco(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.RemoveOnSubmit(book);
            this.Context.SubmitChanges();
        }
    }
}
