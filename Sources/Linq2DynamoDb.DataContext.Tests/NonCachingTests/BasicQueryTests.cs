using System.Diagnostics;
using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using Linq2DynamoDb.DataContext.Tests.QueryTests;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.NonCachingTests
{
    [TestFixture]
    public class BasicQueryTests : BasicQueryTestsCommon
    {
        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();
        }

        public override void TearDown()
        {
        }


        [Test]
        public void DataContext_QueryByHashReturnsEqualResults()
        {
            BooksHelper.CreateBook("A");
            BooksHelper.CreateBook("B");
            BooksHelper.CreateBook("C");

            var table = this.Context.GetTable<Book>();

            foreach (var b in table.Where(b => b.Name.CompareTo("C") == 0))
            {
                Debug.WriteLine(b.Name);
            }

            Debug.WriteLine("Finished");
        }

    }
}
