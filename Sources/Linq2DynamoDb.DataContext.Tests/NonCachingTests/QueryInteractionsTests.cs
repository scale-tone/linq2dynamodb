using Linq2DynamoDb.DataContext.Tests.QueryTests;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.NonCachingTests
{
    [TestFixture]
    public class QueryInteractionsTests : QueryInteractionsTestsCommon
    {
        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();
        }

        public override void TearDown()
        {
        }
    }
}
