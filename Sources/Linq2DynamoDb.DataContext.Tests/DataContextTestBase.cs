using Linq2DynamoDb.DataContext.Tests.Helpers;
using log4net;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests
{
    public abstract class DataContextTestBase
    {
        protected static readonly ILog Logger = LogManager.GetLogger(typeof(DataContextTestBase));

        protected DataContext Context { get; set; }

        [TestFixtureSetUp]
        public static void ClassInit()
        {
            BooksHelper.StartSession();
            BookPocosHelper.StartSession();
        }

        [TestFixtureTearDown]
        public static void ClassClean()
        {
            BooksHelper.CleanSession();
            BookPocosHelper.CleanSession();
        }

        [SetUp]
        public abstract void SetUp();

        [TearDown]
        public abstract void TearDown();
    }
}
