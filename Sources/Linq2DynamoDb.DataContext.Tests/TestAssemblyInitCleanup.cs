using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests
{
	[SetUpFixture]
	public class TestAssemblyInitCleanup
	{
		[SetUp]
		public void Init()
		{
			log4net.Config.XmlConfigurator.Configure();
		}

		[TearDown]
		public void Clean()
		{
		}
	}
}
