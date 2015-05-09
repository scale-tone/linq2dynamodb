using System;

namespace Linq2DynamoDb.DataContext.Tests.Helpers
{
	[Serializable]
	public class AwsCredentialsConfig
	{
		public string AwsAccessKey { get; set; }

		public string AwsSecretKey { get; set; }
	}
}