using System.Configuration;
using System.IO;
using System.Xml.Serialization;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.Runtime;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using log4net;

namespace Linq2DynamoDb.DataContext.Tests
{
	public static class TestConfiguration
	{
		public static readonly RegionEndpoint DynamoDbRegion =
			RegionEndpoint.GetBySystemName(ConfigurationManager.AppSettings["AWSRegion"]);

		public static readonly string TablePrefix = ConfigurationManager.AppSettings["TablePrefix"];

	    public static readonly string MemcachedBinaryPath = ConfigurationManager.AppSettings["MemcachedBinaryPath"];

        private static readonly string AwsCredentialsFilePath = ConfigurationManager.AppSettings["AwsCredentialsFilePath"];

        private static readonly ILog DataContextLogger = LogManager.GetLogger(typeof(DataContext));

		public static AWSCredentials GetAwsCredentials()
		{
			// returning null means we're providing credentials in some other way than a credentials file
		    if (string.IsNullOrEmpty(AwsCredentialsFilePath)) return null;

		    var file = new FileInfo(AwsCredentialsFilePath);
			if (!file.Exists)
			{
				// Create subdirectories
				var directory = file.Directory;
				if (directory != null)
				{
					directory.Create();
				}

				// Create new file
				CreateAwsCredentialsFile(AwsCredentialsFilePath);
			}

			var credentialsConfig = new AwsCredentialsConfig();
			using (var readStream = file.OpenRead())
			{
				var xmlSerializer = new XmlSerializer(typeof(AwsCredentialsConfig));
				try
				{
					credentialsConfig = (AwsCredentialsConfig)xmlSerializer.Deserialize(readStream);
				}
				// ReSharper disable once EmptyGeneralCatchClause
				catch
				{
				}
			}
			
			ValidateAwsCredentials(credentialsConfig.AwsAccessKey, credentialsConfig.AwsSecretKey);

			return new BasicAWSCredentials(credentialsConfig.AwsAccessKey, credentialsConfig.AwsSecretKey);
		}

		public static IAmazonDynamoDB GetDynamoDbClient()
		{
            // uncomment this line to get tests working without credentials file
            //return new AmazonDynamoDBClient();
            return new AmazonDynamoDBClient(GetAwsCredentials(), DynamoDbRegion);
        }

        public static DynamoDBContext GetDynamoDbContext()
        {
            return GetDynamoDbContext(GetDynamoDbClient());
        }

	    public static DynamoDBContext GetDynamoDbContext(IAmazonDynamoDB dynamoDbClient)
	    {
	        return new DynamoDBContext(dynamoDbClient, new DynamoDBContextConfig { TableNamePrefix = TablePrefix });
	    }

	    public static DataContext GetDataContext()
	    {
            return GetDataContext(GetDynamoDbClient());
	    }

        public static DataContext GetDataContext(IAmazonDynamoDB dynamoDbClient)
        {
            return GetDataContext(dynamoDbClient, TablePrefix);
        }

        public static DataContext GetDataContext(string tablePrefix)
        {
            return GetDataContext(GetDynamoDbClient(), tablePrefix);
        }

        public static DataContext GetDataContext(IAmazonDynamoDB dynamoDbClient, string tablePrefix)
        {
            var dataContext = new DataContext(dynamoDbClient, tablePrefix);
            dataContext.OnLog += DataContextLogger.Debug;

            return dataContext;
        }

		private static void CreateAwsCredentialsFile(string filePath)
		{
			var fileInfo = new FileInfo(filePath);
			var defaultConfig = new AwsCredentialsConfig { AwsAccessKey = string.Empty, AwsSecretKey = string.Empty };
			using (var textWriter = fileInfo.CreateText())
			{
				var xmlSerializer = new XmlSerializer(typeof(AwsCredentialsConfig));
				xmlSerializer.Serialize(textWriter, defaultConfig);
			}
		}

		private static void ValidateAwsCredentials(string awsAccessKey, string awsSecretKey)
		{
			if (string.IsNullOrEmpty(awsAccessKey) || string.IsNullOrEmpty(awsSecretKey))
			{
				throw new ConfigurationErrorsException(
					"AwsAccessKey and AwsSecretKey should have correct values. Please add them to file: " + AwsCredentialsFilePath
					+ " or delete file and it will be recreated with empty values");
			}
		}
	}
}
