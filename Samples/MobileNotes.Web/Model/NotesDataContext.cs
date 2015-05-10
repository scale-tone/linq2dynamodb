using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Security.Authentication;
using System.Web;
using System.Xml.Linq;
using Amazon;
using Amazon.DynamoDBv2;
using Enyim.Caching;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching;
using MobileNotes.Common;
using MobileNotes.Web.Common;

namespace MobileNotes.Web.Model
{
    /// <summary>
    /// This DataContext is being exposed as an editable OData endpoint, so it must be inherited from UpdatableDataContext
    /// </summary>
    public class NotesDataContext : UpdatableDataContext
    {
        #region EntitySets

        /// <summary>
        /// User-specific notes
        /// </summary>
        public DataTable<Note> Notes 
        { 
            get
            {
                // restricting access to only authenticated users and providing them access to only their own notes
                string userId = this.GetUserIdFromAuthorizationHeader();

                return this.GetTable<Note>
                (
                    userId,
                    () => new EnyimTableCache(CacheClient, TimeSpan.FromDays(1))
                );
            }
        }

        #endregion

        private static readonly IAmazonDynamoDB DynamoDbClient;
        private static readonly MemcachedClient CacheClient;

        static NotesDataContext()
        {
            // creating a DynamoDb client in some region (AmazonDynamoDBClient is thread-safe and can be reused
            var dynamoDbConfig = new AmazonDynamoDBConfig { RegionEndpoint = RegionEndpoint.APSoutheast1 };

            string accessKey, secretKey;
            GetAwsCredentials(out accessKey, out secretKey);
            DynamoDbClient = new AmazonDynamoDBClient(accessKey, secretKey, dynamoDbConfig);

            // creating a MemcacheD client (it's thread-safe and can be reused)
            CacheClient = new MemcachedClient();

            // creating tables
            CreateTablesIfTheyDoNotExist();
        }
        
        public NotesDataContext() : base(DynamoDbClient, string.Empty)
        {
        }

        private static void CreateTablesIfTheyDoNotExist()
        {
            var ctx = new DataContext(DynamoDbClient, string.Empty);
            ctx.CreateTableIfNotExists
            (
                new CreateTableArgs<Note>
                (
                    // hash key
                    "UserId", typeof(string), 
                    // range key
                    g => g.ID, 
                    // secondary index
                    (Expression<Func<Note, object>>)(g => g.TimeCreated)
                )
            );
        }

        /// <summary>
        /// Obtains a Microsoft Account userId from incoming Authorization header
        /// </summary>
        /// <returns></returns>
        private string GetUserIdFromAuthorizationHeader()
        {
            var authHeader = HttpContext.Current.Request.Headers["Authorization"];
            if
            (
                (string.IsNullOrEmpty(authHeader))
                ||
                (!authHeader.StartsWith(Constants.AuthorizationHeaderPrefix))
            )
            {
                throw new InvalidOperationException("Not authorized");
            }

            string tokenString = authHeader.Substring(Constants.AuthorizationHeaderPrefix.Length);

            // parsing and validating the token
            var jsonWebToken = new JsonWebToken(tokenString, new Dictionary<int, string> { { 0, Constants.LiveClientSecret } });

            if (jsonWebToken.IsExpired)
            {
                throw new InvalidOperationException("Token expired");
            }

            return jsonWebToken.Claims.UserId;
        }

        #region Get AWS credentials from a secret store

        public static void GetAwsCredentials(out string accessKey, out string secretKey)
        {
            try
            {
                var credsDoc = XDocument.Load(@"C:\aws_credentials.xml");

                // ReSharper disable PossibleNullReferenceException
                accessKey = credsDoc.Root.Element("AwsAccessKey").Value;
                secretKey = credsDoc.Root.Element("AwsSecretKey").Value;
                // ReSharper restore PossibleNullReferenceException
            }
            catch (Exception ex)
            {
                if ((ex is System.IO.FileNotFoundException) || (ex is System.IO.DirectoryNotFoundException))
                {
                    throw new InvalidCredentialException("Please, provide your own AWS credentials!");
                }
                throw;
            }
        }

        #endregion
    }
}