using Amazon;
using Amazon.DynamoDBv2;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching.Redis;
using Linq2DynamoDb.WebApi.OData;
using MobileNotes.OAuth;
using MobileNotes.WebApi.Models;
using StackExchange.Redis;
using System;
using System.Configuration;
using System.Linq.Expressions;
using System.Net;
using System.Web.Http;

namespace MobileNotes.WebApi.Controllers
{
    public class NotesController : DynamoDbController<Note>
    {
        static NotesController()
        {
            // creating a DynamoDb client in some region (AmazonDynamoDBClient is thread-safe and can be reused
            var dynamoDbConfig = new AmazonDynamoDBConfig { RegionEndpoint = RegionEndpoint.APSoutheast1 };

            DynamoDbClient = new AmazonDynamoDBClient(dynamoDbConfig);
            RedisConn = ConnectionMultiplexer.Connect(ConfigurationManager.AppSettings["RedisConnectionString"]);

            // creating tables
            CreateTablesIfTheyDoNotExist();
        }

        /// <summary>
        /// Creates an instance of NotesController and implements authentication.
        /// </summary>
        public NotesController() 
            : 
            base(DynamoDbClient, null, () => 
            {
                // Extracting userId from 'Authorization' HTTP header and using it as a predefined HashKey value. 
                try
                {
                    return AuthRoutine.GetUserIdFromAuthorizationHeader();
                }
                catch (UnauthorizedAccessException)
                {
                    // Throwing Web API-specific exception to return 401.
                    throw new HttpResponseException(HttpStatusCode.Unauthorized);
                }
            }, () => new RedisTableCache(RedisConn))
        {
        }

        private static readonly IAmazonDynamoDB DynamoDbClient;
        private static readonly ConnectionMultiplexer RedisConn;

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
    }
}