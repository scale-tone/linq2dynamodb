using System;
using System.Linq.Expressions;
using Amazon;
using Amazon.DynamoDBv2;
using Enyim.Caching;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching.MemcacheD;
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
                string userId = AuthRoutine.GetUserIdFromAuthorizationHeader();

                return this.GetTable<Note>
                (
                    userId,
                    () => new EnyimTableCache(CacheClient, TimeSpan.FromDays(1))
                );
            }
        }

        #endregion

        static NotesDataContext()
        {
            // creating a DynamoDb client in some region (AmazonDynamoDBClient is thread-safe and can be reused
            var dynamoDbConfig = new AmazonDynamoDBConfig { RegionEndpoint = RegionEndpoint.APSoutheast1 };

            DynamoDbClient = new AmazonDynamoDBClient(dynamoDbConfig);

            // creating a MemcacheD client (it's thread-safe and can be reused)
            CacheClient = new MemcachedClient();

            // creating tables
            CreateTablesIfTheyDoNotExist();
        }
        
        public NotesDataContext() : base(DynamoDbClient, string.Empty)
        {
        }

        private static readonly IAmazonDynamoDB DynamoDbClient;
        private static readonly MemcachedClient CacheClient;

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