using System;
using System.Diagnostics;
using System.Security.Authentication;
using System.Xml.Linq;
using Amazon;
using Amazon.DynamoDBv2;
using Enyim.Caching;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching.MemcacheD;

namespace MovieReviews.DataModel
{

    internal class ExternalLoggingRoutine
    {
        /// <summary>
        /// Had to move this routine out from ReviewsDataContext to avoid deadlock in a static ctor
        /// </summary>
        /// <param name="s"></param>
        internal static void Log(string s)
        {
            Debug.WriteLine("{0} {1}", DateTime.Now, s);
        }
    }

    public class ReviewsDataContext : DataContext
    {
        private static readonly IAmazonDynamoDB DynamoDbClient;
        private static readonly MemcachedClient CacheClient;

        static ReviewsDataContext()
        {
            string accessKey, secretKey;
            GetAwsCredentials(out accessKey, out secretKey);

            // creating a MemcacheD client (it's thread-safe and can be reused)
            CacheClient = new MemcachedClient();

            // creating a DynamoDb client in some region (AmazonDynamoDBClient is thread-safe and can be reused)
            DynamoDbClient = new AmazonDynamoDBClient
            (
                accessKey, 
                secretKey,
                new AmazonDynamoDBConfig { RegionEndpoint = RegionEndpoint.APSoutheast1, MaxErrorRetry = 6 }
            );

            // creating tables
            CreateTablesIfTheyDoNotExist();
        }

        public DataTable<Movie> Movies
        {
            get 
            { 
                return this.GetTable<Movie>(() =>
                {
                    var cache = new EnyimTableCache(CacheClient, TimeSpan.FromDays(1));
                    cache.OnLog += s => Debug.WriteLine("{0} MoviesCache {1}", DateTime.Now, s);
                    return cache;
                }); 
            }
        }

        public DataTable<Review> Reviews
        {
            get
            {
                return this.GetTable<Review>(() =>
                {
                    var cache = new EnyimTableCache(CacheClient, TimeSpan.FromDays(1));
                    cache.OnLog += s => Debug.WriteLine("{0} ReviewsCache {1}", DateTime.Now, s);
                    return cache;
                });
            }
        }

        public DataTable<Reviewer> Reviewers
        {
            get
            {
                return this.GetTable<Reviewer>(() =>
                {
                    var cache = new EnyimTableCache(CacheClient, TimeSpan.FromDays(1));
                    cache.OnLog += s => Debug.WriteLine("{0} ReviewersCache {1}", DateTime.Now, s);
                    return cache;
                });
            }
        }

        public DataTable<Genre> Genres
        {
            get
            {
                return this.GetTable<Genre>(() =>
                {
                    var cache = new EnyimTableCache(CacheClient, TimeSpan.FromDays(1));
                    cache.OnLog += s => Debug.WriteLine("{0} GenresCache {1}", DateTime.Now, s);
                    return cache;
                });
            }
        }

        public ReviewsDataContext() : base(DynamoDbClient, string.Empty)
        {
            // configure logging
            this.OnLog += s => Debug.WriteLine("{0} {1}", DateTime.Now, s);
        }


        private static void CreateTablesIfTheyDoNotExist()
        {
            var ctx = new DataContext(DynamoDbClient, string.Empty);

            ctx.OnLog += ExternalLoggingRoutine.Log;

            ctx.CreateTableIfNotExists
            (
                new CreateTableArgs<Genre>
                (
                    1, 1,
                    g => g.Title,
                    null, null,
                    Genre.GetInitialEntities // Do not implement this via an anonymous method! Because it's called within static constructor, the call will block indefinitely!
                )
            );

            ctx.CreateTableIfNotExists
            (
                new CreateTableArgs<Movie>
                (
                    m => m.Genre,
                    m => m.Title,
                    m => m.Director,
                    m => m.Year,
                    m => m.Budget
                )
            );

            ctx.CreateTableIfNotExists
            (
                new CreateTableArgs<Review>
                (
                    10, 10,
                    r => r.MovieId,
                    r => r.Reviewer
                )
            );

            ctx.CreateTableIfNotExists
            (
                new CreateTableArgs<Reviewers>
                (
                    r => r.Login
                )
            );
        }

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
            catch(Exception ex)
            {
                if ((ex is System.IO.FileNotFoundException) || (ex is System.IO.DirectoryNotFoundException))
                {
                    throw new InvalidCredentialException("Please, provide your own AWS credentials!");
                }
                throw;
            }
        }
    }
}