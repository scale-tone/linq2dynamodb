using System;
using System.Diagnostics;
using System.IO;
using System.Security.Authentication;
using System.Xml.Linq;
using Amazon;
using Amazon.DynamoDBv2;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching;
using MovieReviews.AspNet.BackEnd.Model;

namespace MovieReviews.AspNet.Mvc.Models
{
    internal class ExternalLoggingRoutine
    {
        /// <summary>
        ///     Had to move this routine out from ReviewsDataContext to avoid deadlock in a static ctor
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

        static ReviewsDataContext()
        {
            string accessKey, secretKey;
            GetAwsCredentials(out accessKey, out secretKey);

            // creating a MemcacheD client (it's thread-safe and can be reused)

            // creating a DynamoDb client in some region (AmazonDynamoDBClient is thread-safe and can be reused)
            DynamoDbClient = new AmazonDynamoDBClient
                (
                accessKey,
                secretKey,
                new AmazonDynamoDBConfig {RegionEndpoint = RegionEndpoint.APSoutheast1, MaxErrorRetry = 6}
                );

            // creating tables
            CreateTablesIfTheyDoNotExist();
        }

        public ReviewsDataContext()
            : base(DynamoDbClient, string.Empty)
        {
            // configure logging
            OnLog += s => Debug.WriteLine("{0} {1}", DateTime.Now, s);
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
                        Genre.GetInitialEntities
                        // Do not implement this via an anonymous method! Because it's called within static constructor, the call will block indefinitely!
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
                XDocument credsDoc = XDocument.Load(@"C:\aws_credentials.xml");

                accessKey = credsDoc.Root.Element("AwsAccessKey").Value;
                secretKey = credsDoc.Root.Element("AwsSecretKey").Value;
            }
            catch (Exception ex)
            {
                if ((ex is FileNotFoundException) || (ex is DirectoryNotFoundException))
                {
                    throw new InvalidCredentialException("Please, provide your own AWS credentials!");
                }
                throw;
            }
        }
    }
}