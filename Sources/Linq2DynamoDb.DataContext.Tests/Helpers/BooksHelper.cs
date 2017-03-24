using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using Linq2DynamoDb.DataContext.Tests.Entities;
using log4net;

namespace Linq2DynamoDb.DataContext.Tests.Helpers
{
	public static class BooksHelper
	{
        private static readonly ILog Logger = LogManager.GetLogger(typeof(BooksHelper));

		private static readonly IAmazonDynamoDB DynamoDbClient = TestConfiguration.GetDynamoDbClient();
		private static readonly DynamoDBContext PersistenceContext = TestConfiguration.GetDynamoDbContext();
		private static ConcurrentQueue<Book> _recordsForCleanup;

		public static void StartSession()
		{
			CreateBooksTable(TestConfiguration.TablePrefix + "Book");
			_recordsForCleanup = new ConcurrentQueue<Book>();
		}

		public static void CleanSession()
		{
            Logger.DebugFormat("Removing {0} records from DynamoDb", _recordsForCleanup.Count);

			Parallel.ForEach(_recordsForCleanup, book => PersistenceContext.Delete(book, new DynamoDBOperationConfig { SkipVersionCheck = true, ConsistentRead = true }));

			_recordsForCleanup = new ConcurrentQueue<Book>();
		}

		public static Book CreateBook(
			string name = null,
			int publishYear = default(int),
			string author = default(string),
			int numPages = default(int),
			Book.Popularity popularityRating = default(Book.Popularity),
			Book.Stars userFeedbackRating = default(Book.Stars),
			List<string> rentingHistory = default(List<string>),
			IDictionary<string, TimeSpan> filmsBasedOnBook = default(IDictionary<string, TimeSpan>),
			DateTime lastRentTime = default(DateTime),
            bool persistToDynamoDb = true,
            Book.PublisherDto publisher = default(Book.PublisherDto),
            List<Book.ReviewDto> reviews = default(List<Book.ReviewDto>))
		{
			name = name ?? "TestBook" + Guid.NewGuid();

			var book = new Book
			{
				Name = name,
				PublishYear = publishYear,
				Author = author,
				NumPages = numPages,
				PopularityRating = popularityRating,
				UserFeedbackRating = userFeedbackRating,
				RentingHistory = rentingHistory,
				FilmsBasedOnBook = filmsBasedOnBook,
				LastRentTime = lastRentTime,
                Publisher = publisher,
                ReviewsList = reviews,
			};

		    if (persistToDynamoDb)
		    {
		        Logger.DebugFormat("Persisting book: {0}", book.Name);
		        PersistenceContext.Save(book);
		    }
		    else
            {
                Logger.DebugFormat("Created in-memory book: {0}", book.Name);
		    }

		    if (_recordsForCleanup != null)
		    {
                _recordsForCleanup.Enqueue(book);
            }

			return book;
		}

		public static void CreateBooksTable(string tableName)
		{
			try
			{
				DynamoDbClient.CreateTable(
					new CreateTableRequest
					{
						TableName = tableName,
						AttributeDefinitions =
							new List<AttributeDefinition>
							{
								new AttributeDefinition { AttributeName = "Name", AttributeType = "S" },
								new AttributeDefinition { AttributeName = "PublishYear", AttributeType = "N" }
							},
						ProvisionedThroughput = new ProvisionedThroughput { ReadCapacityUnits = 5, WriteCapacityUnits = 5 },
						KeySchema =
							new List<KeySchemaElement>
							{
								new KeySchemaElement { AttributeName = "Name", KeyType = "HASH" },
								new KeySchemaElement { AttributeName = "PublishYear", KeyType = "RANGE" }
							}
					});

				Logger.DebugFormat("Created table {0}", tableName);
			}
			catch
			{
				Logger.DebugFormat("Table already existed {0}", tableName);
			}
		}
	}
}
