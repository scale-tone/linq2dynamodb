using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Linq2DynamoDb.DataContext.Tests.Entities;
using NSubstitute;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests
{
    [TestFixture]
    public class DataContextTests
    {
        [Test]
        public void DataContext()
        {
            var client = TestConfiguration.GetDynamoDbClient();
            var ctx = new DataContext(client);
            ctx.CreateTableIfNotExists(
                new CreateTableArgs<Book>(
                    1,
                    1,
                    r => r.Name,
                    r => r.PublishYear,
                    null,
                    null,
                    () =>
                        new[]
                        {
                            new Book
                            {
                                Name = "TestBook" + Guid.NewGuid(),
                                PublishYear = 2016,
                                Author = "foo",
                                NumPages = 25,
                                PopularityRating = default(Book.Popularity),
                                UserFeedbackRating = default(Book.Stars),
                                RentingHistory = default(List<string>),
                                FilmsBasedOnBook = default(IDictionary<string, TimeSpan>),
                                LastRentTime = default(DateTime),
                                Publisher = default(Book.PublisherDto),
                                ReviewsList = default(List<Book.ReviewDto>)
                            }
                        }));

            var table = ctx.GetTable<Book>();

            //TODO: figure out a way to directly prove that the tablename prefix is getting applied, rather than this indirect approach
            var result = table.FirstOrDefault();
            Assert.IsNotNull(result);
        }

        /// <remarks>Set to run 10 (magic number) times, as race conditions are hard to get to consistently fail.</remarks>
        [Test, Repeat(10)]
        public async Task GetTableDefinitionRaceConditionRunningSomewhatInParallelSynchronised()
        {
            //setup fake dynamodb interactions
            var dynamoDbClient = Substitute.For<IAmazonDynamoDB>();
            dynamoDbClient.DescribeTable(Arg.Any<DescribeTableRequest>()).Returns(new DescribeTableResponse
            {
                Table = new TableDescription { TableStatus = TableStatus.ACTIVE, KeySchema = new List<KeySchemaElement> { new KeySchemaElement("key", KeyType.HASH) }, AttributeDefinitions = new List<AttributeDefinition> { new AttributeDefinition("key", ScalarAttributeType.S) } }
            });

            //subject
            var context = new DataContext(dynamoDbClient);
            context.CreateTableIfNotExists(new CreateTableArgs<Book>(c => c.Name));

            const int numberOfRacers = 10;

            //exercise
            var checkeredFlag = new SemaphoreSlim(0);
            var racers = Enumerable.Range(0, numberOfRacers).Select(i => Task.Run(async () =>
            {
                await checkeredFlag.WaitAsync();
                return context.GetTable<Book>();
            }));
            checkeredFlag.Release(numberOfRacers);
            var tables = await Task.WhenAll(racers);
            var actual = tables.OfType<ITableCudOperations>().Select(t => t.TableWrapper).ToList();

            //assert
            var expected = Enumerable.Repeat(actual.First(), numberOfRacers).ToList();
            CollectionAssert.AreEqual(expected, actual);
        }

        /// <remarks>Set to run 10 (magic number) times, as race conditions are hard to get to consistently fail.</remarks>
        [Test, Repeat(10)]
        public async Task GetTableDefinitionRaceConditionRunningAsCloseToParallelSynchronised()
        {
            //setup fake dynamodb interactions
            var dynamoDbClient = Substitute.For<IAmazonDynamoDB>();
            dynamoDbClient.DescribeTable(Arg.Any<DescribeTableRequest>()).Returns(new DescribeTableResponse
            {
                Table = new TableDescription { TableStatus = TableStatus.ACTIVE, KeySchema = new List<KeySchemaElement> { new KeySchemaElement("key", KeyType.HASH) }, AttributeDefinitions = new List<AttributeDefinition> { new AttributeDefinition("key", ScalarAttributeType.S) } }
            });

            //subject
            var context = new DataContext(dynamoDbClient);
            context.CreateTableIfNotExists(new CreateTableArgs<Book>(c => c.Name));

            const int numberOfRacers = 10;

            //exercise
            var flagman = new AsyncBarrier(numberOfRacers);
            var racers = Enumerable.Range(0, numberOfRacers).Select(i => Task.Run(async () =>
            {
                await flagman.SignalAndWait();
                return context.GetTable<Book>();
            }));

            var tables = await Task.WhenAll(racers);
            var actual = tables.OfType<ITableCudOperations>().Select(t => t.TableWrapper).ToList();

            //assert
            var expected = Enumerable.Repeat(actual.First(), numberOfRacers).ToList();
            CollectionAssert.AreEqual(expected, actual);
        }
    }

    /// <summary>
    /// Thanks to Stephen Toub :)
    /// http://blogs.msdn.com/b/pfxteam/archive/2012/02/11/10266932.aspx
    /// </summary>
    public class AsyncBarrier
    {
        private readonly int _participantCount;
        private int _remainingParticipants;
        private ConcurrentStack<TaskCompletionSource<bool>> m_waiters;

        public AsyncBarrier(int participantCount)
        {
            if (participantCount <= 0) throw new ArgumentOutOfRangeException(nameof(participantCount));
            _remainingParticipants = _participantCount = participantCount;
            m_waiters = new ConcurrentStack<TaskCompletionSource<bool>>();
        }

        public Task SignalAndWait()
        {
            var tcs = new TaskCompletionSource<bool>();
            m_waiters.Push(tcs);
            if (Interlocked.Decrement(ref _remainingParticipants) == 0)
            {
                _remainingParticipants = _participantCount;
                var waiters = m_waiters;
                m_waiters = new ConcurrentStack<TaskCompletionSource<bool>>();
                Parallel.ForEach(waiters, w => w.SetResult(true));
            }
            return tcs.Task;
        }
    }
}
