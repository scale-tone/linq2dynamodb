using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Linq2DynamoDb.DataContext.Tests.Entities;

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
    }
}
