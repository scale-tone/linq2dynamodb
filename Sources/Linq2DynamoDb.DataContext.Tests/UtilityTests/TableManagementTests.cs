using System;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Linq2DynamoDb.DataContext.Tests.Entities;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.UtilityTests
{
    [TestFixture]
    [Category(TestCategories.Slow)]
    public class TableManagementTests : DataContextTestBase
    {
        private string TablePrefix { get; set; }

        private string BooksTableName
        {
            get { return this.TablePrefix + typeof(Book).Name; }
        }

        private IAmazonDynamoDB DynamoDbClient { get; set; }

        public override void SetUp()
        {
            this.TablePrefix = typeof(TableManagementTests).Name + Guid.NewGuid();

            this.DynamoDbClient = TestConfiguration.GetDynamoDbClient();
            this.Context = TestConfiguration.GetDataContext(this.DynamoDbClient, this.TablePrefix);
        }

        public override void TearDown()
        {
            try
            {
                this.DynamoDbClient.DeleteTable(new DeleteTableRequest { TableName = this.BooksTableName });
                Logger.DebugFormat("Table {0} delete initiated", this.BooksTableName);
            }
            catch (ResourceNotFoundException)
            {
                Logger.DebugFormat("Table {0} does not exist", this.BooksTableName);
            }
        }

        [Test]
        public void DataContext_CreateTableIfNotExists_CreatesTableThatDoesNotExist()
        {
            var args = new CreateTableArgs<Book>(book => book.Name, book => book.PublishYear);
            this.Context.CreateTableIfNotExists(args);

            var tableData = this.DynamoDbClient.DescribeTable(new DescribeTableRequest { TableName = this.BooksTableName });
            Assert.IsNotNull(tableData, "Table was not created");
        }

        [Test]
        public void DataContext_CreateTableIfNotExists_CreatesTableThatDoesNotExistWithLocalSecondaryIndexes()
        {
            var args = new CreateTableArgs<Book>(book => book.Name, book => book.PublishYear, book => book.NumPages, book => book.PopularityRating);
            this.Context.CreateTableIfNotExists(args);
            
            var tableData = this.DynamoDbClient.DescribeTable(new DescribeTableRequest { TableName = this.BooksTableName });
            Assert.IsNotNull(tableData, "Table was not created");
            var secondaryIndexes = tableData.Table.LocalSecondaryIndexes;
            Assert.AreEqual(2, secondaryIndexes.Count, "Expected 2 local secondary indexes to be created");

            Assert.IsTrue(secondaryIndexes.Any(description => description.IndexName.Contains("NumPages")));
            Assert.IsTrue(secondaryIndexes.Any(description => description.IndexName.Contains("PopularityRating")));
        }

        [Test]
        public void DataContext_CreateTableIfNotExists_CreatesTableThatDoesNotExistWithGlobalSecondaryIndexes()
        {
            var args = new CreateTableArgs<Book>
            (
                // hash key field
                book => book.Name,
                // range key field
                book => book.PublishYear,

                // local secondary indexes
                null,

                // global secondary indexes
                new GlobalSecondaryIndexDefinitions<Book>
                {
                    book => new GlobalSecondaryIndexDefinition
                    {
                        HashKeyField = book.PublishYear, 
                    },

                    book => new GlobalSecondaryIndexDefinition
                    {
                        HashKeyField = book.Author, 
                        RangeKeyField = book.NumPages,
                        ReadCapacityUnits = 11,
                        WriteCapacityUnits = 12
                    },

                }
            );
            this.Context.CreateTableIfNotExists(args);

            var tableData = this.DynamoDbClient.DescribeTable(new DescribeTableRequest { TableName = this.BooksTableName });
            Assert.IsNotNull(tableData, "Table was not created");

            var secondaryIndexes = tableData.Table.GlobalSecondaryIndexes;
            Assert.AreEqual(2, secondaryIndexes.Count, "Expected 2 global secondary indexes to be created");

            Assert.IsTrue(secondaryIndexes.Any(description => description.IndexName.Contains("PublishYear")));
            Assert.IsTrue
            (
                secondaryIndexes.Any
                (
                    description =>
                        description.IndexName.Contains("Author")
                        &&
                        description.IndexName.Contains("NumPages")
                )
            );
        }

        [Test]
        public void DataContext_CreateTableIfNotExists_CreatesTableThatDoesNotExistWithSpecifiedCapacity()
        {
            const long ReadCapacity = 2;
            const long WriteCapacity = 3;
            var args = new CreateTableArgs<Book>(ReadCapacity, WriteCapacity, book => book.Name, book => book.PublishYear);
            this.Context.CreateTableIfNotExists(args);

            var tableData = this.DynamoDbClient.DescribeTable(new DescribeTableRequest { TableName = this.BooksTableName });
            Assert.IsNotNull(tableData, "Table was not created");

            var tableCapacity = tableData.Table.ProvisionedThroughput;
            Assert.AreEqual(ReadCapacity, tableCapacity.ReadCapacityUnits);
            Assert.AreEqual(WriteCapacity, tableCapacity.WriteCapacityUnits);
        }

        [Test]
        [ExpectedException(typeof(ResourceNotFoundException))]
        public void DataContext_DeleteTable_DeletesExistingTable()
        {
            var args = new CreateTableArgs<Book>(book => book.Name, book => book.PublishYear);
            this.Context.CreateTableIfNotExists(args);

            this.Context.DeleteTable<Book>();

            this.DynamoDbClient.DescribeTable(new DescribeTableRequest { TableName = this.BooksTableName });
        }
    }
}
