using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests.Poco
{
    [TestFixture]
    public class PocoCreationTests : DataContextTestBase
    {
        public override void SetUp()
        {
            this.Context = TestConfiguration.GetDataContext();
        }

        public override void TearDown()
        {
        }

        [Test]
        public void DataContext_EntityCreation_PersistsRecordToDynamoDb()
        {
            var book = BookPocosHelper.CreateBookPoco(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);
            Assert.IsNotNull(storedBookPoco);
        }

        [Ignore("This behavior is currently expected. SubmitChanges() uses DocumentBatchWrite, which only supports PUT operations, which by default replaces existing entities")]
        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenEntityAlreadyExistsInDynamoDbButWasNeverQueriedInCurrentContext()
        {
            var book = BookPocosHelper.CreateBookPoco(popularityRating: BookPoco.Popularity.Average);

            book.PopularityRating = BookPoco.Popularity.High;

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenTryingToAddSameEntityTwice()
        {
            var book = BookPocosHelper.CreateBookPoco(popularityRating: BookPoco.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            book.PopularityRating = BookPoco.Popularity.High;

            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenEntityPreviouslyStoredInDynamoDbWasQueriedInCurrentContext()
        {
            var book = BookPocosHelper.CreateBookPoco(popularityRating: BookPoco.Popularity.Average);

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.Find(book.Name, book.PublishYear);

            book.PopularityRating = BookPoco.Popularity.High;

            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }

        [Test]
        public void DataContext_EntityCreation_StoresComplexObjectProperties()
        {
            var book = BookPocosHelper.CreateBookPoco(persistToDynamoDb: false, publisher: new BookPoco.PublisherDto { Title = "O’Reilly Media", Address = "Sebastopol, CA" });

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.Publisher.ToString(), storedBookPoco.Publisher.ToString(), "Complex object properties are not equal");

            storedBookPoco.Publisher = new BookPoco.PublisherDto { Title = "O’Reilly Media", Address = "Illoqortormiut, Greenland" };

            this.Context.SubmitChanges();

            var storedBookPoco2 = booksTable.Find(book.Name, book.PublishYear);

            Assert.AreEqual(storedBookPoco2.Publisher.ToString(), storedBookPoco.Publisher.ToString(), "Complex object properties are not equal after updating");
        }


        [Test]
        public void DataContext_EntityCreation_StoresComplexObjectListProperties()
        {
            var book = BookPocosHelper.CreateBookPoco(persistToDynamoDb: false, reviews: new List<BookPoco.ReviewDto> { new BookPoco.ReviewDto { Author = "Beavis", Text = "Cool" }, new BookPoco.ReviewDto { Author = "Butt-head", Text = "This sucks!" } });

            var booksTable = this.Context.GetTable<BookPoco>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBookPoco = booksTable.Find(book.Name, book.PublishYear);

            var expectedSequence1 = string.Join(", ", book.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            var actualSequence1 = string.Join(", ", storedBookPoco.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            Assert.AreEqual(expectedSequence1, actualSequence1, "Complex object list properties are not equal");
        }

    }
}
