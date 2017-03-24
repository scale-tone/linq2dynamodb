﻿using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.EntityManagementTests
{
    [TestFixture]
    public class EntityCreationTests : DataContextTestBase
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
            var book = BooksHelper.CreateBook(persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBook = booksTable.Find(book.Name, book.PublishYear);
            Assert.IsNotNull(storedBook);
        }

        [Test]
        [ExpectedException(typeof(AggregateException))]
        public void DataContext_EntityCreation_ThrowsExceptionWhenEntityAlreadyExistsInDynamoDbButWasNeverQueriedInCurrentContext()
        {
            var persistedBook = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average);

            var bookCopy = BooksHelper.CreateBook(name: persistedBook.Name, publishYear: persistedBook.PublishYear, persistToDynamoDb: false);

            bookCopy.PopularityRating = Book.Popularity.High;

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(bookCopy);
            this.Context.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenTryingToAddSameEntityTwice()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average, persistToDynamoDb: false);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            book.PopularityRating = Book.Popularity.High;

            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException), ExpectedMessage = "cannot be added, because entity with that key already exists", MatchType = MessageMatch.Contains)]
        public void DataContext_EntityCreation_ThrowsExceptionWhenEntityPreviouslyStoredInDynamoDbWasQueriedInCurrentContext()
        {
            var book = BooksHelper.CreateBook(popularityRating: Book.Popularity.Average);

            var booksTable = this.Context.GetTable<Book>();
            booksTable.Find(book.Name, book.PublishYear);

            book.PopularityRating = Book.Popularity.High;

            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();
        }

        [Test]
        public void DataContext_EntityCreation_StoresComplexObjectProperties()
        {
            var book = BooksHelper.CreateBook(persistToDynamoDb: false, publisher: new Book.PublisherDto { Title = "O’Reilly Media", Address = "Sebastopol, CA" });

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBook = booksTable.Find(book.Name, book.PublishYear);
            Assert.AreEqual(book.Publisher.ToString(), storedBook.Publisher.ToString(), "Complex object properties are not equal");

            storedBook.Publisher = new Book.PublisherDto { Title = "O’Reilly Media", Address = "Illoqortormiut, Greenland" };

            this.Context.SubmitChanges();

            var storedBook2 = booksTable.Find(book.Name, book.PublishYear);

            Assert.AreEqual(storedBook2.Publisher.ToString(), storedBook.Publisher.ToString(), "Complex object properties are not equal after updating");
        }


        [Test]
        public void DataContext_EntityCreation_StoresComplexObjectListProperties()
        {
            var book = BooksHelper.CreateBook(persistToDynamoDb: false, reviews: new List<Book.ReviewDto> { new Book.ReviewDto { Author = "Beavis", Text = "Cool" }, new Book.ReviewDto { Author = "Butt-head", Text = "This sucks!" } });

            var booksTable = this.Context.GetTable<Book>();
            booksTable.InsertOnSubmit(book);
            this.Context.SubmitChanges();

            var storedBook = booksTable.Find(book.Name, book.PublishYear);

            var expectedSequence1 = string.Join(", ", book.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            var actualSequence1 = string.Join(", ", storedBook.ReviewsList.Select(r => r.ToString()).OrderBy(s => s));
            Assert.AreEqual(expectedSequence1, actualSequence1, "Complex object list properties are not equal");
        }

    }
}
