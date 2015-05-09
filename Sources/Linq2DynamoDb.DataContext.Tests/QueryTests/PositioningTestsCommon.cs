using System;
using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.QueryTests
{
    public abstract class PositioningTestsCommon : DataContextTestBase
	{
        // ReSharper disable InconsistentNaming
		[Test]
		public void DateContext_Query_FirstFunctionReturnsFirstElementWithoutPredicate()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var firstElement = booksQuery.First();

			Assert.AreEqual(bookRev1.PublishYear, firstElement.PublishYear);
		}

		[Test]
		public void DateContext_Query_FirstFunctionReturnsFirstElementMatchingPredicate()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			var bookRev2 = BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var firstElement = booksQuery.First(book => book.PublishYear == bookRev2.PublishYear);

			Assert.AreEqual(bookRev2.PublishYear, firstElement.PublishYear);
		}

		[Test]
		public void DateContext_Query_FirstOrDefaultFunctionReturnsDefaultValueIfNotFound()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var firstElement = booksQuery.FirstOrDefault(book => book.Name == null);

			Assert.IsNull(firstElement);
		}

		[Test]
		public void DateContext_Query_LastFunctionReturnsFirstElementWithoutPredicate()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			var bookRev2 = BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var lastElement = booksQuery.Last();

			Assert.AreEqual(bookRev2.PublishYear, lastElement.PublishYear);
		}

		[Test]
		public void DateContext_Query_LastFunctionReturnsFirstElementMatchingPredicate()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var lastElement = booksQuery.Last(book => book.PublishYear == bookRev1.PublishYear);

			Assert.AreEqual(bookRev1.PublishYear, lastElement.PublishYear);
		}

		[Test]
		public void DateContext_Query_LastOrDefaultFunctionReturnsDefaultValueIfNotFound()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var lastElement = booksQuery.LastOrDefault(book => book.Name == null);

			Assert.IsNull(lastElement);
		}

		[Test]
		public void DateContext_Query_ElementAtReturnsCorrectElement()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			var bookRev2 = BooksHelper.CreateBook(bookRev1.Name, 2013);
			BooksHelper.CreateBook(bookRev1.Name, 2014);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var positionedBook = booksQuery.ElementAt(1);

			Assert.AreEqual(bookRev2.PublishYear, positionedBook.PublishYear);
		}

		[Test]
		[ExpectedException(typeof(ArgumentOutOfRangeException))]
		public void DateContext_Query_ElementAtThrowsArgumentOutOfRangeExceptionWhenInvalidIndexSpecified()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			// ReSharper disable once ReturnValueOfPureMethodIsNotUsed
			booksQuery.ElementAt(5);
		}

		[Test]
		public void DateContext_Query_ElementAtOrDefaultReturnsDefaultValueIfNotFound()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var positionedBook = booksQuery.ElementAtOrDefault(4);

			Assert.IsNull(positionedBook);
		}

		[Test]
		public void DateContext_Query_SkipsSpecifiedNumberOfRecords()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);
			BooksHelper.CreateBook(bookRev1.Name, 2014);
			var bookRev4 = BooksHelper.CreateBook(bookRev1.Name, 2015);
			var bookRev5 = BooksHelper.CreateBook(bookRev1.Name, 2016);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var querySubset = booksQuery.Skip(3);

			Assert.AreEqual(2, querySubset.Count());

			var subsetList = querySubset.ToList();

			Assert.IsTrue(subsetList.Contains(bookRev4, new BooksComparer()));
			Assert.IsTrue(subsetList.Contains(bookRev5, new BooksComparer()));
		}

		[Test]
		public void DateContext_Query_SkipsWhileConditionIsTrue()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);
			BooksHelper.CreateBook(bookRev1.Name, 2014);
			var bookRev4 = BooksHelper.CreateBook(bookRev1.Name, 2015);
			var bookRev5 = BooksHelper.CreateBook(bookRev1.Name, 2016);

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == bookRev1.Name select record;

			var querySubset = booksQuery.SkipWhile(book => book.PublishYear < bookRev4.PublishYear);

			Assert.AreEqual(2, querySubset.Count());

			var subsetList = querySubset.ToList();

			Assert.IsTrue(subsetList.Contains(bookRev4, new BooksComparer()));
			Assert.IsTrue(subsetList.Contains(bookRev5, new BooksComparer()));
		}

		// ReSharper restore InconsistentNaming
	}
}
