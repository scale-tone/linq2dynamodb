using System.Collections.Generic;
using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.QueryTests
{
    public abstract class QueryInteractionsTestsCommon : DataContextTestBase
	{
        // ReSharper disable InconsistentNaming
        [Ignore("Seems to be a bug in DynamoDb: conditions for the same field are combined with OR operator instead of AND")]
        [Test]
        public void DateContext_Query_QueryResultSubqueryReturnsSameRecordSetIfSearchCritereaExpanded()
        {
            var book = BooksHelper.CreateBook();
            // Create another book that will be stored as second+ record in table
            BooksHelper.CreateBook();

            var bookTable = Context.GetTable<Book>();
            var booksQuery = from record in bookTable where record.Name == book.Name select record;
            // Expand search criterea to also match another record(s)
            // Keep in mind that we still have first query as strict hash-key equality which should not be overriden by subqueries
            var subQuery = from record in booksQuery where record.Name != null select record;
            Assert.AreEqual(1, subQuery.Count());

            var storedBook = subQuery.First();

            Assert.AreEqual(book.Name, storedBook.Name);
        }

        [Test]
        public void DateContext_Query_QueryResultSubqueryReturnsShrinkedRecordSet()
        {
            // ReSharper disable once RedundantArgumentDefaultValue
            var book = BooksHelper.CreateBook(publishYear: 0);
            var book2 = BooksHelper.CreateBook(book.Name, 1);
            // Another book that will fit subquery search criterea
            BooksHelper.CreateBook(publishYear: book2.PublishYear);

            var bookTable = Context.GetTable<Book>();
            var booksQuery = from record in bookTable where record.Name == book.Name select record;

            var subQuery = from record in booksQuery where record.PublishYear == book2.PublishYear select record;
            Assert.AreEqual(1, subQuery.Count());

            var storedBook = subQuery.First();

            Assert.AreEqual(book2.Name, storedBook.Name);
            Assert.AreEqual(book2.PublishYear, storedBook.PublishYear);
        }

		[Test]
		public void DateContext_Query_SupportsZipOperationBetweenTwoQueries()
		{
			var book1 = BooksHelper.CreateBook(publishYear: 2012);
			var book2 = BooksHelper.CreateBook(publishYear: 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery1 = from record in bookTable where record.Name == book1.Name select record;
			var booksQuery2 = from record in bookTable where record.Name == book2.Name select record;

			// AsEnumerable should be used for such linq requests by design
			var zipResult = booksQuery1.AsEnumerable().Zip(
				booksQuery2,
				(_book1, _book2) => new Book { Name = _book1.Name + _book2.Name, PublishYear = _book1.PublishYear + _book2.PublishYear });

			Assert.AreEqual(1, zipResult.Count());

			var combinedBook = zipResult.First();

			Assert.AreEqual(book1.Name + book2.Name, combinedBook.Name);
			Assert.AreEqual(book1.PublishYear + book2.PublishYear, combinedBook.PublishYear);
		}

		[Test]
		public void DateContext_Query_UnionReturnsCombinationOfTwoQueries()
		{
			var book1 = BooksHelper.CreateBook();
			var book2 = BooksHelper.CreateBook();

			var bookTable = Context.GetTable<Book>();
			var booksQuery1 = from record in bookTable where record.Name == book1.Name select record;
			var booksQuery2 = from record in bookTable where record.Name == book2.Name select record;

			// AsEnumerable should be used for such linq requests by design
			var unionResult = booksQuery1.AsEnumerable().Union(booksQuery2);

			Assert.AreEqual(2, unionResult.Count());

			var listResult = unionResult.ToList();
			var firstBook = listResult[0];
			var secondBook = listResult[1];

			Assert.AreEqual(book1.Name, firstBook.Name);
			Assert.AreEqual(book2.Name, secondBook.Name);
		}

		[Test]
		public void DateContext_Query_ConcatReturnsCombinationOfTwoQueries()
		{
			var book1 = BooksHelper.CreateBook();
			var book2 = BooksHelper.CreateBook();

			var bookTable = Context.GetTable<Book>();
			var booksQuery1 = from record in bookTable where record.Name == book1.Name select record;
			var booksQuery2 = from record in bookTable where record.Name == book2.Name select record;

			// AsEnumerable should be used for such linq requests by design
			var concatResult = booksQuery1.AsEnumerable().Concat(booksQuery2);

			Assert.AreEqual(2, concatResult.Count());

			var resultAsList = concatResult.ToList();
			Assert.IsTrue(resultAsList.Contains(book1, new BooksComparer()));
			Assert.IsTrue(resultAsList.Contains(book2, new BooksComparer()));
		}

		[Test]
		public void DateContext_Query_DistinctReturnsFilteredCollectionWithoutDuplicates()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			BooksHelper.CreateBook(bookRev1.Name, 2013);

			var bookTable = Context.GetTable<Book>();
			var booksQuery1 = from record in bookTable where record.Name == bookRev1.Name select record;

			var distinctResult = booksQuery1.Distinct(new BooksByNameComparer());

			Assert.AreEqual(1, distinctResult.Count());

			var resultAsList = distinctResult.ToList();
			Assert.IsTrue(resultAsList.Contains(bookRev1, new BooksComparer()));
		}

		[Test]
        [Ignore]
        public void DateContext_Query_ExceptReturnsSubsetOfQueryNotIncludingAnotherQueryResults()
		{
			var bookRev1 = BooksHelper.CreateBook(publishYear: 2012);
			var bookRev2 = BooksHelper.CreateBook(bookRev1.Name, 2013);
			var bookRev3 = BooksHelper.CreateBook(bookRev1.Name, 2014);
			BooksHelper.CreateBook(bookRev1.Name, 2015);

			var bookTable = Context.GetTable<Book>();
			// ReSharper disable once ImplicitlyCapturedClosure
			var booksQuery1 = from record in bookTable where record.Name == bookRev1.Name select record;
			var booksQuery2 = from record in bookTable
				where record.Name == bookRev1.Name && record.PublishYear < bookRev3.PublishYear
				select record;

			var exceptResult = booksQuery1.Except(booksQuery2, new BooksComparer());

			Assert.AreEqual(2, exceptResult.Count());

			var resultAsList = exceptResult.ToList();
			Assert.IsTrue(resultAsList.Contains(bookRev1, new BooksComparer()));
			Assert.IsTrue(resultAsList.Contains(bookRev2, new BooksComparer()));
		}

		private class BooksByNameComparer : IEqualityComparer<Book>
		{
			public bool Equals(Book x, Book y)
			{
				return x.Name == y.Name;
			}

			public int GetHashCode(Book obj)
			{
				return obj.Name.GetHashCode();
			}
		}

		// ReSharper restore InconsistentNaming
	}
}
