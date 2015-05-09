using System.Linq;
using Linq2DynamoDb.DataContext.Tests.Entities;
using Linq2DynamoDb.DataContext.Tests.Helpers;
using NUnit.Framework;

namespace Linq2DynamoDb.DataContext.Tests.QueryTests
{
    public abstract class ConversionTestsCommon : DataContextTestBase
	{
        // ReSharper disable InconsistentNaming
		[Test]
		public void DateContext_Query_SupportsToList()
		{
			var book = BooksHelper.CreateBook();

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;

			var queryList = booksQuery.ToList();

			Assert.AreEqual(1, queryList.Count);

			var storedBook = queryList.First();

			Assert.AreEqual(book.Name, storedBook.Name);
		}

		[Test]
		public void DateContext_Query_SupportsToArray()
		{
			var book = BooksHelper.CreateBook();

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;

			var queryList = booksQuery.ToArray();

			Assert.AreEqual(1, queryList.Length);

			var storedBook = queryList.First();

			Assert.AreEqual(book.Name, storedBook.Name);
		}

		[Test]
		public void DateContext_Query_SupportsToDictionary()
		{
			var book = BooksHelper.CreateBook();

			var bookTable = Context.GetTable<Book>();
			var booksQuery = from record in bookTable where record.Name == book.Name select record;

			var i = 0;
			var queryList = booksQuery.ToDictionary(book1 => book1, book1 => i++);

			Assert.AreEqual(1, queryList.Count);

			var storedBook = queryList.First();

			Assert.AreEqual(book.Name, storedBook.Key.Name);
			Assert.AreEqual(0, storedBook.Value);
		}

		// ReSharper restore InconsistentNaming
	}
}
