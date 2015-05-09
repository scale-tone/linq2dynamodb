using System.Collections.Generic;

namespace Linq2DynamoDb.DataContext.Tests.Entities
{
	public class BooksComparer : IEqualityComparer<Book>
	{
		public bool Equals(Book x, Book y)
		{
			return x.Name == y.Name && x.PublishYear == y.PublishYear;
		}

		public int GetHashCode(Book obj)
		{
			return string.Format("{0}:{1}", obj.Name, obj.PublishYear).GetHashCode();
		}
	}
}