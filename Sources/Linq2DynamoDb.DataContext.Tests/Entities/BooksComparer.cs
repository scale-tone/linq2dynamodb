using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext.Tests.Entities
{
	public class BooksComparer : IEqualityComparer<Book>
	{
        protected static readonly Func<object, Document> ToDocumentConverter = DynamoDbConversionUtils.ToDocumentConverter(typeof(Entities.Book));

		public bool Equals(Book x, Book y)
		{
		    var docX = ToDocumentConverter(x);
		    var docY = ToDocumentConverter(y);

		    if (docX.Count != docY.Count)
		    {
		        return false;
		    }

		    return docY.All(field => (docX.ContainsKey(field.Key)) && (field.Value.Equals(docX[field.Key])));
		}

		public int GetHashCode(Book obj)
		{
			return ToDocumentConverter(obj).GetHashCode();
		}
	}
}