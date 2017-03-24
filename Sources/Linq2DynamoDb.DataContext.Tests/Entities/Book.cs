using System;
using System.Collections.Generic;
using System.Globalization;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Tests.Entities
{
	[DynamoDBTable("Book")]
	public class Book : EntityBase
	{
		[DynamoDBHashKey]
		public string Name { get; set; }

		[DynamoDBRangeKey]
		public int PublishYear { get; set; }

		public int NumPages { get; set; }

		public string Author { get; set; }

		[DynamoDBProperty(typeof(PopularityConverter))]
		public Popularity PopularityRating { get; set; }

		[DynamoDBProperty(typeof(StarsConverter))]
		public Stars UserFeedbackRating { get; set; }

		public List<string> RentingHistory { get; set; }

		public DateTime LastRentTime { get; set; }

		[DynamoDBProperty(typeof(StringTimeSpanDictionaryConverter))]
		public IDictionary<string, TimeSpan> FilmsBasedOnBook { get; set; }

        [DynamoDBVersion]
        public int? VersionNumber { get; set; }

		public enum Popularity
		{
			Low,

			BelowAverage,

			Average,

			AboveAverage,

			High
		}

		public enum Stars
		{
			None = 0,
			Bronze = 1,
			Silver = 2,
			Gold = 3,
			Platinum = 4,
			Diamond = 5
		}

		private class PopularityConverter : IPropertyConverter
		{
			public DynamoDBEntry ToEntry(object value)
			{
				return new Primitive(value.ToString());
			}

			public object FromEntry(DynamoDBEntry entry)
			{
				return Enum.Parse(typeof(Popularity), entry.AsString());
			}
		}

		private class StarsConverter : IPropertyConverter
		{
			public DynamoDBEntry ToEntry(object value)
			{
				var enumValue = (int)value;
				return new Primitive(enumValue.ToString(CultureInfo.InvariantCulture), true);
			}

			public object FromEntry(DynamoDBEntry entry)
			{
				return Enum.Parse(typeof(Stars), entry.AsString());
			}
		}

		private class StringTimeSpanDictionaryConverter : IPropertyConverter
		{
			public DynamoDBEntry ToEntry(object value)
			{
                if (value == null)
                {
                    return null;
                }

				var dictionary = (IDictionary<string, TimeSpan>)value;
				var primitiveList = new PrimitiveList(DynamoDBEntryType.String);
				foreach (var keyValuePair in dictionary)
				{
					primitiveList.Add(new Primitive(string.Format("{0}@{1}", keyValuePair.Key, keyValuePair.Value)));
				}

				return primitiveList;
			}

			public object FromEntry(DynamoDBEntry entry)
			{
                if ((entry == null) || (entry is DynamoDBNull))
                {
                    return null;
                }

				var list = entry.AsListOfString();
				var dictionary = new Dictionary<string, TimeSpan>();
				foreach (var record in list)
				{
					var split = record.Split('@');

					var key = split[0];
					var value = TimeSpan.Parse(split[1]);

					dictionary.Add(key, value);
				}

				return dictionary;
			}
		}

        public class PublisherDto
        {
            public string Title { get; set; }
            public string Address { get; set; }

            public override string ToString()
            {
                return this.Title + this.Address;
            }
        }

        public PublisherDto Publisher { get; set; }

        public class ReviewDto
        {
            public string Author { get; set; }
            public string Text { get; set; }

            public override string ToString()
            {
                return this.Author + this.Text;
            }
        }

        public List<ReviewDto> ReviewsList { get; set; } 
    }
}
