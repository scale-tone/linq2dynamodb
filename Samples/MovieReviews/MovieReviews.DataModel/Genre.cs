using Amazon.DynamoDBv2.DataModel;

namespace MovieReviews.DataModel
{
    [DynamoDBTable("Genres")]
    public class Genre
    {
        public string Title { get; set; }

        [DynamoDBIgnore]
        public string IgnoredField { get { return "123"; } }

        public static Genre[] GetInitialEntities()
        {
            return new []
            {
                new Genre {Title = "Thriller"},
                new Genre {Title = "Horror"},
                new Genre {Title = "Comedy"}
            };
        }
    }
}