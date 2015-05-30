using System.IO;
using Amazon.DynamoDBv2.DataModel;

namespace MovieReviews.DataModel
{
    [DynamoDBTable("Movies")]
    public class Movie 
    {
        public string Genre { get; set; }
        public string Title { get; set; }
        public string Director { get; set; }
        public int Year { get; set; }
        public double Budget { get; set; }
        public string Description { get; set; }
        public MemoryStream Picture { get; set; }

        public static string GetMovieId(Movie movie)
        {
            return movie.Genre + "-" + movie.Title;
        }
    }
}