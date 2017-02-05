using System;
using Amazon.DynamoDBv2.DataModel;
using Linq2DynamoDb.DataContext;

namespace MovieReviews.DataModel
{
    public enum RatingEnum
    {
        Awful,
        Average,
        Good,
        Excellent
    }

    [DynamoDBTable("Reviews")]
    public class Review
    {
        public Guid Id { get; set; }
        public string MovieId { get; set; }
        public string Reviewer { get; set; }
        public string Text { get; set; }
        public RatingEnum Rating { get; set; }
    }
}