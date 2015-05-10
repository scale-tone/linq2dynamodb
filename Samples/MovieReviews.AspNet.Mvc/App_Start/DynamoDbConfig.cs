using MovieReviews.DataModel;

namespace MovieReviews.AspNet.Mvc
{
    public static class DynamoDbConfig
    {
        public static void RegisterContext()
        {
            typeof(ReviewsDataContext).ToString();
        }
    }
}