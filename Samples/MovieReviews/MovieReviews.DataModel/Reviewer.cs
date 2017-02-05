
namespace MovieReviews.DataModel
{
    public interface Reviewers
    {
        string Login { get; set; }
        string Password { get; set; }
    }

    public class Reviewer : Reviewers
    {
        public string Login { get; set; }
        public string Password { get; set; }
    }
}