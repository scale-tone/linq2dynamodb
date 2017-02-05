using System;
using System.Diagnostics;
using System.Text;
using System.Web.UI;
using MovieReviews.DataModel;

namespace MovieReviews.AspNet.BackEnd
{
    public partial class _Default : Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
        }

        static readonly Random Rnd = new Random(DateTime.Now.Millisecond);

        static string GetRandomString(int length)
        {
            var sb = new StringBuilder();
            for (int i = 0; i < length; i++)
            {
                sb.Append((char)('A' + Rnd.Next(0, 26)));
            }
            return sb.ToString();
        }

        protected void Button1_Click(object sender, EventArgs e)
        {
            var ctx = new ReviewsDataContext();

            var newReview = new Review()
                {
                    MovieId = "Thriller-Terminator 123",
                    Reviewer = GetRandomString(5),
                    Rating = RatingEnum.Awful,
                    Text = "blah-blah"
                };

            ctx.Reviews.InsertOnSubmit(newReview);

            ctx.SubmitChanges();
        }

        protected void Button2_Click(object sender, EventArgs e)
        {
            var ctx = new ReviewsDataContext();

            foreach (var g in ctx.Genres)
            {
                Debug.WriteLine(g.IgnoredField);
            }


            foreach (var r in ctx.Reviewers)
            {
                Debug.WriteLine(r.Login);
            }

            ctx.Reviewers.InsertOnSubmit(new Reviewer() { Login = GetRandomString(5) });

            ctx.SubmitChanges();
        }
    }
}