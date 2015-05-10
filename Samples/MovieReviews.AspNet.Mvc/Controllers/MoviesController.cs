using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Web.Mvc;
using MovieReviews.DataModel;
using MvcSiteMapProvider.Web.Mvc.Filters;

namespace MovieReviews.AspNet.Mvc.Controllers
{
    public class MoviesController : ControllerBase
    {
        //
        // GET: /Movie/

        public ActionResult Index()
        {
            List<ExpandoObject> movies = (from movie in _dataContext.Movies.AsEnumerable()
                select new {movie.Title, movie.Year}.ToExpando()).ToList();
            return View(movies);
        }

        //
        // GET: /Movie/Details/5
        [SiteMapTitle("Title")]
        public ActionResult Details(string id)
        {
            ViewBag.ReviewsCount = (from review in _dataContext.Reviews
                where review.MovieId == id
                select review.MovieId).Count();
            return View((from movie in _dataContext.GetTable<Movie>()
                where movie.Title == id
                select movie).First());
        }
    }
}