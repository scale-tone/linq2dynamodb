using System.Web.Mvc;
using MovieReviews.DataModel;

namespace MovieReviews.AspNet.Mvc.Controllers
{
    public class ControllerBase : Controller
    {
        protected ReviewsDataContext _dataContext = new ReviewsDataContext();
    }
}