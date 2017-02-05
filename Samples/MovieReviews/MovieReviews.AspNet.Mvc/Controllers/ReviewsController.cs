using System.Linq;
using System.Web.Mvc;
using MovieReviews.DataModel;

namespace MovieReviews.AspNet.Mvc.Controllers
{
    public class ReviewsController : ControllerBase
    {
        public ActionResult Index(string movieId)
        {
            return PartialView((from review in _dataContext.Reviews
                where review.MovieId == movieId
                select review).ToList());
        }

        //
        // GET: /Review/Create

        public ActionResult Create(string movieId)
        {
            return View();
        }

        //
        // POST: /Review/Create

        [HttpPost]
        public ActionResult Create(string movieId, FormCollection collection)
        {
            try
            {
                // TODO: Add insert logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        //
        // GET: /Review/Edit/5

        public ActionResult Edit(string movieId, int id)
        {
            return View();
        }

        //
        // POST: /Review/Edit/5

        [HttpPost]
        public ActionResult Edit(string movieId, int id, FormCollection collection)
        {
            try
            {
                // TODO: Add update logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }

        //
        // GET: /Review/Delete/5

        public ActionResult Delete(string movieId, int id)
        {
            return View();
        }

        //
        // POST: /Review/Delete/5

        [HttpPost]
        public ActionResult Delete(string movieId, int id, FormCollection collection)
        {
            try
            {
                // TODO: Add delete logic here

                return RedirectToAction("Index");
            }
            catch
            {
                return View();
            }
        }
    }
}