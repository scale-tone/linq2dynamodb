using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Web.Routing;

namespace MovieReviews.AspNet.Mvc
{
    public class RouteConfig
    {
        public static void RegisterRoutes(RouteCollection routes)
        {
            routes.IgnoreRoute("{resource}.axd/{*pathInfo}");

            routes.MapRoute(
                name: "Default",
                url: "{controller}/{action}/{id}",
                defaults: new { controller = "Movies", action = "Index", id = UrlParameter.Optional }
            );
            routes.MapRoute(
                name: "Reviews",
                url: "Movies/Details/{movieId}/Reviews/{action}/{id}",
                defaults: new {controller = "Reviews", action = "Index", id = UrlParameter.Optional});
        }
    }
}