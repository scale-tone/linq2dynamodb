using Linq2DynamoDb.WebApi.OData;
using MobileNotes.WebApi.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;

namespace MobileNotes.WebApi
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            new Linq2DynamoDbModelBuilder()
                .WithEntitySet<Note>("Notes")
                .MapODataServiceRoute(config, "ODataRoute");
        }
    }
}
