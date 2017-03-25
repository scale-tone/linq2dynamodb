using Amazon.DynamoDBv2.DataModel;
using System.Linq;
using System.Reflection;
using System.Web.Http;
using System.Web.OData.Builder;
using System.Web.OData.Extensions;

namespace Linq2DynamoDb.WebApi.OData
{
    /// <summary>
    /// A specific OData Model builder for Linq2DynamoDb
    /// </summary>
    public class Linq2DynamoDbModelBuilder : ODataConventionModelBuilder
    {
        /// <summary>
        /// Adds a Linq2DynamoDb entity to the Model.
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="name">The name for this OData resource. If omitted, the DynamoDB Table name is used.</param>
        /// <returns>This Linq2DynamoDbModelBuilder instance, to do fluent interface.</returns>
        public Linq2DynamoDbModelBuilder WithEntitySet<TEntity>(string name = null) where TEntity : class
        {
            if (name == null)
            {
                var entityType = typeof(TEntity);
                var entityAttributes = entityType.GetTypeInfo().GetCustomAttributes(typeof(DynamoDBTableAttribute), true);
                name = entityAttributes.Any() ? ((DynamoDBTableAttribute)entityAttributes.First()).TableName : entityType.Name;
            }

            this.EntitySet<TEntity>(name);
            return this;
        }

        /// <summary>
        /// Maps the specified OData route and the OData route attributes.
        /// </summary>
        /// <param name="config">The server configuration.</param>
        /// <param name="routeName">The name of the route to map.</param>
        /// <param name="routePrefix">The prefix to add to the OData route's path template.</param>
        public void MapODataServiceRoute(HttpConfiguration config, string routeName, string routePrefix = null)
        {
            config.MapODataServiceRoute(routeName, routePrefix, model: this.GetEdmModel());
        }
    }
}
