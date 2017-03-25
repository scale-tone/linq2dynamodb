using Amazon.DynamoDBv2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Web.Http;
using System.Web.OData;
using Linq2DynamoDb.DataContext.Caching;

namespace Linq2DynamoDb.WebApi.OData
{
    using DataContext;

    /// <summary>
    /// Allows to build an Web API OData controller on top of Linq2DynamoDb.DataContext
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    [ODataFormatting]
    public abstract class DynamoDbController<TEntity> : ODataController where TEntity : class
    {
        #region ctors

        public DynamoDbController(IAmazonDynamoDB client, object hashKeyValue, Func<ITableCache> cacheImplementationFactory)
        {
            this._dataContext = new DataContext(client);
            this._hashKeyValue = hashKeyValue;
            this._cacheImplementationFactory = cacheImplementationFactory;
        }

        public DynamoDbController(IAmazonDynamoDB client, object hashKeyValue = null) : this(client, hashKeyValue, null)
        {
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Implements the 'Retrieve' operation.
        /// </summary>
        [EnableQuery]
        public async Task<IHttpActionResult> Get([FromODataUri] object key)
        {
            if (key == null)
            {
                return this.Ok(this.GetTable());
            }

            TEntity result;
            try
            {
                result = await this.GetTable().FindAsync(key);
            }
            catch (InvalidOperationException)
            {
                return this.NotFound();
            }

            var resultAsQueryable = new List<TEntity>() { result }.AsQueryable();
            return this.Ok(new SingleResult<TEntity>(resultAsQueryable));
        }

        /// <summary>
        /// Implements the 'Create' operation.
        /// </summary>
        public async Task<IHttpActionResult> Post(TEntity product)
        {
            if (!this.ModelState.IsValid)
            {
                return this.BadRequest(this.ModelState);
            }

            this.GetTable().InsertOnSubmit(product);
            await this._dataContext.SubmitChangesAsync();

            return this.Created(product);
        }

        /// <summary>
        /// Implements the 'Update' operation.
        /// </summary>
        public async Task<IHttpActionResult> Patch([FromODataUri] object key, Delta<TEntity> product)
        {
            if (!this.ModelState.IsValid)
            {
                return this.BadRequest(ModelState);
            }

            var entity = await this.GetTable().FindAsync(key);
            if (entity == null)
            {
                return this.NotFound();
            }
            product.Patch(entity);
            await this._dataContext.SubmitChangesAsync();

            return this.Updated(entity);
        }

        /// <summary>
        /// Implements the 'Update' operation.
        /// </summary>
        public async Task<IHttpActionResult> Put([FromODataUri] object key, TEntity update)
        {
            if (!this.ModelState.IsValid)
            {
                return this.BadRequest(this.ModelState);
            }

            this.GetTable().InsertOnSubmit(update);
            await this._dataContext.SubmitChangesAsync();

            return this.Updated(update);
        }

        /// <summary>
        /// Implements the 'Delete' operation.
        /// </summary>
        public async Task<IHttpActionResult> Delete([FromODataUri] object key)
        {
            var product = await this.GetTable().FindAsync(key);
            if (product == null)
            {
                return this.NotFound();
            }

            this.GetTable().RemoveOnSubmit(product);
            await this._dataContext.SubmitChangesAsync();

            return this.StatusCode(HttpStatusCode.NoContent);
        }

        #endregion

        #region Private Properties

        private readonly DataContext _dataContext;

        private readonly object _hashKeyValue;

        private readonly Func<ITableCache> _cacheImplementationFactory;

        #endregion

        #region Private Methods

        protected DataTable<TEntity> GetTable()
        {
            return this._dataContext.GetTable<TEntity>(this._hashKeyValue, this._cacheImplementationFactory);
        }

        #endregion
    }
}