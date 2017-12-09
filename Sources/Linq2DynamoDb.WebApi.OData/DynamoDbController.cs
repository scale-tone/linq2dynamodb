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

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class from a pre-configured Linq2DynamoDb.DataContext instance.
        /// </summary>
        /// <param name="dataContext">The pre-configured instance of DataContext</param>
        /// <param name="hashKeyValueFunc">A functor for getting a predefined value for the HashKey field. You can do some user authentication inside this functor and return the resulting userId.</param>
        /// <param name="cacheImplementationFactory">A functor, that returns ITableCache implementation instance, which will be used for caching DynamoDB data.</param>
        public DynamoDbController(DataContext dataContext, Func<object> hashKeyValueFunc, Func<ITableCache> cacheImplementationFactory)
        {
            this._dataContext = dataContext;
            this._hashKeyValueFunc = hashKeyValueFunc;
            this._cacheImplementationFactory = cacheImplementationFactory;
        }

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class from a pre-configured Linq2DynamoDb.DataContext instance.
        /// </summary>
        /// <param name="dataContext">The pre-configured instance of DataContext</param>
        /// <param name="hashKeyValueFunc">A functor for getting a predefined value for the HashKey field. You can do some user authentication inside this functor and return the resulting userId.</param>
        public DynamoDbController(DataContext dataContext, Func<object> hashKeyValueFunc)
        {
            this._dataContext = dataContext;
            this._hashKeyValueFunc = hashKeyValueFunc;
        }

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class from a pre-configured Linq2DynamoDb.DataContext instance.
        /// </summary>
        /// <param name="dataContext">The pre-configured instance of DataContext</param>
        public DynamoDbController(DataContext dataContext)
        {
            this._dataContext = dataContext;
        }

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class.
        /// </summary>
        /// <param name="client">IAmazonDynamoDB instance to be used to communicate with DynamoDB</param>
        /// <param name="tableNamePrefix">A prefix to be added to table names. Allows to switch betweeen different environments.</param>
        /// <param name="hashKeyValueFunc">A functor for getting a predefined value for the HashKey field. You can do some user authentication inside this functor and return the resulting userId.</param>
        /// <param name="cacheImplementationFactory">A functor, that returns ITableCache implementation instance, which will be used for caching DynamoDB data.</param>
        public DynamoDbController(IAmazonDynamoDB client, string tableNamePrefix, Func<object> hashKeyValueFunc, Func<ITableCache> cacheImplementationFactory)
        {
            this._dataContext = new DataContext(client, tableNamePrefix);
            this._hashKeyValueFunc = hashKeyValueFunc;
            this._cacheImplementationFactory = cacheImplementationFactory;
        }

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class.
        /// </summary>
        /// <param name="client">IAmazonDynamoDB instance to be used to communicate with DynamoDB</param>
        /// <param name="tableNamePrefix">A prefix to be added to table names. Allows to switch betweeen different environments.</param>
        /// <param name="hashKeyValueFunc">A functor for getting a predefined value for the HashKey field. You can do some user authentication inside this functor and return the resulting userId.</param>
        public DynamoDbController(IAmazonDynamoDB client, string tableNamePrefix, Func<object> hashKeyValueFunc = null) : this(client, tableNamePrefix, hashKeyValueFunc, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class.
        /// </summary>
        /// <param name="client">IAmazonDynamoDB instance to be used to communicate with DynamoDB</param>
        /// <param name="tableNamePrefix">A prefix to be added to table names. Allows to switch betweeen different environments.</param>
        /// <param name="cacheImplementationFactory">A functor, that returns ITableCache implementation instance, which will be used for caching DynamoDB data.</param>
        public DynamoDbController(IAmazonDynamoDB client, string tableNamePrefix, Func<ITableCache> cacheImplementationFactory) : this(client, tableNamePrefix, null, cacheImplementationFactory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class.
        /// If a table name prefix is specified in config, it will be picked up automatically.
        /// </summary>
        /// <param name="client">IAmazonDynamoDB instance to be used to communicate with DynamoDB</param>
        /// <param name="cacheImplementationFactory">A functor, that returns ITableCache implementation instance, which will be used for caching DynamoDB data.</param>
        public DynamoDbController(IAmazonDynamoDB client, Func<ITableCache> cacheImplementationFactory) : this(client)
        {
            this._cacheImplementationFactory = cacheImplementationFactory;
        }

        /// <summary>
        /// Initializes a new instance of the DynamoDbController class.
        /// If a table name prefix is specified in config, it will be picked up automatically.
        /// </summary>
        /// <param name="client">IAmazonDynamoDB instance to be used to communicate with DynamoDB</param>
        /// <param name="hashKeyValueFunc">A functor for getting a predefined value for the HashKey field. You can do some user authentication inside this functor and return the resulting userId.</param>
        public DynamoDbController(IAmazonDynamoDB client, Func<object> hashKeyValueFunc = null)
        {
            this._dataContext = new DataContext(client);
            this._hashKeyValueFunc = hashKeyValueFunc;
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

            var result = await this.GetTable().TryFindAsync(key);
            if (result == null)
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

            var entity = await this.GetTable().TryFindAsync(key);
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
            var entity = await this.GetTable().TryFindAsync(key);
            if (entity == null)
            {
                return this.NotFound();
            }

            this.GetTable().RemoveOnSubmit(entity);
            await this._dataContext.SubmitChangesAsync();

            return this.StatusCode(HttpStatusCode.NoContent);
        }

        #endregion

        #region Private Properties

        private readonly DataContext _dataContext;

        private readonly Func<object> _hashKeyValueFunc;

        private readonly Func<ITableCache> _cacheImplementationFactory;

        #endregion

        #region Private Methods

        protected DataTable<TEntity> GetTable()
        {
            var hashKeyValue = this._hashKeyValueFunc == null ? null : this._hashKeyValueFunc();
            return this._dataContext.GetTable<TEntity>(hashKeyValue, this._cacheImplementationFactory);
        }

        #endregion
    }
}