using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;

using Amazon;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Caching;

#if AWSSDK_1_5
using IAmazonDynamoDB = Amazon.DynamoDBv2.AmazonDynamoDB;
#else
using Amazon.DynamoDBv2;
#endif

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// The root object in the whole hierarchy
    /// </summary>
    public partial class DataContext
    {
        #region ctors

        /// <summary>
        /// This ctor takes an AmazonDynamoDBClient, that will be used to load table metadata from DynamoDb
        /// </summary>
        public DataContext(IAmazonDynamoDB client, string tableNamePrefix)
        {
            this._client = client;
            this._tableNamePrefix = tableNamePrefix;
        }

        /// <summary>
        /// This ctor is supposed to be used with GetTable() method, that takes a pre-loaded Table object
        /// </summary>
        public DataContext(string tableNamePrefix)
        {
            this._tableNamePrefix = tableNamePrefix;
        }

        /// <summary>
        /// Initializes a new instance of the Linq2DynamoDb.DataContext.DataContext class. If a table
        /// prefix is specified in config, it will be picked up automatically, rather than explicitly
        /// defining it in the constructor.
        /// </summary>
        /// <param name="client">   Returns an AmazonDynamoDb instance passed via ctor. </param>
        public DataContext(IAmazonDynamoDB client)
        {
            this._client = client;
            this._tableNamePrefix = AWSConfigsDynamoDB.Context.TableNamePrefix;
        }

        #endregion

        #region Events

        /// <summary>
        /// Occurs when DataContext wants to log some debugging info
        /// </summary>
        public event Action<string> OnLog;

        #endregion

        #region Public Properties

        /// <summary>
        /// Returns an AmazonDynamoDb instance passed via ctor
        /// </summary>
        public IAmazonDynamoDB Client { get { return this._client; } }

        #endregion

        #region Public Methods

        /// <summary>
        /// Creates an instance of DataTable class for the specified entity.
        /// </summary>
        public DataTable<TEntity> GetTable<TEntity>()
        {
            return this.GetTable<TEntity>(null, null);
        }

        /// <summary>
        /// Creates an instance of DataTable class for the specified entity 
        /// with predefined HashKey value
        /// </summary>
        public DataTable<TEntity> GetTable<TEntity>(object hashKeyValue)
        {
            return this.GetTable<TEntity>(hashKeyValue, null);
        }

        /// <summary>
        /// Creates an instance of DataTable class for the specified entity. 
        /// The table contents and queries will be cached using 
        /// the provided ITableCache implementation
        /// </summary>
        public DataTable<TEntity> GetTable<TEntity>(Func<ITableCache> cacheImplementationFactory)
        {
            return this.GetTable<TEntity>(null, cacheImplementationFactory);
        }

        /// <summary>
        /// Creates an instance of DataTable class for the specified entity 
        /// with predefined HashKey value. The table contents and queries will be cached using 
        /// the provided ITableCache implementation
        /// </summary>
        public DataTable<TEntity> GetTable<TEntity>(object hashKeyValue, Func<ITableCache> cacheImplementationFactory)
        {
            return this.GetTable<TEntity>(hashKeyValue, cacheImplementationFactory, this.GetTableDefinition(typeof(TEntity)));
        }

        /// <summary>
        /// Creates an instance of DataTable class for the specified entity 
        /// with predefined HashKey value. The table contents and queries will be cached using 
        /// the provided ITableCache implementation.
        /// The provided AWS SDK's Table object is used for all communications with DynamoDb. 
        /// </summary>
        public DataTable<TEntity> GetTable<TEntity>(object hashKeyValue, Func<ITableCache> cacheImplementationFactory, Table tableDefinition)
        {
            var entityType = typeof(TEntity);

            var tableWrapper = TableWrappers.GetOrAdd
            (
                new Tuple<Type, object>(entityType, hashKeyValue),
                t => new Lazy<TableDefinitionWrapper>(() =>
                {
                    // if cache is not provided, then passing a fake implementation
                    var cacheImplementation =
                        cacheImplementationFactory == null
                        ?
                        FakeCacheImplementation
                        :
                        cacheImplementationFactory();

                    var wrapper = new TableDefinitionWrapper
                    (
                        tableDefinition,
                        entityType,
                        hashKeyValue,
                        cacheImplementation,
                        this.ConsistentRead
                    );
                    wrapper.OnLog += this.OnLog;
                    return wrapper;
                }, LazyThreadSafetyMode.ExecutionAndPublication)
            ).Value;

            if (tableWrapper.TableDefinition != tableDefinition)
            {
                throw new InvalidOperationException("You shouldn't pass different Table instances for the same entityType/hashKeyValue pair");
            }

            // this is to avoid situation, when the same DataContext instance is being used for table creation and then for cached data access.
            if 
            (
                (tableWrapper.Cache == FakeCacheImplementation)
                &&
                (cacheImplementationFactory != null)
            )
            {
                throw new InvalidOperationException("You're trying to get a DataTable<T> instance first without caching and then with cache implementation specified. This is not supported. For creating DynamoDb tables, please, use another separate instance of DataContext");
            }

            return new DataTable<TEntity>(tableWrapper);
        }

        /// <summary>
        /// Saves all modifications to DynamoDb and to cache, if one is used
        /// </summary>
        public void SubmitChanges()
        {
            Task.WaitAll(this.TableWrappers.Values.Select(t => t.Value.SubmitChangesAsync()).ToArray());
        }

        /// <summary>
        /// Asynchronously saves all modifications to DynamoDb and to cache, if one is used
        /// </summary>
        public Task SubmitChangesAsync()
        {
            return Task.WhenAll(this.TableWrappers.Values.Select(t => t.SubmitChangesAsync()).ToArray());
        }

        #endregion

        #region Private Properties

        /// <summary>
        /// It's possible to set this flag in child's constructor. But in most cases it's not necessary!
        /// That's because when using caching, the data read from DynamoDb never substitutes data in cache.
        /// And data in cache is always consistent.
        /// And don't forget, that consistent reads are supported only by Get/Query operations. 
        /// Scan is always inconsistent, so this flag is skipped when making a scan anyway.
        /// </summary>
        protected bool ConsistentRead = false;

        private readonly IAmazonDynamoDB _client;
        private readonly string _tableNamePrefix;

        /// <summary>
        /// TableDefinitionWrapper instances for each entity type and HashKey value (if specified)
        /// </summary>
        protected internal readonly ConcurrentDictionary<Tuple<Type, object>, Lazy<TableDefinitionWrapper>> TableWrappers = new ConcurrentDictionary<Tuple<Type, object>, Lazy<TableDefinitionWrapper>>();

        /// <summary>
        /// A fake cache implementation, which does no caching
        /// </summary>
        private static readonly ITableCache FakeCacheImplementation = new FakeTableCache(); 


        private class CachedTableDefinitions : ConcurrentDictionary<string, Lazy<Table>>
        {
            /// <summary>
            /// Instead of storing a reference to DynamoDBClient we're storing it's HashCode
            /// </summary>
            private readonly int _dynamoDbClientHashCode;

            public CachedTableDefinitions(IAmazonDynamoDB client)
            {
                this._dynamoDbClientHashCode = client.GetHashCode();
            }

            public bool IsAssignedToThisClientInstance(IAmazonDynamoDB client)
            {
                return this._dynamoDbClientHashCode == client.GetHashCode();
            }
        }

        /// <summary>
        /// Used to lock while updating the <see cref="_cachedTableDefinitions" /> field
        /// </summary>
        private static readonly object CacheTableDefinitionsLock = new object();

        /// <summary>
        /// Table objects are cached here per DynamoDBClient instance.
        /// In order not to reload table metadata between DataContext instance creations.
        /// </summary>
        private static CachedTableDefinitions _cachedTableDefinitions;

        #endregion

        #region Private Methods

        /// <summary>
        /// Gets a full table name for a table entity type
        /// </summary>
        internal string GetTableNameForType(Type entityType)
        {
            string fullTableName;

            var entityAttributes = entityType.GetTypeInfo().GetCustomAttributes(typeof(DynamoDBTableAttribute), true);
            if (entityAttributes.Any())
            {
                fullTableName = this._tableNamePrefix + ((DynamoDBTableAttribute)entityAttributes.First()).TableName;
            }
            else
            {
                fullTableName = this._tableNamePrefix + entityType.Name;
            }

            return fullTableName;
        }

        internal void Log(string format, params object[] args)
        {
            var handler = this.OnLog;
            if (handler == null)
            {
                return;
            }
            handler("DataContext : " + string.Format(format, args));
        }

        /// <summary>
        /// Loads an AWS SDK's Table object, which is used for all get/query/scan/update operations 
        /// </summary>
        private Table GetTableDefinition(Type entityType)
        {
            if (this._client == null)
            {
                throw new InvalidOperationException("An instance of AmazonDynamoDbClient was not provided. Use either a ctor, that takes AmazonDynamoDbClient instance or GetTable() method, that takes Table object");
            }

            if
            (
                (_cachedTableDefinitions == null)
                ||
                (!_cachedTableDefinitions.IsAssignedToThisClientInstance(this._client))
            )
            {
                lock (CacheTableDefinitionsLock)
                {
                    if ((_cachedTableDefinitions == null)
                        ||
                        (!_cachedTableDefinitions.IsAssignedToThisClientInstance(this._client)))
                    {
                        _cachedTableDefinitions = new CachedTableDefinitions(this._client);
                    }
                }
            }

            var cachedTableDefinitions = _cachedTableDefinitions;

            string tableName = this.GetTableNameForType(entityType);
            return cachedTableDefinitions.GetOrAdd(tableName, name => new Lazy<Table>(() => Table.LoadTable(this._client, name), LazyThreadSafetyMode.ExecutionAndPublication)).Value;
        }

        #endregion
    }
}
