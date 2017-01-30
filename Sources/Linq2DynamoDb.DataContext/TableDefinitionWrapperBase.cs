using System;
using System.Collections;
using System.Diagnostics;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.ExpressionUtils;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Implements execution of get/query/scan requests against DynamoDb and cache
    /// </summary>
    public abstract partial class TableDefinitionWrapperBase
    {
        #region ctor

        internal TableDefinitionWrapperBase(Table tableDefinition, Type tableEntityType, object hashKeyValue, ITableCache cacheImplementation, bool consistentRead)
        {
            this.TableDefinition = tableDefinition;
            this.TableEntityType = tableEntityType;
            this._consistentRead = consistentRead;

            // if a predefined HashKey value was specified
            if (hashKeyValue != null)
            {
                this._hashKeyType = hashKeyValue.GetType();
                this.HashKeyValue = hashKeyValue.ToPrimitive(this._hashKeyType);
            }

            this.Cache = cacheImplementation;
            this.Cache.Initialize(tableDefinition.TableName, this.TableEntityType, this.HashKeyValue);
        }

        #endregion

        #region Public Properties

        internal string[] KeyNames { get { return this.EntityKeyGetter.KeyNames; } }

        /// <summary>
        /// Occurs when this object wants to log some debugging info
        /// </summary>
        internal event Action<string> OnLog;

        #endregion

        #region Public Methods

        /// <summary>
        /// Executes a get/query/scan request against the table
        /// </summary>
        internal object LoadEntities(TranslationResult translationResult, Type entityType)
        {
            // cancelling the previous index creation, if there was one
            this.CurrentIndexCreator = null;

            // skipping added and removed entities
            this.ClearModifications();

            // if a HashKey value was explicitly specified
            if (this.HashKeyValue != null)
            {
                // then adding a condition for it
                translationResult.Conditions.AddCondition
                    (
                        this.TableDefinition.HashKeys[0],
                        new SearchCondition(ScanOperator.Equal, this.HashKeyValue)
                    );
            }

#if DEBUG
            this._loadOperationStopwatch = new Stopwatch();
            this._loadOperationStopwatch.Start();
#endif

            return this.InternalLoadEntities(translationResult, entityType);
        }

        /// <summary>
        /// Called before a new get/query/scan request is sent to DynamoDb
        /// </summary>
        public virtual void ClearModifications()
        {
        }

        #endregion

        #region Private Properties

        /// <summary>
        /// The Table object, through which all DynamoDb operations are made
        /// </summary>
        internal Table TableDefinition;

        /// <summary>
        /// Should consistent read mode be used?
        /// </summary>
        private readonly bool _consistentRead;

        /// <summary>
        /// The type of the entity, that is stored in this table
        /// </summary>
        protected Type TableEntityType;

        /// <summary>
        /// HashKey field type
        /// </summary>
        private readonly Type _hashKeyType;

        /// <summary>
        /// Predefined hash key value
        /// </summary>
        protected readonly Primitive HashKeyValue;

        private IEntityKeyGetter _entityKeyGetter;
        /// <summary>
        /// Implements key extraction (caches everything it needs for that)
        /// </summary> 
        internal IEntityKeyGetter EntityKeyGetter
        {
            get
            {
                if (this._entityKeyGetter != null)
                {
                    return this._entityKeyGetter;
                }
                // performing lazy initialization
                this._entityKeyGetter = Linq2DynamoDb.DataContext.EntityKeyGetter.CreateInstance
                    (
                        this.TableDefinition, 
                        this.TableEntityType, 
                        this._hashKeyType, 
                        this.HashKeyValue
                    );
                return this._entityKeyGetter;
            }
        }

        /// <summary>
        /// Cache implementation
        /// </summary>
        internal ITableCache Cache;

        /// <summary>
        /// This object represents the process of filling an index in cache.
        /// The process should start after the query/scan request is successfully completed.
        /// And it should finish, when all the data for that request are read.
        /// </summary>
        protected IIndexCreator CurrentIndexCreator;

#if DEBUG
        /// <summary>
        /// A Stopwatch to measure the time taken to load entities
        /// </summary>
        private Stopwatch _loadOperationStopwatch;
#endif

        #endregion

        #region Private Methods

        private object InternalLoadEntities(TranslationResult translationResult, Type entityType)
        {
            // first trying to execute get
            object result;
            if (this.TryExecuteGet(translationResult, entityType, out result))
            {
                return result;
            }

            // now trying to load a query from cache
            if (this.TryLoadFromCache(translationResult, entityType, out result))
            {
                return result;
            }

            // finally requesting data from DynamoDb
            if (!this.TryExecuteBatchGet(translationResult, entityType, out result))
            {
                if (!this.TryExecuteQuery(translationResult, entityType, out result))
                {
                    result = this.ExecuteScan(translationResult, entityType);
                }
            }

            // Implementing Count().
            // Currently Count() causes a full fetch of all matched entities from DynamoDb.
            // Yes, there's an option to request just the count of them from DynamoDb. 
            // But it will cost you the same money as a full fetch!
            // So, it might be more efficient to request (and put to cache) all of them right now.
            // TODO: implement an option for this.
            if (translationResult.CountRequested)
            {
                return ((IEnumerable)result).Count(entityType);
            }

            // implementing OrderBy
            if (!string.IsNullOrEmpty(translationResult.OrderByColumn))
            {
                result = ((IEnumerable)result).OrderBy(entityType, translationResult.OrderByColumn, translationResult.OrderByDesc);
            }

            return result;
        }

        private bool TryLoadFromCache(TranslationResult translationResult, Type entityType, out object result)
        {
            result = null;

            var conditions4Cache = translationResult.Conditions;
            // if an explicit HashKey value is specified for table, then
            // we need to remove a condition for it from SearchConditions before passing them to cache 
            // implementation. That's because the entity's Type doesn't contain a HashKey property.
            if (this.HashKeyValue != null)
            {
                conditions4Cache = conditions4Cache.ExcludeField(this.TableDefinition.HashKeys[0]);
            }

            // implementing Count()
            if (translationResult.CountRequested)
            {
                int? countFromCache = this.Cache.GetCount(conditions4Cache);
                if (countFromCache.HasValue)
                {
                    result = countFromCache.Value;
                    return true;
                }
            }
            else
            {
                // getting the entities themselves from cache
                var docsFromCache = this.Cache.GetEntities(conditions4Cache, translationResult.AttributesToGet, translationResult.OrderByColumn, translationResult.OrderByDesc);
                if (docsFromCache != null)
                {
                    result = this.CreateDocArrayReader(docsFromCache, entityType, translationResult.ProjectionFunc);
                    return true;
                }
            }

            // If we failed to get from cache, then start filling an index in cache 
            // (this should be started before querying DynamoDb and only when table (full) entities are requested)
            this.CurrentIndexCreator =
                (translationResult.ProjectionFunc == null)
                ?
                this.Cache.StartCreatingIndex(conditions4Cache)
                :
                this.Cache.StartCreatingProjectionIndex(conditions4Cache, translationResult.AttributesToGet)
            ;

            return false;
        }

        private bool TryExecuteGet(TranslationResult translationResult, Type entityType, out object result)
        {
            result = null;

            var entityKey = translationResult.TryGetEntityKeyForTable(this.TableDefinition);
            if (entityKey == null)
            {
                return false;
            }

            Document resultDoc = null;

            // first trying to get entity from cache, but only if it's not a projection
            if (ReferenceEquals(this.TableEntityType, entityType))
            {
                resultDoc = this.Cache.GetSingleEntity(entityKey);
            }

            if (resultDoc != null)
            {
                this.Log("Get from cache: {0}", translationResult);
            }
            else
            {
                // if the entity is not found in cache - then getting it from DynamoDb
                resultDoc = this.TableDefinition.GetItemAsync
                    (
                        this.EntityKeyGetter.GetKeyDictionary(entityKey),
                        new GetItemOperationConfig
                        {
                            AttributesToGet = translationResult.AttributesToGet,
                            ConsistentRead = this._consistentRead
                        }
                    )
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();

                // putting the entity to cache as well
                this.Cache.PutSingleLoadedEntity(entityKey, resultDoc);

                this.Log("Get from DynamoDb: {0}", translationResult);
            }

            // creating an enumerator for a single value or an empty enumerator
            result = this.CreateSingleDocReader(resultDoc, entityType, translationResult.ProjectionFunc);

            return true;
        }

        private bool TryExecuteBatchGet(TranslationResult translationResult, Type entityType, out object resultingReader)
        {
            resultingReader = null;

            var batchGet = translationResult.GetBatchGetOperationForTable(this.TableDefinition);
            if (batchGet == null)
            {
                return false;
            }

            // if a projection is specified - then getting only the required list of fields
            if (translationResult.AttributesToGet != null)
            {
                batchGet.AttributesToGet = translationResult.AttributesToGet;
            }
            // using async method, because it's the only available in .Net Core version
            batchGet.ExecuteAsync().ConfigureAwait(false).GetAwaiter().GetResult();

            this.Log("DynamoDb batch get: {0}", translationResult);

            resultingReader = this.CreateDocArrayReader(batchGet.Results, entityType, translationResult.ProjectionFunc);
            return true;
        }

        private bool TryExecuteQuery(TranslationResult translationResult, Type entityType, out object resultingReader)
        {
            resultingReader = null;
            QueryFilter queryFilter;
            string indexName;

            // if we failed to compose a query with table's keys and local secondary indexes
            if (!translationResult.TryGetQueryFilterForTable(this.TableDefinition, out queryFilter, out indexName))
            {
                // then trying to find a suitable Global Secondary Index
                var matchingIndex = this.TableDefinition
                    .GlobalSecondaryIndexes.Values
                    .FirstOrDefault
                    (
                        index => translationResult.TryGetQueryFilterForGlobalSeconaryIndex(index, out queryFilter)
                    );

                if (matchingIndex == null)
                {
                    return false;
                }

                indexName = matchingIndex.IndexName;
            }

            var queryConfig = new QueryOperationConfig
            {
                Filter = queryFilter,
                CollectResults = false,
                ConsistentRead = this._consistentRead,
                IndexName = indexName
            };

            // if a projection is specified - then getting only the required list of fields
            if (translationResult.AttributesToGet != null)
            {
                queryConfig.Select = SelectValues.SpecificAttributes;
                queryConfig.AttributesToGet = translationResult.AttributesToGet;
            }

            var searchResult = this.TableDefinition.Query(queryConfig);

            if (string.IsNullOrEmpty(queryConfig.IndexName))
            {
                this.Log("DynamoDb query: {0}", translationResult);
            }
            else
            {
                this.Log("DynamoDb index query: {0}. Index name: {1}", translationResult, queryConfig.IndexName);
            }

            resultingReader = this.CreateReader(searchResult, entityType, translationResult.ProjectionFunc);
            return true;
        }

        private object ExecuteScan(TranslationResult translationResult, Type entityType)
        {
            var scanConfig = new ScanOperationConfig
            {
                Filter = translationResult.GetScanFilterForTable(this.TableDefinition),
                CollectResults = false
            };

            if (translationResult.AttributesToGet != null)
            {
                scanConfig.Select = SelectValues.SpecificAttributes;
                scanConfig.AttributesToGet = translationResult.AttributesToGet;
            }

            var searchResult = this.TableDefinition.Scan(scanConfig);

            this.Log("DynamoDb scan: {0}", translationResult);

            return this.CreateReader(searchResult, entityType, translationResult.ProjectionFunc);
        }

        protected virtual void InitReader(ISupervisableEnumerable reader)
        {
            // We need to detect the moment, when enumeration is finished. To put an index to cache
            reader.EnumerationFinished += () =>
            {
#if DEBUG
                if (this._loadOperationStopwatch != null)
                {
                    this._loadOperationStopwatch.Stop();
                    this.Log("Load operation took {0} ms", this._loadOperationStopwatch.ElapsedMilliseconds);
                    this._loadOperationStopwatch = null;
                }
#endif

                // storing the filled index to cache
                var curIndexCreator = this.CurrentIndexCreator;
                if (curIndexCreator != null)
                {
                    curIndexCreator.Dispose();
                }
                this.CurrentIndexCreator = null;
            };
        }

        protected void Log(string format, params object[] args)
        {
            var handler = this.OnLog;
            if (handler == null)
            {
                return;
            }

            var tableNameWithHashKey = this.TableDefinition.TableName;
            if (this.HashKeyValue != null)
            {
                tableNameWithHashKey += ":" + this.HashKeyValue.AsString();
            }

            string formattedString;
            try
            {
                formattedString = string.Format(format, args);
            }
            catch (FormatException exception)
            {
                handler(
                    "Failed to create diagnostics message due to error " + exception.Message + " @ " + exception.StackTrace + " @ input message " + format +
                        " @ input args " + string.Join(",", args ?? new object[0]));
                return;
            }

            handler(tableNameWithHashKey + " : " + formattedString);
        }

        #endregion
    }
}