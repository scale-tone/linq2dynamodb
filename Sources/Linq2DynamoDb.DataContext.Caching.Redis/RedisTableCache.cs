using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    /// <summary>
    /// Implements caching in Redis
    /// </summary>
    public partial class RedisTableCache : ITableCache
    {
        #region ctors

        public RedisTableCache(ConnectionMultiplexer redisConn, int dbIndex, TimeSpan cacheItemsTtl)
        {
            this._redisConn = redisConn;
            this._dbIndex = dbIndex;
            this._cacheItemsTtl = cacheItemsTtl;
        }

        public RedisTableCache(ConnectionMultiplexer redisConn, TimeSpan cacheItemsTtl) : this(redisConn, -1, cacheItemsTtl)
        {
        }

        public RedisTableCache(ConnectionMultiplexer redisConn, int dbIndex = -1) : this(redisConn, dbIndex, TimeSpan.MaxValue)
        {
        }

        #endregion

        #region ITableCache implementation

        public void Initialize(string tableName, Type tableEntityType, Primitive hashKeyValue)
        {
            if (this._tableEntityType != null)
            {
                throw new InvalidOperationException("An attempt to re-use an instance of RedisTableCache for another <table>:<hash key> pair was made. This is not allowed");
            }

            this._tableEntityType = tableEntityType;

            this.TableName = tableName;
            this.HashKeyValue = hashKeyValue == null ? string.Empty : hashKeyValue.AsString();

            this._redis = new RedisWrapper(this._redisConn, this._dbIndex, this.GetCacheKeyPrefix(), this._cacheItemsTtl, s => this.OnLog.FireSafely(s));
        }

        public Document GetSingleEntity(EntityKey entityKey)
        {
            try
            {
                var docWrapper = this._redis.GetWithRetries<CacheDocumentWrapper>(entityKey.ToRedisKey()).Single();
                var result = docWrapper.Document;
                this.OnHit.FireSafely();
                return result;
            }
            catch (Exception)
            {
                this.OnMiss.FireSafely();
                return null;
            }
        }

        public void PutSingleLoadedEntity(EntityKey entityKey, Document doc)
        {
            try
            {
                // again, never overwrite local changes
                this._redis.SetWithRetries(entityKey.ToRedisKey(), new CacheDocumentWrapper(doc), When.NotExists);
            }
            catch (RedisCacheException)
            {
                // this means the item existed in cache
            }
            catch (Exception ex)
            {
                this.Log("An entity with key {0} failed to be put to cache with exception: {1}", entityKey, ex);
            }
        }

        public IEnumerable<Document> GetEntities(SearchConditions searchConditions, IEnumerable<string> projectedFields, string orderByFieldName, bool orderByDesc)
        {
            string indexKey = this.GetIndexKey(searchConditions);
            string indexListKey = this.GetIndexListKeyInCache();
            try
            {
                Document[] result = null;

                // if a full index was found
                if (this._redis.HashFieldExistsWithRetries(indexListKey, indexKey))
                {
                    result = RedisIndex.LoadIndexEntities(this._redis, indexKey, indexListKey);
                }
                else if (projectedFields != null)
                {
                    // then trying to use a projection index
                    indexKey = this.GetIndexKey(searchConditions, projectedFields);

                    if (this._redis.HashFieldExistsWithRetries(indexListKey, indexKey))
                    {
                        result = RedisProjectionIndex.LoadProjectionIndexEntities(this._redis, indexKey, indexListKey);
                    }
                }

                // if we failed to load both full and projection index
                if (result == null)
                {
                    this.OnMiss.FireSafely();
                    return null;
                }

                this.OnHit.FireSafely();
                this.Log("Index ({0}) with {1} items successfully loaded from cache", indexKey, result.Length);

                if (string.IsNullOrEmpty(orderByFieldName))
                {
                    return result;
                }

                // creating a comparer to sort the results
                var comparer = PrimitiveComparer.GetComparer(this._tableEntityType, orderByFieldName);

                return orderByDesc
                    ?
                    result.OrderByDescending(doc => doc[orderByFieldName].AsPrimitive(), comparer)
                    :
                    result.OrderBy(doc => doc[orderByFieldName].AsPrimitive(), comparer)
                ;
            }
            catch (Exception ex)
            {
                this.Log("Failed to load index ({0}) from cache. {1}", indexKey, ex);
                this.OnMiss.FireSafely();
                return null;
            }
        }

        public int? GetCount(SearchConditions searchConditions)
        {
            string indexKey = this.GetIndexKey(searchConditions);
            try
            {
                if (!this._redis.HashFieldExistsWithRetries(this.GetIndexListKeyInCache(), indexKey))
                {
                    this.OnMiss.FireSafely();
                    return null;
                }

                long hashLengthWithVersionField = this._redis.GetHashLengthWithRetries(indexKey);
                if (hashLengthWithVersionField == 0)
                {
                    // zero means that index doesn't exist in the cache, because each index has at least a Version field added
                    throw new RedisCacheException("Index wasn't found in cache");
                }

                long result = hashLengthWithVersionField - 1;

                this.OnHit.FireSafely();
                this.Log("Contents of index ({0}) successfully loaded from cache and number of items returned is {1}", searchConditions.Key, result);
                return (int)result;
            }
            catch (Exception ex)
            {
                this.Log("Failed to get index size ({0}). {1}", searchConditions.Key, ex);
                this.OnMiss.FireSafely();
                return null;
            }
        }

        public void RemoveEntities(IEnumerable<EntityKey> entities)
        {
            try
            {
                this._redis.RemoveWithRetries(entities.Select(k => k.ToRedisKey()).ToArray());
            }
            catch (Exception ex)
            {
                this.Log("Failed to remove entities. {0}", ex);
            }
        }

        public IIndexCreator StartCreatingIndex(SearchConditions searchConditions)
        {
            try
            {
                return new RedisIndex(this, this.GetIndexKey(searchConditions), searchConditions);
            }
            catch (Exception ex)
            {
                this.Log("Failed to start creating index ({0}). {1}", searchConditions.Key, ex);
                return null;
            }
        }

        public IIndexCreator StartCreatingProjectionIndex(SearchConditions searchConditions, IList<string> projectedFields)
        {
            try
            {
                return new RedisProjectionIndex(this, this.GetIndexKey(searchConditions, projectedFields), searchConditions);
            }
            catch (Exception ex)
            {
                this.Log("Failed to start creating projection index ({0}). {1}", searchConditions.Key, ex);
                return null;
            }
        }

        public void UpdateCacheAndIndexes(IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            var affectedIndexKeys = new List<RedisKey>();
            try
            {
                var transaction = this._redis.BeginTransaction();

                // remembering the list of indexes, that will be affected by this transaction
                transaction.OnKeyAffected += affectedIndexKeys.Add;

                // first updating the entities themselves
                foreach (var entity in addedEntities.Union(modifiedEntities))
                {
                    transaction.Set(entity.Key.ToRedisKey(), new CacheDocumentWrapper(entity.Value).ToRedisValue());
                }
                foreach (var entityKey in removedEntities)
                {
                    transaction.Remove(entityKey.ToRedisKey());
                }

                var affectedHashKeys = new List<string> {this.HashKeyValue};

                // To support scenarios, when a context contains both a full table and a table filtered by a HashKey,
                // by updating HashKey-filtered indexes we also should update indexes of the full table.
                // And vice versa.
                if (string.IsNullOrEmpty(this.HashKeyValue))
                {
                    // Trying to update lists of indexes with predefined HashKey values
                    affectedHashKeys.AddRange
                    (
                        addedEntities
                        .Where(kv => kv.Key.RangeKey != null)
                        .Select(kv => kv.Key.HashKey.AsString())
                        .Union
                        (
                            modifiedEntities
                            .Where(kv => kv.Key.RangeKey != null)
                            .Select(kv => kv.Key.HashKey.AsString())
                        )
                        .Union
                        (
                            removedEntities
                            .Where(kv => kv.RangeKey != null)
                            .Select(kv => kv.HashKey.AsString())
                        )
                        .Distinct()
                    );
                }
                else
                {
                    // updating the full table indexes as well
                    affectedHashKeys.Add(string.Empty);
                }

                foreach (var hashKey in affectedHashKeys)
                {
                    this.UpdateIndexes(transaction, hashKey, addedEntities, modifiedEntities, removedEntities);
                }

                transaction.Execute();
            }
            catch (Exception ex)
            {
                this.Log("Failed to update indexes. {0}", ex);

                // dropping all affected indexes
                try
                {
                    this._redis.RemoveWithRetries(affectedIndexKeys.ToArray());
                }
                catch (Exception ex2)
                {
                    this.Log("Failed to drop the indexes affected by failed update transaction. {0}", ex2);
                }
            }
        }

        public IDisposable AcquireTableLock(string lockKey, TimeSpan lockTimeout)
        {
            return new TableLock(this, lockKey, lockTimeout);
        }

        public event Action OnHit;
        public event Action OnMiss;
        public event Action<string> OnLog;

        #endregion

        #region Private Members

        private const string ProjectionIndexKeyPrefix = "[projection]";
        private static readonly Random Rnd = new Random(DateTime.Now.Millisecond);

        protected string TableName;
        protected string HashKeyValue;

        private readonly ConnectionMultiplexer _redisConn;
        private readonly int _dbIndex;
        private readonly TimeSpan _cacheItemsTtl;
        private RedisWrapper _redis;
        private Type _tableEntityType;

        /// <summary>
        /// Returns a key prefix to be prepended to all keys in Redis.
        /// The default implementation respects the sharding algorithm, that is used in Redis clustering mode.
        /// For RedisTableCache to work fine, all keys must fall to the same Redis shard (otherwise transactions will fail).
        /// </summary>
        protected virtual string GetCacheKeyPrefix()
        {
            return "{" + this.TableName + "}";
        }

        /// <summary>
        /// Gets a cache key for the list of indexes
        /// </summary>
        private string GetIndexListKeyInCache(string hashKeyValue = null)
        {
            if (hashKeyValue == null)
            {
                hashKeyValue = this.HashKeyValue;
            }
            return hashKeyValue + ":indexes";
        }

        /// <summary>
        /// Gets a cache key for a table-wide lock
        /// </summary>
        private string GetLockKeyInCache(string lockKey)
        {
            return this.TableName + this.HashKeyValue + lockKey;
        }

        /// <summary>
        /// Composes a key for an index
        /// </summary>
        private string GetIndexKey(SearchConditions searchConditions, IEnumerable<string> projectedFields = null)
        {
            if (projectedFields == null)
            {
                return this.HashKeyValue + ":" + searchConditions.Key;
            }
            else
            {
                return ProjectionIndexKeyPrefix + this.HashKeyValue + ":" + projectedFields.Aggregate((i, s) => i + "," + s) + ":" + searchConditions.Key;
            }
        }

        /// <summary>
        /// Checks if the provided key is a projection index's key
        /// </summary>
        private bool IsProjectionIndex(string indexKey)
        {
            return indexKey.StartsWith(ProjectionIndexKeyPrefix);
        }

        /// <summary>
        /// Adds matching entities and removes removed entities from indexes
        /// </summary>
        private void UpdateIndexes(RedisTransactionWrapper transaction, string hashKeyValue, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            foreach (var indexEntry in this._redis.GetHashFieldsWithRetries(this.GetIndexListKeyInCache(hashKeyValue)))
            {
                string indexKey = indexEntry.Name;
                var filter = indexEntry.Value.ToObject<SearchConditions>();

                if (this.IsProjectionIndex(indexKey))
                {
                    this.UpdateProjectionIndex(transaction, hashKeyValue, indexKey, filter, addedEntities, modifiedEntities, removedEntities);
                }
                else
                {
                    this.UpdateIndex(transaction, indexKey, filter, addedEntities, modifiedEntities, removedEntities);
                }
            }
        }

        /// <summary>
        /// Adds matching entities and removes removed entities from index
        /// </summary>
        private void UpdateIndex(RedisTransactionWrapper transaction, string indexKey, SearchConditions filter, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            bool indexChanged = false;

            // adding added entities, if they match the index conditions
            foreach
            (
                var entityPair in addedEntities.Union(modifiedEntities).Where
                (
                    entityPair => filter.MatchesSearchConditions(entityPair.Value, this._tableEntityType)
                )
            )
            {
                transaction.HashSet(indexKey, entityPair.Key.ToRedisValue(), string.Empty);
                indexChanged = true;
            }

            // removing modified entities, if they do not match the index conditions any more
            foreach
            (
                var entityPair in modifiedEntities.Where
                (
                    entityPair => !filter.MatchesSearchConditions(entityPair.Value, this._tableEntityType)
                )
            )
            {
                transaction.HashRemove(indexKey, entityPair.Key.ToRedisValue());
                indexChanged = true;
            }

            // removing removed entities
            foreach (var entityKey in removedEntities)
            {
                transaction.HashRemove(indexKey, entityKey.ToRedisValue());
                indexChanged = true;
            }

            if (indexChanged)
            {
                // The index should exist in the cache at the moment transaction is being executed.
                // Otherwise this update operation will cause the index to occasionally "resurrect" after being expired.
                transaction.AddHashFieldExistsCondition(indexKey, IndexVersionField.Name);

                // also incrementing the version field
                transaction.HashIncrement(indexKey, IndexVersionField.Name);
            }
        }

        /// <summary>
        /// Checks if a projection index should be dropped because of some added/modified/removed entities
        /// </summary>
        private void UpdateProjectionIndex(RedisTransactionWrapper transaction, string hashKeyValue, string indexKey, SearchConditions filter, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            if
            (
                (modifiedEntities.Count > 0)
                ||
                (removedEntities.Count > 0)
                ||
                addedEntities.Values.Any // or some entities were added, that satisfy the index condition
                (
                    en => filter.MatchesSearchConditions(en, this._tableEntityType)
                )
            )
            {
                // then the only option for us is to drop the index - as we don't know, if these entities conform to index's conditions or not
                this.Log("Projection index ({0}) removed because of some modified entities", indexKey);
                transaction.HashRemove(this.GetIndexListKeyInCache(hashKeyValue), indexKey);
                transaction.Remove(indexKey);
            }
        }

        private void Log(string format, params object[] args)
        {
            var handler = this.OnLog;
            if (handler == null)
            {
                return;
            }
            string tableName = this.TableName;
            if (!string.IsNullOrEmpty(this.HashKeyValue))
            {
                tableName += ":" + this.HashKeyValue;
            }

            handler("RedisTableCache(" + tableName + ") : " + string.Format(format, args));
        }

        #endregion
    }
}
