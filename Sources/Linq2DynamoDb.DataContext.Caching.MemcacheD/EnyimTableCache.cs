using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Enyim.Caching;
using Enyim.Caching.Memcached;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext.Caching.MemcacheD
{
    /// <summary>
    /// Implements caching in MemcacheD/ElastiCache via Enyim caching client
    /// </summary>
    public partial class EnyimTableCache : ITableCache
    {
        public EnyimTableCache(MemcachedClient cacheClient, TimeSpan cacheItemsTtl)
        {
            this._cacheClient = cacheClient;
            this._ttl = cacheItemsTtl;
        }

        #region ITableCache implementation

        public void Initialize(string tableName, Type tableEntityType, Primitive hashKeyValue)
        {
            if (this._tableEntityType != null)
            {
                throw new InvalidOperationException("An attempt to re-use an instance of EnyimTableCache for another <table>:<hash key> pair was made. This is not allowed");
            }

            this._tableEntityType = tableEntityType;

            this._tableName = tableName;
            this._hashKeyValue = hashKeyValue == null ? string.Empty : hashKeyValue.AsString();

            if (this.GetIndexListKeyInCache(this._hashKeyValue).Length > MaxKeyLength)
            {
                throw new ArgumentException("The hash key value is too long for MemcacheD. Cannot use cache with that value.");
            }
        }

        public Document GetSingleEntity(EntityKey entityKey)
        {
            string entityKeyInCache = this.GetEntityKeyInCache(entityKey);
            if (entityKeyInCache.Length > MaxKeyLength)
            {
                return null;
            }

            var wrapper = this._cacheClient.Get<CacheDocumentWrapper>(entityKeyInCache);
            if (wrapper == null)
            {
                this.OnMiss.FireSafely();
                return null;
            }

            this.OnHit.FireSafely();
            return wrapper.Document;
        }

        public IEnumerable<Document> GetEntities(SearchConditions searchConditions, IEnumerable<string> projectedFields, string orderByFieldName, bool orderByDesc)
        {
            // first trying to find a full index
            string indexKey = searchConditions.Key;
            var index = this.TryLoadHealthyIndex(indexKey);

            Document[] result = null;

            // if no full index exist
            if (index == null)
            {
                if (projectedFields != null)
                {
                    // then there still might be a projection index
                    indexKey = this.GetProjectionIndexKey(searchConditions, projectedFields);
                    result = this.TryLoadProjectionIndexEntities(indexKey);
                }
            }
            else
            {
                result = this.TryLoadIndexEntities(index, indexKey);
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

        public int? GetCount(SearchConditions searchConditions)
        {
            var index = this.TryLoadHealthyIndex(searchConditions.Key);

            if (index == null)
            {
                this.OnMiss.FireSafely();
                return null;
            }

            this.OnHit.FireSafely();
            this.Log("Contents of index ({0}) successfully loaded from cache and number of items returned is {1}", searchConditions.Key, index.Count);

            return index.Count;
        }

        public void PutSingleLoadedEntity(EntityKey entityKey, Document doc)
        {
            string entityKeyInCache = this.GetEntityKeyInCache(entityKey);
            if (entityKeyInCache.Length > MaxKeyLength)
            {
                return;
            }

            // Putting the entity to cache, but only if it doesn't exist there.
            // That's because when loading from DynamoDb whe should never overwrite local updates.
            this._cacheClient.Store(StoreMode.Add, entityKeyInCache, new CacheDocumentWrapper(doc), this._ttl);
        }

        public void RemoveEntities(IEnumerable<EntityKey> entities)
        {
            var entityKeysInCache = entities
                .Select(this.GetEntityKeyInCache)
                // extracting too long keys
                .Where(ek => ek.Length <= MaxKeyLength);

            Parallel.ForEach(entityKeysInCache, key => this._cacheClient.Remove(key));
        }

        /// <summary>
        /// Applies modifications to cached entities and indexes
        /// </summary>
        public void UpdateCacheAndIndexes(IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            var allEntities = addedEntities
                .Union(modifiedEntities)
                .Union(removedEntities.ToDictionary(k => k, k => (Document)null))
                // converting keys
                .Select(kv => new KeyValuePair<string, Document>(this.GetEntityKeyInCache(kv.Key), kv.Value))
                // extracting entities with too long keys
                .Where(kv => kv.Key.Length <= MaxKeyLength)
                .ToArray();

            // modifying/removing all entities in parallel
            var loopResult = Parallel.ForEach(allEntities, (entityPair, loopState) =>
            {
                bool result = true;
                if (entityPair.Value == null)
                {
                    var removeResult = this._cacheClient.ExecuteRemove(entityPair.Key);
                    if 
                    (
                        (removeResult.InnerResult != null)
                        &&
                        (removeResult.InnerResult.Exception != null)
                    )
                    {
                        // this means, that item failed to be removed because of a communication error, not because the entity doesn't exist already
                        result = false;
                    }
                }
                else
                {
                    result = this._cacheClient.Store(StoreMode.Set, entityPair.Key, new CacheDocumentWrapper(entityPair.Value), this._ttl);
                }

                if (!result)
                {
                    loopState.Stop();
                }
            });

            // All operations should succeed, otherwise removing all affected entities from cache.
            // That's because in some cases partially succeded updates might become a problem.
            if (!loopResult.IsCompleted)
            {
                this.Log("Failed to put updates for table {0} to cache", this._tableName);

                Parallel.ForEach(allEntities, entityPair => this._cacheClient.Remove(entityPair.Key));

                // Still need to proceed updating indexes. They shouldn't become stale because of a communication error during the previous step.
            }

            // now updating indexes
            this.UpdateIndexes(this._hashKeyValue, addedEntities, modifiedEntities, removedEntities);

            // To support scenarios, when a context contains both a full table and a table filtered by a HashKey,
            // by updating HashKey-filtered indexes we also should update indexes of the full table.
            // And vice versa.

            if (string.IsNullOrEmpty(this._hashKeyValue))
            {
                // Trying to update lists of indexes with predefined HashKey values
                var affectedHashKeys = addedEntities
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
                    .Distinct();

                foreach (var hashKeyValue in affectedHashKeys)
                {
                    this.UpdateIndexes(hashKeyValue, addedEntities, modifiedEntities, removedEntities);
                }
            }
            else
            {
                // updating the full table indexes as well
                this.UpdateIndexes(string.Empty, addedEntities, modifiedEntities, removedEntities);
            }
        }

        public IIndexCreator StartCreatingIndex(SearchConditions searchConditions)
        {
            string indexKeyInCache = this.GetIndexKeyInCache(searchConditions.Key, this._hashKeyValue);
            if (indexKeyInCache.Length > MaxKeyLength)
            {
                this.Log("Index key ({0}) is too long", searchConditions.Key);
                return null;
            }

            var creator = new EnyimIndexCreator(this, indexKeyInCache, searchConditions);

            if (!creator.StartCreatingIndex())
            {
                this.Log("Failed to start creating index ({0})", searchConditions.Key);
                return null;
            }

            return creator;
        }

        public IIndexCreator StartCreatingProjectionIndex(SearchConditions searchConditions, IList<string> projectedFields)
        {
            string indexKey = this.GetProjectionIndexKey(searchConditions, projectedFields);
            string indexKeyInCache = this.GetIndexKeyInCache(indexKey, this._hashKeyValue);
            if (indexKeyInCache.Length > MaxKeyLength)
            {
                this.Log("Index key ({0}) is too long", searchConditions.Key);
                return null;
            }

            var creator = new EnyimProjectionIndexCreator(this, indexKey, indexKeyInCache, searchConditions);

            if (!creator.StartCreatingIndex())
            {
                this.Log("Failed to start creating projection index ({0})", indexKey);
                return null;
            }

            return creator;
        }

        /// <summary>
        /// Acquires a table-wide named lock and returns a disposable object, that represents it
        /// </summary>
        public IDisposable AcquireTableLock(string lockKey, TimeSpan lockTimeout)
        {
            return new TableLock(this, lockKey, lockTimeout);
        }

        public event Action OnHit;
        public event Action OnMiss;
        public event Action<string> OnLog;

        #endregion

        #region Public Methods

        /// <summary>
        /// Acquires a named lock around the table by storing a random value in cache
        /// </summary>
        internal void LockTable(string lockKey, TimeSpan lockTimeout)
        {
            if (this._lockIds.ContainsKey(lockKey))
            {
                throw new NotSupportedException("Recursive locks are not supported. Or maybe you're trying to use EnyimTableCache object from multiple threads?");
            }

            string cacheLockKey = this.GetLockKeyInCache(this._hashKeyValue, lockKey);
            int cacheLockId = Rnd.Next();

            var timeStart = DateTime.Now;
            while (true)
            {
                if (DateTime.Now - timeStart > lockTimeout)
                {
                    break;
                }

                // Trying to create a new value in cache
                if (this._cacheClient.Store(StoreMode.Add, cacheLockKey, cacheLockId))
                {
                    this._lockIds[lockKey] = cacheLockId;
                    return;
                }

                Thread.Sleep(10);
            }

            // If we failed to acquire a lock within CacheLockTimeoutInSeconds 
            // (this means, that another process crached), then we should forcibly acquire it

            this.Log("Forcibly acquiring the table lock object {0} after {1} ms of waiting", lockKey, lockTimeout.TotalMilliseconds);

            this._cacheClient.Store(StoreMode.Set, cacheLockKey, cacheLockId);
            this._lockIds[lockKey] = cacheLockId;
        }

        /// <summary>
        /// Releases a named lock around the table
        /// </summary>
        internal void UnlockTable(string lockKey)
        {
            int lockId;
            if (!this._lockIds.TryRemove(lockKey, out lockId))
            {
                throw new InvalidOperationException
                (
                    string.Format("The table lock {0} wasn't acquired, so it cannot be released. Check your code!", lockKey)
                );
            }

            string cacheLockKey = this.GetLockKeyInCache(this._hashKeyValue, lockKey);
            var cacheLockId = this._cacheClient.Get(cacheLockKey);
            if (cacheLockId == null)
            {
                // The cache miss might happen here, if a cache server crashed.
                // In this case we just silently return.
                this.Log("The table lock object {0} is missing in cache, but we don't care about that too much (probably, the cache node was restarted)", lockKey);
                return;
            }

            if (((int)cacheLockId) != lockId)
            {
                // This means, that another process has forcibly replaced our lock.
                throw new InvalidOperationException
                (
                    string.Format("The table lock {0} was forcibly acquired by another process", lockKey)
                );
            }

            this._cacheClient.Remove(cacheLockKey);
        }

        #endregion

        #region Private Properties

        private readonly MemcachedClient _cacheClient;

        /// <summary>
        /// The time-to-live for all entities and indexes stored in cache
        /// </summary>
        private readonly TimeSpan _ttl;

        /// <summary>
        /// Entities and indexes with key longer than this limit are not saved to cache
        /// </summary>
        private const int MaxKeyLength = 250;

        /// <summary>
        /// Limit to the number of indexes
        /// TODO: estimate this number more precisely
        /// </summary>
        private const int MaxNumberOfIndexes = 100;

        /// <summary>
        /// The number of times to try an optimistic update operation
        /// </summary>
        private const int MaxUpdateAttempts = 10;

        private Type _tableEntityType;

        private string _tableName;
        private string _hashKeyValue;

        /// <summary>
        /// Here all lock keys and their IDs are stored, for debugging purposes
        /// </summary>
        private readonly ConcurrentDictionary<string, int> _lockIds = new ConcurrentDictionary<string, int>();

        private static readonly Random Rnd = new Random(DateTime.Now.Millisecond);

        #endregion

        #region Private Methods

        private const string ProjectionIndexKeyPrefix = "[proj]";

        /// <summary>
        /// Composes a key for a projection index
        /// </summary>
        private string GetProjectionIndexKey(SearchConditions searchConditions, IEnumerable<string> projectedFields)
        {
            return ProjectionIndexKeyPrefix + projectedFields.Aggregate((i, s) => i + "," + s) + "; " + searchConditions.Key;
        }

        /// <summary>
        /// Checks if the provided key is a projection index's key
        /// </summary>
        private bool IsProjectionIndex(string indexKey)
        {
            return indexKey.StartsWith(ProjectionIndexKeyPrefix);
        }

        /// <summary>
        /// Gets a cache key for an entity
        /// </summary>
        private string GetEntityKeyInCache(EntityKey entityKey)
        {
            // entities will always be identified in cache by their key prefixed by table name.
            // entityKey might contain spaces, which are not allowed for MemcacheD keys. This is why ToBase64() is used.
            return (this._tableName + ":" + entityKey).ToBase64();
        }

        /// <summary>
        /// Gets a cache key for an index
        /// </summary>
        private string GetIndexKeyInCache(string indexKey, string hashKeyValue)
        {
            // indexKey might contain spaces, which are not allowed for MemcacheD keys
            return (this._tableName + hashKeyValue + indexKey).ToBase64();
        }

        /// <summary>
        /// Gets a cache key for the list of all indexes
        /// </summary>
        private string GetIndexListKeyInCache(string hashKeyValue)
        {
            return (this._tableName + hashKeyValue + ":indexes").ToBase64();
        }

        /// <summary>
        /// Gets a cache key for a table-wide lock
        /// </summary>
        private string GetLockKeyInCache(string lockKey, string hashKeyValue)
        {
            return (this._tableName + hashKeyValue + lockKey).ToBase64();
        }

        /// <summary>
        /// Adds matching entities and removes removed entities from indexes
        /// </summary>
        private void UpdateIndexes(string hashKeyValue, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            string indexListKeyInCache = this.GetIndexListKeyInCache(hashKeyValue);

            var indexes = this._cacheClient.Get<TableIndexList>(indexListKeyInCache);
            if (indexes == null)
            {
                this._cacheClient.Remove(indexListKeyInCache);
                return;
            }

            // storing indexes, that fail to be updated
            var indexesToBeRemoved = new ConcurrentDictionary<string, string>();

            Parallel.ForEach(indexes, indexKey =>
            {
                string indexKeyInCache = this.GetIndexKeyInCache(indexKey, hashKeyValue);

                bool indexUpdateSucceeded = 
                    this.IsProjectionIndex(indexKey) 
                    ? 
                    this.UpdateProjectionIndex(indexKey, indexKeyInCache, addedEntities, modifiedEntities, removedEntities) 
                    : 
                    this.UpdateIndex(indexKey, indexKeyInCache, addedEntities, modifiedEntities, removedEntities);

                if (!indexUpdateSucceeded)
                {
                    indexesToBeRemoved[indexKey] = indexKeyInCache;
                }
            });

            // removing bad indexes
            if (indexesToBeRemoved.Count > 0)
            {
                this.RemoveIndexesFromList(indexesToBeRemoved, indexListKeyInCache);
            }
        }

        /// <summary>
        /// Adds matching entities and removes removed entities from an index
        /// </summary>
        private bool UpdateIndex(string indexKey, string indexKeyInCache, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexGetResult = this._cacheClient.GetWithCas<TableIndex>(indexKeyInCache);
                var index = indexGetResult.Result;
                if (index == null)
                {
                    return false;
                }

                bool indexChanged = false;

                try
                {
                    // adding added entities, if they match the index conditions
                    foreach 
                    (
                        var entityPair in addedEntities.Union(modifiedEntities).Where
                        (
                            entityPair => index.MatchesSearchConditions(entityPair.Value, this._tableEntityType)
                        )
                    )
                    {
                        index.Index.Add(entityPair.Key);
                        indexChanged = true;
                    }

                    // removing modified entities, if they do not match the index conditions any more
                    foreach
                    (
                        var entityPair in modifiedEntities.Where
                        (
                            entityPair => !index.MatchesSearchConditions(entityPair.Value, this._tableEntityType)
                        )
                    )
                    {
                        index.Index.Remove(entityPair.Key);
                        indexChanged = true;
                    }
                }
                catch (Exception ex)
                {
                    // some stupid exceptions might occur within TableIndex.MatchesSearchConditions()
                    this.Log("Failed to update index ({0}) because of the following exception: {1}", indexKey, ex);
                    return false;
                }

                // removing removed entities
                foreach (var entityKey in removedEntities.Where(entityKey => index.Index.Contains(entityKey)))
                {
                    index.Index.Remove(entityKey);
                    indexChanged = true;
                }

                if (!indexChanged)
                {
                    return true;
                }

                if (this._cacheClient.Cas(StoreMode.Set, indexKeyInCache, index, this._ttl, indexGetResult.Cas).Result)
                {
                    this.Log("Index ({0}) updated. {1} entities added, {2} entities removed", indexKey, addedEntities.Count, removedEntities.Count);
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Checks if a projection index should be dropped because of some added/modified/removed entities
        /// </summary>
        private bool UpdateProjectionIndex(string indexKey, string indexKeyInCache, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            // if some entities were modified or removed
            if 
            (
                (modifiedEntities.Count > 0)
                ||
                (removedEntities.Count > 0)
            )
            {
                // then the only option for us is to drop the index - as we don't know, if these entities conform to index's conditions or not
                this.Log("Projection index ({0}) removed because some entities were removed", indexKey);
                return false;
            }

            var indexGetResult = this._cacheClient.GetWithCas<TableProjectionIndex>(indexKeyInCache);
            var index = indexGetResult.Result;
            if (index == null)
            {
                return false;
            }

            try
            {
                // if any added or modified entities conform to index's conditions
                if
                (
                    addedEntities.Values.Any
                    (
                        en => index.MatchesSearchConditions(en, this._tableEntityType)
                    )
                )
                {
                    this.Log("Projection index ({0}) removed because of some added entities", indexKey);
                    return false;
                }
            }
            catch (Exception ex)
            {
                // some stupid exceptions might occur within TableIndex.MatchesSearchConditions()
                this.Log("Failed to check projection index ({0}) because of the following exception: {1}", indexKey, ex);
                return false;
            }

            return true;
        }

        private bool PutIndexToList(string indexKey)
        {
            string indexListKeyInCache = this.GetIndexListKeyInCache(this._hashKeyValue);

            // trying multiple times
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexesGetResult = this._cacheClient.GetWithCas<TableIndexList>(indexListKeyInCache);

                if (indexesGetResult.StatusCode != 0)
                {
                    this.Log("Failed to put index ({0}) to list because of a communication error", indexKey);
                    return false;
                }

                TableIndexList indexes;
                if (indexesGetResult.Result == null)
                {
                    indexes = new TableIndexList(MaxNumberOfIndexes);

                    // saving the newly created value and re-reading CAS token
                    this._cacheClient.Store(StoreMode.Add, indexListKeyInCache, indexes);
                    continue;
                }
                indexes = indexesGetResult.Result;
                
                if (indexes.Contains(indexKey))
                {
                    return true;
                }

                string poppedIndexKey = indexes.Push(indexKey);
                if (poppedIndexKey != null)
                {
                    // if an older index was removed from the list during push operation - then removing it from cache
                    this._cacheClient.Remove(this.GetIndexKeyInCache(poppedIndexKey, this._hashKeyValue));
                    this.Log("An old index ({0}) was removed from cache because the number of supported indexes ({1}) is exceeded", poppedIndexKey, MaxNumberOfIndexes);
                }

                if (this._cacheClient.Cas(StoreMode.Set, indexListKeyInCache, indexes, this._ttl, indexesGetResult.Cas).Result)
                {
                    return true;
                }
            }

            this.Log("Failed to put index ({0}) to list after {1} attempts", indexKey, MaxUpdateAttempts);
            return false;
        }

        private void RemoveIndexFromList(string indexKey, string indexKeyInCache)
        {
            // always removing the index itself
            this._cacheClient.Remove(indexKeyInCache);

            string indexListKeyInCache = this.GetIndexListKeyInCache(this._hashKeyValue);

            // trying multiple times
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexesGetResult = this._cacheClient.GetWithCas<TableIndexList>(indexListKeyInCache);

                if (indexesGetResult.StatusCode != 0)
                {
                    this.Log("Failed to remove index ({0}) from list because of a communication error", indexKey);
                    // just in case, trying to remove the whole list
                    this._cacheClient.Remove(indexListKeyInCache);
                    return;
                }

                if (indexesGetResult.Result == null)
                {
                    return;
                }

                var indexes = indexesGetResult.Result;

                if (!indexes.Contains(indexKey))
                {
                    return;
                }

                indexes.Remove(indexKey);

                var casResult = this._cacheClient.Cas(StoreMode.Set, indexListKeyInCache, indexes, this._ttl, indexesGetResult.Cas);
                if (casResult.Result)
                {
                    return;
                }
            }

            this.Log("Failed to remove index ({0}) from list after {1} attempts. Removing the whole list.", indexKey, MaxUpdateAttempts);
            this._cacheClient.Remove(indexListKeyInCache);
        }

        private void RemoveIndexesFromList(IDictionary<string, string> indexKeys, string indexListKeyInCache)
        {
            // always removing indexes themselves
            Parallel.ForEach(indexKeys.Values, indexKeyInCache => this._cacheClient.Remove(indexKeyInCache));

            // trying multiple times
            //TODO: implement a helper for multiple attempts
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexesGetResult = this._cacheClient.GetWithCas<TableIndexList>(indexListKeyInCache);

                if (indexesGetResult.StatusCode != 0)
                {
                    this.Log("Failed to remove {0} indexes from list because of a communication error", indexKeys.Count);
                    // just in case, trying to remove the whole list
                    this._cacheClient.Remove(indexListKeyInCache);
                    return;
                }

                if (indexesGetResult.Result == null)
                {
                    return;
                }

                var indexes = indexesGetResult.Result;
                bool indexesChanged = false;

                foreach (var indexKey in indexKeys.Keys)
                {
                    if (indexes.Contains(indexKey))
                    {
                        indexes.Remove(indexKey);
                        indexesChanged = true;
                    }
                }

                if (!indexesChanged)
                {
                    return;
                }

                var casResult = this._cacheClient.Cas(StoreMode.Set, indexListKeyInCache, indexes, this._ttl, indexesGetResult.Cas);
                if (casResult.Result)
                {
                    return;
                }
            }

            this.Log("Failed to remove {0} indexes from list after {1} attempts. Removing the whole list.", indexKeys.Count, MaxUpdateAttempts);
            this._cacheClient.Remove(indexListKeyInCache);
        }

        /// <summary>
        /// Loads the index by it's key, ensuring everything
        /// </summary>
        private HashSet<EntityKey> TryLoadHealthyIndex(string indexKey)
        {
            string indexKeyInCache = this.GetIndexKeyInCache(indexKey, this._hashKeyValue);
            if (indexKeyInCache.Length > MaxKeyLength)
            {
                return null;
            }

            // first trying to get the index from cache
            var index = this._cacheClient.Get<TableIndex>(indexKeyInCache);
            if (index == null)
            {
                return null;
            }

            // Checking, that index is mentioned in the list of indexes.
            // Only indexes from list are updated with local updates.
            if (!this.DoesIndexExistInTheListOfIndexes(indexKey))
            {
                this._cacheClient.Remove(indexKeyInCache);
                return null;
            }

            // If index is currently being filled with data from DynamoDb, then we can't use it yet
            if (index.IsBeingRebuilt)
            {
                return null;
            }

            return index.Index;
        }

        /// <summary>
        /// Loads a projection index by it's key
        /// </summary>
        private Document[] TryLoadProjectionIndexEntities(string indexKey)
        {
            string indexKeyInCache = this.GetIndexKeyInCache(indexKey, this._hashKeyValue);
            if (indexKeyInCache.Length > MaxKeyLength)
            {
                return null;
            }

            // first trying to get the index from cache
            var index = this._cacheClient.Get<TableProjectionIndex>(indexKeyInCache);
            if (index == null)
            {
                return null;
            }

            // Checking, that index is mentioned in the list of indexes.
            // Only indexes from list are updated with local updates.
            if (!this.DoesIndexExistInTheListOfIndexes(indexKey))
            {
                this._cacheClient.Remove(indexKeyInCache);
                return null;
            }

            // If index is currently being filled with data from DynamoDb, then we can't use it yet
            if (index.IsBeingRebuilt)
            {
                return null;
            }

            return index.Entities;
        }

        private Document[] TryLoadIndexEntities(HashSet<EntityKey> entityKeys, string indexKey)
        {
            var result = new Document[entityKeys.Count];

            // now we have to succeed loading all entities from cache
            var loopResult = Parallel.ForEach(entityKeys, (entityKey, loopState, i) =>
            {
                var wrapper = this._cacheClient.Get<CacheDocumentWrapper>(this.GetEntityKeyInCache(entityKey));
                if (wrapper == null)
                {
                    loopState.Stop();
                    return;
                }

                result[i] = wrapper.Document;
            });

            if (loopResult.IsCompleted)
            {
                return result;
            }

            this.Log("Failed to get contents of index ({0}) from cache", indexKey);

            // the index is not usable any more, we'd better delete it
            this.RemoveIndexFromList
            (
                indexKey,
                this.GetIndexKeyInCache(indexKey, this._hashKeyValue)
            );
            return null;
        }

        private bool DoesIndexExistInTheListOfIndexes(string indexKey)
        {
            var indexList = this._cacheClient.Get<TableIndexList>(this.GetIndexListKeyInCache(this._hashKeyValue));
            return ((indexList != null) && (indexList.Contains(indexKey)));
        }

        internal void Log(string format, params object[] args)
        {
            var handler = this.OnLog;
            if (handler == null)
            {
                return;
            }
            string tableName = this._tableName;
            if (!string.IsNullOrEmpty(this._hashKeyValue))
            {
                tableName += ":" + this._hashKeyValue;
            }

            handler("EnyimTableCache(" + tableName + ") : " + string.Format(format, args));
        }

        #endregion
    }
}
