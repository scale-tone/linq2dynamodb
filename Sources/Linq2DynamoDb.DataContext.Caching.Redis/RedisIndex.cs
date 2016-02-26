using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    public partial class RedisTableCache
    {
        /// <summary>
        /// Represents an index in Redis.
        /// The index is stored as a Hash. The entities are stored as Strings.
        /// </summary>
        private class RedisIndex : IIndexCreator
        {
            protected readonly string IndexKeyInCache;
            protected readonly RedisTransactionWrapper RedisTransaction;
            private readonly RedisTableCache _parent;
            private readonly SearchConditions _searchConditions;

            public RedisIndex(RedisTableCache parent, string indexKey, SearchConditions searchConditions)
            {
                this._parent = parent;
                this.IndexKeyInCache = indexKey;
                this._searchConditions = searchConditions;

                try
                {
                    // (re)registering it in the list of indexes (it should be visible for update operations)
                    this._parent._redis.SetHashWithRetries(this._parent.GetIndexListKeyInCache(), this.IndexKeyInCache, this._searchConditions.ToRedisValue());

                    // asynchronously checking the total number of indexes
                    Task.Run(() => this.TryKeepIndexListSlim(indexKey));

                    // creating an index and marking it as being rebuilt
                    this._parent._redis.CreateNewHashWithRetries(this.IndexKeyInCache, IndexVersionField.Name, IndexVersionField.IsBeingRebuiltValue);

                    this.RedisTransaction = this._parent._redis.BeginTransaction(IndexVersionField.IsBeingRebuiltCondition(this.IndexKeyInCache));
                    this._parent.Log("Index ({0}) was marked as being rebuilt", this._searchConditions.Key);
                }
                catch (Exception ex)
                {
                    this._parent.Log("Failed to start creating index: {0}", ex);

                    // if something goes wrong - dropping the index (but only from the list - we don't want to let parallel update transaction to fail)
                    this._parent._redis.RemoveHashFieldsWithRetries(this._parent.GetIndexListKeyInCache(), this.IndexKeyInCache);
                    throw;
                }
            }

            public virtual void AddEntityToIndex(EntityKey entityKey, Document doc)
            {
                try
                {
                    // putting the entity to cache, but never overwriting local changes
                    this._parent._redis.SetWithRetries(entityKey.ToRedisKey(), new CacheDocumentWrapper(doc), When.NotExists);
                }
                catch (RedisCacheException)
                {
                    // this means the item exists in cache
                }
                catch (Exception ex)
                {
                    this._parent.Log("Error while creating index ({0}): an entity with key {1} failed to be put to cache with exception: {2}", this._searchConditions.Key, entityKey, ex);
                }

                this.RedisTransaction.HashSet(this.IndexKeyInCache, entityKey.ToRedisValue(), string.Empty);
            }

            public void Dispose()
            {
                try
                {
                    // unmarking the index, so that it becomes usable after the transaction succeedes
                    this.RedisTransaction.HashSet(this.IndexKeyInCache, IndexVersionField.Name, IndexVersionField.ZeroVersionValue);
                    this.RedisTransaction.Execute();
                }
                catch (Exception ex)
                {
                    this._parent.Log("Index ({0}) wasn't saved to cache because of exception {1}", this._searchConditions.Key, ex);
                }
            }

            /// <summary>
            /// Loads all entities from an index or throws, if something wasn't found
            /// </summary>
            public static Document[] LoadIndexEntities(RedisWrapper redis, string indexKey, string indexListKey)
            {
                var rawIndex = redis.GetHashFieldsWithRetries(indexKey);

                if (rawIndex.Length <= 0)
                {
                    redis.RemoveHashFieldsWithRetries(indexListKey, indexKey);
                    throw new RedisCacheException("Index wasn't found in cache");
                }

                var indexVersionField = new IndexVersionField();
                var entityKeys =
                    (
                        from hashField in rawIndex
                        where !indexVersionField.TryInitialize(hashField)
                        select hashField.Name.ToEntityKey()
                    )
                    .ToList();

                // if the index is being rebuilt
                if (indexVersionField.IsIndexBeingRebuilt)
                {
                    throw new RedisCacheException("Index is being rebuilt");
                }

                try
                {
                    var wrappers = redis.GetWithRetries<CacheDocumentWrapper>(entityKeys.Select(k => k.ToRedisKey()).ToArray());
                    return wrappers.Select(w => w.Document).ToArray();
                }
                catch (Exception)
                {
                    // if failed to load all entities - dropping the index
                    redis.RemoveHashFieldsWithRetries(indexListKey, indexKey);
                    redis.RemoveWithRetries(indexKey);
                    throw;
                }
            }

            /// <summary>
            /// TODO: estimate the limit more precisely
            /// </summary>
            private const int MaxNumberOfIndexes = 100;

            private void TryKeepIndexListSlim(string lastAddedIndexKey)
            {
                try
                {
                    if (MaxNumberOfIndexes >= this._parent._redis.GetHashLengthWithRetries(this._parent.GetIndexListKeyInCache()))
                    {
                        return;
                    }

                    // dropping some randomly selected indexes

                    var indexListEntries = this._parent._redis.GetHashFieldsWithRetries(this._parent.GetIndexListKeyInCache());
                    var indexKeys = indexListEntries.Select(he => he.Name).Where(k => k != lastAddedIndexKey).ToList();
                    var indexKeysToRemove = new List<RedisValue>();

                    while (indexKeys.Count > MaxNumberOfIndexes)
                    {
                        int i = Rnd.Next(0, indexKeys.Count - 1);
                        indexKeysToRemove.Add(indexKeys[i]);
                        indexKeys.RemoveAt(i);
                    }

                    this._parent.Log("Dropping {0} indexes from cache", indexKeysToRemove.Count);

                    this._parent._redis.RemoveHashFieldsWithRetries(this._parent.GetIndexListKeyInCache(), indexKeysToRemove.ToArray());
                }
                catch (Exception ex)
                {
                    this._parent.Log("Failed to shrink the list of indexes: {0}", ex);
                }
            }
        }
    }
}
