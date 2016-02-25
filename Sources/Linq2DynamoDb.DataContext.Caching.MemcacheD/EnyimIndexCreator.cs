using Amazon.DynamoDBv2.DocumentModel;
using Enyim.Caching.Memcached;

namespace Linq2DynamoDb.DataContext.Caching.MemcacheD
{
    public partial class EnyimTableCache
    {
        /// <summary>
        /// Implements the process of creating and filling the index
        /// </summary>
        private class EnyimIndexCreator : IIndexCreator
        {
            private readonly EnyimTableCache _parent;

            private TableIndex _index;
            private readonly string _indexKey;
            private readonly string _indexKeyInCache;
            private ulong _indexVersionInCache;

            internal EnyimIndexCreator(EnyimTableCache parent, string indexKeyInCache, SearchConditions searchConditions)
            {
                this._parent = parent;
                this._index = new TableIndex(searchConditions);
                this._indexKey = searchConditions.Key;
                this._indexKeyInCache = indexKeyInCache;
            }

            public bool StartCreatingIndex()
            {
                // (re)registering it in the list of indexes (it should be visible for update operations)
                if (!this._parent.PutIndexToList(this._indexKey))
                {
                    this._parent._cacheClient.Remove(this._indexKeyInCache);
                    return false;
                }

                // Marking the index as being rebuilt. If we don't mark it, a parallel read might return 0 records.
                // Note: we're using Set mode. This means, that if an index exists in cache, it will be 
                // overwritten. That's OK, because if an index exists in cache, then in most cases we
                // will not be in this place (the data will be simply read from cache).
                if (!this._parent._cacheClient.Store(StoreMode.Set, this._indexKeyInCache, this._index, this._parent._ttl))
                {
                    this._parent._cacheClient.Remove(this._indexKeyInCache);
                    return false;
                }

                // remembering the index's current version
                var casResult = this._parent._cacheClient.GetWithCas<TableIndex>(this._indexKeyInCache);
                if
                (
                    (casResult.StatusCode != 0)
                    ||
                    (casResult.Result == null)
                )
                {
                    this._parent._cacheClient.Remove(this._indexKeyInCache);
                    return false;
                }

                // Checking, that index wasn't changed between previous two operations.
                // This is the only way, because there's no such operation as 'AddAndReturnCas' in Enyim
                if((!casResult.Result.IsBeingRebuilt) || (casResult.Result.Index.Count != 0))
                {
                    this._parent._cacheClient.Remove(this._indexKeyInCache);
                    return false;
                }

                this._indexVersionInCache = casResult.Cas;
                this._parent.Log("Index ({0}) was marked as being rebuilt", (object)this._indexKey);
                return true;
            }

            public void AddEntityToIndex(EntityKey entityKey, Document doc)
            {
                // stop filling index, if at least one entity failed to be added
                if (this._index == null)
                {
                    return;
                }

                string key = this._parent.GetEntityKeyInCache(entityKey);
                if (key.Length > MaxKeyLength)
                {
                    this._index = null;
                    return;
                }

                // adding key to index
                this._index.Index.Add(entityKey);

                // Putting the entity to cache, but only if it doesn't exist there.
                // That's because when loading from DynamoDb whe should never overwrite local updates.
                this._parent._cacheClient.Store(StoreMode.Add, key, new CacheDocumentWrapper(doc), this._parent._ttl);
            }

            public void Dispose()
            {
                if (this._index == null)
                {
                    this._parent.Log("Index ({0}) wasn't saved to cache, probably, because entity key was too large", (object)this._indexKey);
                    this._parent._cacheClient.Remove(this._indexKeyInCache);
                    return;
                }

                this._index.IsBeingRebuilt = false;

                // saving the index to cache only if it's version didn't change since we started reading results from DynamoDb
                var casResult = this._parent._cacheClient.Cas
                (
                    StoreMode.Replace, 
                    this._indexKeyInCache, 
                    this._index, 
                    this._parent._ttl, 
                    this._indexVersionInCache
                );

                if (casResult.Result)
                {
                    this._parent.Log("Index ({0}) with {1} entities saved to cache", (object)this._indexKey, this._index.Index.Count);
                }
                else
                {
                    this._parent.Log("Index ({0}) wasn't saved to cache due to version conflict", (object)this._indexKey);
                }
            }
        }
    }
}
