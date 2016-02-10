﻿using System.Diagnostics;
using Amazon.DynamoDBv2.DocumentModel;
using Enyim.Caching.Memcached;

namespace Linq2DynamoDb.DataContext.Caching
{
    public partial class EnyimTableCache
    {
        /// <summary>
        /// Implements the process of creating and filling the projection (readonly) index
        /// </summary>
        private class EnyimProjectionIndexCreator : ProjectionIndexCreator
        {
            private EnyimTableCache _tableCache
			{
				get { return (EnyimTableCache) _parent; }
			}

            private ulong _indexVersionInCache;

            internal EnyimProjectionIndexCreator(EnyimTableCache parent, string indexKey, string indexKeyInCache, SearchConditions searchConditions)
				: base(parent, indexKey, indexKeyInCache, searchConditions)
            {
            }

            public override bool StartCreatingIndex()
            {
                // Marking the index as being rebuilt.
                // Note: we're using Set mode. This means, that if an index exists in cache, it will be 
                // overwritten. That's OK, because if an index exists in cache, then in most cases we
                // will not be in this place (the data will be simply read from cache).
                if (!this._tableCache._cacheClient.SetValue(this._indexKeyInCache, this._index))
                {
                    this._tableCache._cacheClient.Remove(this._indexKeyInCache);
                    return false;
                }

                // (re)registering it in the list of indexes (it should be visible for update operations)
                if (!this._tableCache.PutIndexToList(this._indexKey))
                {
                    this._tableCache._cacheClient.Remove(this._indexKeyInCache);
                    return false;
                }

                // remembering the index's current version
                var casResult = this._tableCache._memcachedClient.GetWithCas<TableIndex>(this._indexKeyInCache);
                if
                (
                    (casResult.StatusCode != 0)
                    ||
                    (casResult.Result == null)
                )
                {
                    this._tableCache._cacheClient.Remove(this._indexKeyInCache);
                    return false;
                }

                this._indexVersionInCache = casResult.Cas;
                this._tableCache.Log("Index ({0}) was marked as being rebuilt", (object)this._indexKey);
                return true;
            }

            public override void AddEntityToIndex(EntityKey entityKey, Document doc)
            {
                // when creating a projection index, the entity key should not be passed
                Debug.Assert(entityKey == null);

                // adding document to index
                this._index.AddEntity(doc);
            }

            public override void Dispose()
            {
                this._index.IsBeingRebuilt = false;

                // saving the index to cache only if it's version didn't change since we started reading results from DynamoDb
                var casResult = this._tableCache._memcachedClient.Cas
                (
                    StoreMode.Replace, 
                    this._indexKeyInCache, 
                    this._index, 
                    this._tableCache._cacheClient.DefaultTimeToLive, 
                    this._indexVersionInCache
                );

                if (casResult.Result)
                {
                    this._tableCache.Log("Index ({0}) with {1} entities saved to cache", (object)this._indexKey, this._index.Entities.Length);
                }
                else
                {
                    this._tableCache.Log("Index ({0}) wasn't saved to cache due to version conflict", (object)this._indexKey);
                }
            }
        }
    }
}
