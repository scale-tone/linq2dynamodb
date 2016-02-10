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

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Implements caching in MemcacheD/ElastiCache via Enyim caching client
    /// </summary>
    public partial class EnyimTableCache : TableCache
    {
		private MemcachedClient _memcachedClient
		{
			get { return (MemcachedClient) _cacheClient; }
		}
        public EnyimTableCache(MemcachedClient cacheClient, TimeSpan cacheItemsTtl)
			: base(new EnyimMemcachedClient(cacheClient, cacheItemsTtl))
        {
        }
		public EnyimTableCache(EnyimMemcachedClient cacheClient)
			: base(cacheClient)
		{
		}

        #region ITableCache implementation

        public override void Initialize(string tableName, Type tableEntityType, Primitive hashKeyValue)
        {
			base.Initialize(tableName, tableEntityType, hashKeyValue);

            if (GetIndexListKeyInCache(_hashKeyValue).Length > MaxKeyLength)
            {
                throw new ArgumentException("The hash key value is too long for MemcacheD. Cannot use cache with that value.");
            }
        }

        public override IIndexCreator StartCreatingIndex(SearchConditions searchConditions)
        {
            string indexKeyInCache = this.GetIndexKeyInCache(searchConditions.Key, this._hashKeyValue);
            if (indexKeyInCache.Length > MaxKeyLength)
            {
                this.Log("Index key ({0}) is too long", searchConditions.Key);
                return null;
            }

			return base.StartCreatingIndex<EnyimIndexCreator>(indexKeyInCache, searchConditions);
        }

        public override IIndexCreator StartCreatingProjectionIndex(SearchConditions searchConditions, IList<string> projectedFields)
        {
            string indexKey = this.GetProjectionIndexKey(searchConditions, projectedFields);
            string indexKeyInCache = this.GetIndexKeyInCache(indexKey, this._hashKeyValue);
            if (indexKeyInCache.Length > MaxKeyLength)
            {
                this.Log("Index key ({0}) is too long", searchConditions.Key);
                return null;
            }

            return base.StartCreatingProjectionIndex<EnyimProjectionIndexCreator>(indexKey, indexKeyInCache, searchConditions);
        }
        #endregion

        #region Private Properties

        /// <summary>
        /// Entities and indexes with key longer than this limit are not saved to cache
        /// </summary>
        private const int MaxKeyLength = 250;

        /// <summary>
        /// The number of times to try an optimistic update operation
        /// </summary>
        private const int MaxUpdateAttempts = 10;

        #endregion

        #region Private Methods
		protected override string GetEntityKeyInCache(EntityKey entityKey)
		{
			string entityKeyInCache = base.GetEntityKeyInCache(entityKey);
			if (entityKeyInCache.Length > MaxKeyLength)
			{
				return null;
			}
			return entityKeyInCache;
		}
		protected override string[] GetEntityKeysInCache(IEnumerable<EntityKey> entities)
		{
			var entityKeysInCache = entities
				.Select(this.GetEntityKeyInCache)
				// extracting too long keys
				.Where(ek => ek.Length <= MaxKeyLength)
				.ToArray();
			return entityKeysInCache;
		}
		protected override KeyValuePair<string, Document>[] CombineEntityDictionaries(params IDictionary<EntityKey, Document>[] entities)
		{
			var allEntities = base.CombineEntityDictionaries(entities);
			allEntities = allEntities
				// extracting entities with too long keys
				.Where(kv => kv.Key.Length <= MaxKeyLength)
				.ToArray();
			return allEntities;
		}

		/// <summary>
        /// Adds matching entities and removes removed entities from an index
        /// </summary>
        protected override bool UpdateIndex(string indexKey, string indexKeyInCache, IDictionary<EntityKey, Document> addedEntities, ICollection<EntityKey> removedEntities)
        {
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexGetResult = _memcachedClient.GetWithCas<TableIndex>(indexKeyInCache);
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
                        var entityPair in addedEntities.Where
                        (
                            entityPair => index.MatchesSearchConditions(entityPair.Value, this._tableEntityType)
                        )
                    )
                    {
                        index.Index.Add(entityPair.Key);
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

                if (_memcachedClient.Cas(StoreMode.Set, indexKeyInCache, index, _cacheClient.DefaultTimeToLive, indexGetResult.Cas).Result)
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
        protected override bool UpdateProjectionIndex(string indexKey, string indexKeyInCache, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
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

            var indexGetResult = _memcachedClient.GetWithCas<TableProjectionIndex>(indexKeyInCache);
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

        protected override bool PutIndexToList(string indexKey)
        {
            string indexListKeyInCache = this.GetIndexListKeyInCache(this._hashKeyValue);

            // trying multiple times
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexesGetResult = _memcachedClient.GetWithCas<TableIndexList>(indexListKeyInCache);

                if (indexesGetResult.StatusCode != 0)
                {
                    this.Log("Failed to put index ({0}) to list because of a communication error", indexKey);
                    return false;
                }

                var indexes = indexesGetResult.Result ?? new TableIndexList(MaxNumberOfIndexes);
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

                if (_memcachedClient.Cas(StoreMode.Set, indexListKeyInCache, indexes, _cacheClient.DefaultTimeToLive, indexesGetResult.Cas).Result)
                {
                    return true;
                }
            }

            this.Log("Failed to put index ({0}) to list after {1} attempts", indexKey, MaxUpdateAttempts);
            return false;
        }

        protected override void RemoveIndexFromList(string indexKey, string indexKeyInCache, string indexListKeyInCache)
        {
            // always removing the index itself
            this._cacheClient.Remove(indexKeyInCache);

            // trying multiple times
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexesGetResult = _memcachedClient.GetWithCas<TableIndexList>(indexListKeyInCache);

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

                var casResult = _memcachedClient.Cas(StoreMode.Set, indexListKeyInCache, indexes, _cacheClient.DefaultTimeToLive, indexesGetResult.Cas);
                if (casResult.Result)
                {
                    return;
                }
            }

            this.Log("Failed to remove index ({0}) from list after {1} attempts. Removing the whole list.", indexKey, MaxUpdateAttempts);
            this._cacheClient.Remove(indexListKeyInCache);
        }

        protected override void RemoveIndexesFromList(IDictionary<string, string> indexKeys, string indexListKeyInCache)
        {
            // always removing indexes themselves
            Parallel.ForEach(indexKeys.Values, indexKeyInCache => this._cacheClient.Remove(indexKeyInCache));

            // trying multiple times
            //TODO: implement a helper for multiple attempts
            for (int i = 0; i < MaxUpdateAttempts; i++)
            {
                var indexesGetResult = _memcachedClient.GetWithCas<TableIndexList>(indexListKeyInCache);

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

                var casResult = _memcachedClient.Cas(StoreMode.Set, indexListKeyInCache, indexes, _cacheClient.DefaultTimeToLive, indexesGetResult.Cas);
                if (casResult.Result)
                {
                    return;
                }
            }

            this.Log("Failed to remove {0} indexes from list after {1} attempts. Removing the whole list.", indexKeys.Count, MaxUpdateAttempts);
            this._cacheClient.Remove(indexListKeyInCache);
        }
        #endregion
    }
}
