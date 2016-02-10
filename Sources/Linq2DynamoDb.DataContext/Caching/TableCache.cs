using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext.Caching
{
	public abstract partial class TableCache : ITableCache
	{
		protected Type _tableEntityType;
		protected string _tableName;
		protected string _hashKeyValue;
		protected ICacheClient _cacheClient;
        protected const string ProjectionIndexKeyPrefix = "[proj]";

		/// <summary>
		/// Limit to the number of indexes
		/// TODO: estimate this number more precisely
		/// </summary>
		protected const int MaxNumberOfIndexes = 100;

		/// <summary>
		/// Here all lock keys and their IDs are stored, for debugging purposes
		/// </summary>
		private readonly ConcurrentDictionary<string, int> _lockIds = new ConcurrentDictionary<string, int>();
		private static readonly Random Rnd = new Random(DateTime.Now.Millisecond);

		protected TableCache(ICacheClient client)
		{
			_cacheClient = client;
		}

		#region ITableCache implementation
		public event Action OnHit;
		public event Action<string> OnLog;
		public event Action OnMiss;

		public virtual void Initialize(string tableName, Type tableEntityType, Primitive hashKeyValue)
		{
			if (this._tableEntityType != null)
			{
				throw new InvalidOperationException("An attempt to re-use an instance of TableCache for another <table>:<hash key> pair was made. This is not allowed");
			}

			this._tableEntityType = tableEntityType;

			this._tableName = tableName;
			this._hashKeyValue = hashKeyValue == null ? string.Empty : hashKeyValue.AsString();
		}

		public virtual Document GetSingleEntity(EntityKey entityKey)
		{
			string entityKeyInCache = this.GetEntityKeyInCache(entityKey);
			if (entityKeyInCache == null)
				return null;

			CacheDocumentWrapper wrapper;
			if (!_cacheClient.TryGetValue<CacheDocumentWrapper>(entityKeyInCache, out wrapper))
			{
				this.OnMiss.FireSafely();
				return null;
			}

			this.OnHit.FireSafely();
			return wrapper.Document;
		}

		public virtual IEnumerable<Document> GetEntities(SearchConditions searchConditions, IEnumerable<string> projectedFields, string orderByFieldName, bool orderByDesc)
		{
			// first trying to find a full index
			string indexKey = searchConditions.Key;
			HashSet<EntityKey> index = this.TryLoadHealthyIndex(indexKey);

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
			HashSet<EntityKey> index = this.TryLoadHealthyIndex(searchConditions.Key);

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
			if (entityKeyInCache == null)
				return;

			// Putting the entity to cache, but only if it doesn't exist there.
			// That's because when loading from DynamoDb whe should never overwrite local updates.
			this._cacheClient.AddValue(entityKeyInCache, new CacheDocumentWrapper(doc));
		}

		public virtual void RemoveEntities(IEnumerable<EntityKey> entities)
		{
			var entityKeysInCache = entities
				.Select(this.GetEntityKeyInCache);

			Parallel.ForEach(entityKeysInCache, key => this._cacheClient.Remove(key));
		}

		/// <summary>
		/// Applies modifications to cached entities and indexes
		/// </summary>
		public virtual void UpdateCacheAndIndexes(IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
		{
			var allEntities = CombineEntityDictionaries(addedEntities, modifiedEntities, removedEntities.ToDictionary(k => k, k => (Document) null));

			// modifying/removing all entities in parallel
			var loopResult = Parallel.ForEach(allEntities, (entityPair, loopState) =>
			{
				bool result = true;
				if (entityPair.Value == null)
				{
					result = this._cacheClient.TryRemove(entityPair.Key);
				}
				else
				{
					result = this._cacheClient.SetValue(entityPair.Key, new CacheDocumentWrapper(entityPair.Value));
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
				return;
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

		public virtual IIndexCreator StartCreatingIndex(SearchConditions searchConditions) 
		{
			string indexKeyInCache = this.GetIndexKeyInCache(searchConditions.Key, this._hashKeyValue);
			return StartCreatingIndex<IndexCreator>(indexKeyInCache, searchConditions);
		}

		protected T StartCreatingIndex<T>(string indexKeyInCache, SearchConditions searchConditions) 
			where T : IIndexCreator
		{
			T creator = (T) Activator.CreateInstance(typeof(T), this, indexKeyInCache, searchConditions);

			if (!creator.StartCreatingIndex())
			{
				this.Log("Failed to start creating index ({0})", searchConditions.Key);
				return default(T);
			}

			return creator;
		}

		public virtual IIndexCreator StartCreatingProjectionIndex(SearchConditions searchConditions, IList<string> projectedFields)
		{
			string indexKey = this.GetProjectionIndexKey(searchConditions, projectedFields);
			string indexKeyInCache = this.GetIndexKeyInCache(indexKey, this._hashKeyValue);
			return StartCreatingProjectionIndex<ProjectionIndexCreator>(indexKey, indexKeyInCache, searchConditions);
		}

		protected T StartCreatingProjectionIndex<T>(string indexKey, string indexKeyInCache, SearchConditions searchConditions)
			where T : IIndexCreator
		{
			T creator = (T) Activator.CreateInstance(typeof(T), this, indexKey, indexKeyInCache, searchConditions);

			if (!creator.StartCreatingIndex())
			{
				this.Log("Failed to start creating projection index ({0})", indexKey);
				return default(T);
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
				if (this._cacheClient.AddValue(cacheLockKey, cacheLockId))
				{
					this._lockIds[lockKey] = cacheLockId;
					return;
				}

				Thread.Sleep(10);
			}

			// If we failed to acquire a lock within CacheLockTimeoutInSeconds 
			// (this means, that another process crached), then we should forcibly acquire it

			this.Log("Forcibly acquiring the table lock object {0} after {1} ms of waiting", lockKey, lockTimeout.TotalMilliseconds);

			this._cacheClient.SetValue(cacheLockKey, cacheLockId);
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
			int cacheLockId;
			if (!_cacheClient.TryGetValue(cacheLockKey, out cacheLockId))
			{
				// The cache miss might happen here, if a cache server crashed.
				// In this case we just silently return.
				this.Log("The table lock object {0} is missing in cache, but we don't care about that too much (probably, the cache node was restarted)", lockKey);
				return;
			}

			if (cacheLockId != lockId)
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

		#region Cache key methods
		/// <summary>
		/// Gets a cache key for an entity
		/// </summary>
		protected virtual string GetEntityKeyInCache(EntityKey entityKey)
		{
			return string.Format("{0}:{1}", _tableName, entityKey);
		}

		protected virtual string[] GetEntityKeysInCache(IEnumerable<EntityKey> entities)
		{
            string[] entityKeysInCache = entities
                .Select(this.GetEntityKeyInCache).ToArray();
			return entityKeysInCache;
		}

		protected virtual KeyValuePair<string, Document>[] CombineEntityDictionaries(params IDictionary<EntityKey, Document>[] entities)
		{
			IEnumerable<KeyValuePair<EntityKey, Document>> allEntities = new Dictionary<EntityKey, Document>(entities.Sum(x => x.Count));
			bool first = true;
			foreach (var item in entities)
			{
				allEntities = allEntities.Union(item);
			}
			return allEntities.Select(kv => new KeyValuePair<string, Document>(this.GetEntityKeyInCache(kv.Key), kv.Value)).ToArray();
		}

		/// <summary>
		/// Gets a cache key for an index. Returns null if the key is invalid for any reason.
		/// </summary>
		protected string GetIndexKeyInCache(string indexKey, string hashKeyValue)
		{
			return string.Format("{0}{1}{2}", _tableName, hashKeyValue, indexKey).ToBase64();
		}

		/// <summary>
		/// Gets a cache key for the list of all indexes
		/// </summary>
		protected string GetIndexListKeyInCache(string hashKeyValue)
		{
			return string.Format("{0}{1}:indexes", _tableName, hashKeyValue).ToBase64();
		}

		/// <summary>
		/// Gets a cache key for a table-wide lock
		/// </summary>
		protected string GetLockKeyInCache(string lockKey, string hashKeyValue)
		{
			return string.Format("{0}{1}{2}", _tableName, hashKeyValue, lockKey).ToBase64();
		}

		/// <summary>
		/// Composes a key for a projection index
		/// </summary>
		protected string GetProjectionIndexKey(SearchConditions searchConditions, IEnumerable<string> projectedFields)
		{
			return string.Format("{0}{1}; {2}",
				ProjectionIndexKeyPrefix,
				projectedFields.Aggregate((i, s) => string.Format("{0},{1}", i, s)),
				searchConditions.Key);
		}
		#endregion

		#region Index-related methods
		/// <summary>
		/// Loads the index by its key, ensuring everything
		/// </summary>
		protected HashSet<EntityKey> TryLoadHealthyIndex(string indexKey)
		{
			string indexKeyInCache = this.GetIndexKeyInCache(indexKey, this._hashKeyValue);

			// first trying to get the index from cache
			TableIndex index;
			if (!_cacheClient.TryGetValue(indexKeyInCache, out index))
			{
				return null;
			}

			// Checking, that index is mentioned in the list of indexes.
			// Only indexes from list are updated with local updates.
			if (!DoesIndexExistInTheListOfIndexes(indexKey))
			{
				_cacheClient.Remove(indexKeyInCache);
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
		/// Loads a projection index by its key
		/// </summary>
		protected Document[] TryLoadProjectionIndexEntities(string indexKey)
		{
			string indexKeyInCache = this.GetIndexKeyInCache(indexKey, this._hashKeyValue);
			if (indexKeyInCache == null)
			{
				return null;
			}

			// first trying to get the index from cache
			TableProjectionIndex index;
			if (!_cacheClient.TryGetValue(indexKeyInCache, out index))
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
				CacheDocumentWrapper wrapper;
				string entityKeyInCache = GetEntityKeyInCache(entityKey);
				if (!_cacheClient.TryGetValue(entityKeyInCache, out wrapper))
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
				this.GetIndexKeyInCache(indexKey, this._hashKeyValue),
				this.GetIndexListKeyInCache(this._hashKeyValue)
			);
			return null;
		}

		protected bool DoesIndexExistInTheListOfIndexes(string indexKey)
		{
			TableIndexList indexList;
			_cacheClient.TryGetValue(GetIndexListKeyInCache(_hashKeyValue), out indexList);
			return ((indexList != null) && (indexList.Contains(indexKey)));
		}

		/// <summary>
		/// Adds matching entities and removes removed entities from indexes
		/// </summary>
		protected virtual void UpdateIndexes(string hashKeyValue, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
		{
			string indexListKeyInCache = this.GetIndexListKeyInCache(hashKeyValue);

			TableIndexList indexes;
			if (!_cacheClient.TryGetValue<TableIndexList>(indexListKeyInCache, out indexes))
			{
				_cacheClient.Remove(indexListKeyInCache);
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
					this.UpdateIndex(indexKey, indexKeyInCache, addedEntities, removedEntities);

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
		protected virtual bool UpdateIndex(string indexKey, string indexKeyInCache, IDictionary<EntityKey, Document> addedEntities, ICollection<EntityKey> removedEntities)
		{
			TableIndex index;
			if (!_cacheClient.TryGetValue(indexKeyInCache, out index))
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

			_cacheClient.SetValue(indexKeyInCache, index);
			this.Log("Index ({0}) updated. {1} entities added, {2} entities removed", indexKey, addedEntities.Count, removedEntities.Count);
			return true;
		}

		/// <summary>
		/// Checks if a projection index should be dropped because of some added/modified/removed entities
		/// </summary>
		protected virtual bool UpdateProjectionIndex(string indexKey, string indexKeyInCache, IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
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

			TableProjectionIndex index;
			if (!_cacheClient.TryGetValue(indexKeyInCache, out index))
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

		/// <summary>
		/// Checks if the provided key is a projection index's key
		/// </summary>
		protected bool IsProjectionIndex(string indexKey)
		{
			return indexKey.StartsWith(ProjectionIndexKeyPrefix);
		}

		protected virtual bool PutIndexToList(string indexKey)
		{
			string indexListKeyInCache = this.GetIndexListKeyInCache(this._hashKeyValue);

			TableIndexList indexes;
			if (!_cacheClient.TryGetValue(indexListKeyInCache, out indexes))
				indexes = new TableIndexList(MaxNumberOfIndexes);

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

			try
			{
				_cacheClient.SetValue(indexListKeyInCache, indexes);
				return true;
			}
			catch 
			{
				return false;
			}
		}

		protected virtual void RemoveIndexFromList(string indexKey, string indexKeyInCache, string indexListKeyInCache)
		{
			// always removing the index itself
			_cacheClient.Remove(indexKeyInCache);

			TableIndexList indexes;
			if (!_cacheClient.TryGetValue(indexListKeyInCache, out indexes))
				return;

			indexes.Remove(indexKey);

			// store updated index list
			_cacheClient.ReplaceValue(indexListKeyInCache, indexes);
		}

		protected virtual void RemoveIndexesFromList(IDictionary<string, string> indexKeys, string indexListKeyInCache)
		{
			// always removing indexes themselves
			Parallel.ForEach(indexKeys.Values, indexKeyInCache => this._cacheClient.Remove(indexKeyInCache));

			TableIndexList indexes;
			if (!_cacheClient.TryGetValue(indexListKeyInCache, out indexes))
				return;

			foreach (KeyValuePair<string, string> pair in indexKeys)
			{
				indexes.Remove(pair.Value);
			}

			// store updated index list
			_cacheClient.ReplaceValue(indexListKeyInCache, indexes);
		}
		#endregion

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
				tableName = string.Format("{0}:{1}", tableName, this._hashKeyValue);
			}

			handler(string.Format("TableCache({0}) : ", tableName, string.Format(format, args)));
		}
	}
}
