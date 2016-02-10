using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching
{
	public abstract partial class TableCache
	{
		protected class IndexCreator : IIndexCreator
		{
			protected readonly TableCache _parent;
			protected TableIndex _index;
			protected readonly string _indexKey;
			protected readonly string _indexKeyInCache;

			internal IndexCreator(TableCache parent, string indexKeyInCache, SearchConditions searchConditions)
			{
				_parent = parent;
				_index = new TableIndex(searchConditions);
				_indexKey = searchConditions.Key;
				_indexKeyInCache = indexKeyInCache;
			}

			public virtual bool StartCreatingIndex()
			{
				// Marking the index as being rebuilt.
				// Note: we're using Set mode. This means, that if an index exists in cache, it will be 
				// overwritten. That's OK, because if an index exists in cache, then in most cases we
				// will not be in this place (the data will be simply read from cache).
				_parent._cacheClient.SetValue(_indexKeyInCache, _index);

				// (re)registering it in the list of indexes (it should be visible for update operations)
				if (!this._parent.PutIndexToList(_indexKey))
				{
					_parent._cacheClient.Remove(_indexKeyInCache);
					return false;
				}

				_parent.Log("Index ({0}) was marked as being rebuilt", _indexKey);
				return true;
			}

			public virtual void AddEntityToIndex(EntityKey entityKey, Document doc)
			{
				string key = _parent.GetEntityKeyInCache(entityKey);

				// adding key to index (it's essential to do this _before_ checking the key length - the index should fail to be read next time)
				_index.Index.Add(entityKey);

				// Putting the entity to cache, but only if it doesn't exist there.
				// That's because when loading from DynamoDb whe should never overwrite local updates.
				_parent._cacheClient.SetValue(key, new CacheDocumentWrapper(doc));
			}

			public virtual void Dispose()
			{
				if (this._index == null)
				{
					_parent._cacheClient.Remove(_indexKeyInCache);
					return;
				}

				this._index.IsBeingRebuilt = false;

				// saving the index to cache
				_parent._cacheClient.ReplaceValue(_indexKeyInCache, _index);
				_parent.Log("Index ({0}) with {1} entities saved to cache", _indexKey, _index.Index.Count);
			}
		}
	}
}