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
		protected class ProjectionIndexCreator : IIndexCreator
		{
			protected readonly TableCache _parent;

			protected readonly TableProjectionIndex _index;
			protected readonly string _indexKey;
			protected readonly string _indexKeyInCache;

			internal ProjectionIndexCreator(TableCache parent, string indexKey, string indexKeyInCache, SearchConditions searchConditions)
			{
				this._parent = parent;
				this._index = new TableProjectionIndex(searchConditions);
				this._indexKey = indexKey;
				this._indexKeyInCache = indexKeyInCache;
			}

			public virtual bool StartCreatingIndex()
			{
				// Marking the index as being rebuilt.
				// Note: we're using Set mode. This means, that if an index exists in cache, it will be 
				// overwritten. That's OK, because if an index exists in cache, then in most cases we
				// will not be in this place (the data will be simply read from cache).
				_parent._cacheClient.SetValue(_indexKeyInCache, _index);

				// (re)registering it in the list of indexes (it should be visible for update operations)
				if (!this._parent.PutIndexToList(this._indexKey))
				{
					this._parent._cacheClient.Remove(this._indexKeyInCache);
					return false;
				}

				this._parent.Log("Index ({0}) was marked as being rebuilt", (object) this._indexKey);
				return true;
			}

			public virtual void AddEntityToIndex(EntityKey entityKey, Document doc)
			{
				// adding document to index
				this._index.AddEntity(doc);
			}

			public virtual void Dispose()
			{
				this._index.IsBeingRebuilt = false;

				// saving the index to cache only if its version didn't change since we started reading results from DynamoDb
				_parent._cacheClient.ReplaceValue(_indexKeyInCache, _index);
				this._parent.Log("Index ({0}) with {1} entities saved to cache", (object) this._indexKey, this._index.Entities.Length);
			}
		}
	}
}
