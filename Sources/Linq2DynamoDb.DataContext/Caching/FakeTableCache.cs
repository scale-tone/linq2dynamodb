using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Implements no caching
    /// </summary>
    internal class FakeTableCache : ITableCache
    {
        public void Initialize(string tableName, Type tableEntityType, Primitive hashKeyValue)
        {
            
        }

        public Document GetSingleEntity(EntityKey entityKey)
        {
            return null;
        }

        public void PutSingleLoadedEntity(EntityKey entityKey, Document doc)
        {
            
        }

        public IEnumerable<Document> GetEntities(SearchConditions searchConditions, IEnumerable<string> projectedFields, string orderByFieldName, bool orderByDesc)
        {
            return null;
        }

        public int? GetCount(SearchConditions searchConditions)
        {
            return null;
        }

        public void RemoveEntities(IEnumerable<EntityKey> entities)
        {
        }

        public void UpdateCacheAndIndexes(IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
        }

        public IIndexCreator StartCreatingIndex(SearchConditions searchConditions)
        {
            return null;
        }

        public IIndexCreator StartCreatingProjectionIndex(SearchConditions searchConditions, IList<string> projectedFields)
        {
            return null;
        }

        public IDisposable AcquireTableLock(string lockKey, TimeSpan lockTimeout)
        {
            throw new NotImplementedException("Table-wide locks require a cache implementation");
        }

        public event Action OnHit;
        public event Action OnMiss;
        public event Action<string> OnLog;
    }
}
