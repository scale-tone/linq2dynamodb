using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext;
using Linq2DynamoDb.DataContext.Caching;

namespace MovieReviews.AspNet.Mvc
{
    public class AspNetCacheAdapter : ITableCache
    {
        public void Initialize(string tableName, Type tableEntityType, Primitive hashKeyValue)
        {
            throw new NotImplementedException();
        }

        public Document GetSingleEntity(EntityKey entityKey)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<Document> GetEntities(SearchConditions searchConditions, string orderByFieldName, bool orderByDesc)
        {
            throw new NotImplementedException();
        }

        public int? GetCount(SearchConditions searchConditions)
        {
            throw new NotImplementedException();
        }

        public void PutSingleLoadedEntity(EntityKey entityKey, Document doc)
        {
            throw new NotImplementedException();
        }

        public void UpdateCacheAndIndexes(IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities)
        {
            throw new NotImplementedException();
        }

        public void RemoveEntities(IEnumerable<EntityKey> entities)
        {
            throw new NotImplementedException();
        }

        public IIndexCreator StartCreatingIndex(SearchConditions searchConditions)
        {
            throw new NotImplementedException();
        }

        public event Action OnHit;
        public event Action OnMiss;
        public event Action<string> OnLog;
    }
}