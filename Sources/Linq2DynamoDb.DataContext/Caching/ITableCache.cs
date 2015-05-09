using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Interface for implementing caching. There might be custom implementations of this.
    /// </summary>
    public interface ITableCache
    {
        /// <summary>
        /// Always called by TableDefinitionWrapper in it's constructor. hashKeyValue might be null.
        /// </summary>
        void Initialize(string tableName, Type tableEntityType, Primitive hashKeyValue);

        /// <summary>
        /// Tries to get a single entity from cache by it's key
        /// </summary>
        Document GetSingleEntity(EntityKey entityKey);

        /// <summary>
        /// Tries to find a saved index for the passed conditions and return documents from that index 
        /// </summary>
        IEnumerable<Document> GetEntities(SearchConditions searchConditions, IEnumerable<string> projectedFields, string orderByFieldName, bool orderByDesc);

        /// <summary>
        /// Tries to count entities, that satisfy the conditions
        /// </summary>
        int? GetCount(SearchConditions searchConditions);

        /// <summary>
        /// Puts to cache a single entity loaded from DynamoDb. Should never overwrite local changes.
        /// </summary>
        /// <param name="entityKey"></param>
        /// <param name="doc"></param>
        void PutSingleLoadedEntity(EntityKey entityKey, Document doc);
        
        /// <summary>
        /// Adds/updates/removes a list of entities in cache and also updates the indexes with added/removed entities 
        /// </summary>
        void UpdateCacheAndIndexes(IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, ICollection<EntityKey> removedEntities);

        /// <summary>
        /// Removes a list of entities from cache. Called when an update in DynamoDb partially failed.
        /// </summary>
        /// <param name="entities"></param>
        void RemoveEntities(IEnumerable<EntityKey> entities);

        /// <summary>
        /// Returns a disposable object, that is used to fill in an index. 
        /// Called before a query/scan operation is started against DynamoDb
        /// </summary>
        IIndexCreator StartCreatingIndex(SearchConditions searchConditions);

        /// <summary>
        /// Returns a disposable object, that is used to fill in a readonly projection index. 
        /// Called before a query/scan operation is started against DynamoDb
        /// </summary>
        IIndexCreator StartCreatingProjectionIndex(SearchConditions searchConditions, IList<string> projectedFields);

        /// <summary>
        /// Acquires a table-wide named lock and returns a disposable object, that represents it.
        /// This method is not used by DataContext internally, so feel free to throw NotImplementedException.
        /// </summary>
        IDisposable AcquireTableLock(string lockKey, TimeSpan lockTimeout);

        /// <summary>
        /// Occurs when an item is successfully loaded from cache
        /// </summary>
        event Action OnHit;

        /// <summary>
        /// Occurs when an item couldn't be loaded from cache
        /// </summary>
        event Action OnMiss;

        /// <summary>
        /// Occurs when cache implementation wants to log some debugging info
        /// </summary>
        event Action<string> OnLog;
    }
}
