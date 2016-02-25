using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Implements an index stored in cache
    /// </summary>
    [Serializable]
    public class TableIndex
    {
        /// <summary>
        /// Shows, that the index is in progress of being rebuilt
        /// </summary>
        public bool IsBeingRebuilt;

        /// <summary>
        /// Keys of entities, that conform to the list of conditions
        /// </summary>
        public readonly HashSet<EntityKey> Index;

        /// <summary>
        /// List of conditions
        /// </summary>
        private readonly SearchConditions _conditions;

        public TableIndex(SearchConditions conditions)
        {
            this.Index = new HashSet<EntityKey>();
            this._conditions = conditions;
            this.IsBeingRebuilt = true;
        }

        /// <summary>
        /// Checks if a Document satisfies the list of conditions for this index
        /// </summary>
        public bool MatchesSearchConditions(Document doc, Type entityType)
        {
            return this._conditions.MatchesSearchConditions(doc, entityType);
        }
    }
}
