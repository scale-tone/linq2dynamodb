using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching.MemcacheD
{
    /// <summary>
    /// Implements a projection index stored in cache
    /// </summary>
    [Serializable]
    public class TableProjectionIndex : TableIndex
    {
        private readonly List<CacheDocumentWrapper> _wrappers = new List<CacheDocumentWrapper>();

        public void AddEntity(Document doc)
        {
            this._wrappers.Add(new CacheDocumentWrapper(doc));
        }

        public Document[] Entities { get { return this._wrappers.Select(w => w.Document).ToArray(); } }

        public TableProjectionIndex(SearchConditions conditions) : base(conditions)
        {
        }
    }
}
