using System;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Represents an object for creating and filling an index in cache.
    /// The implementation saves the index to cache upon disposal.
    /// </summary>
    public interface IIndexCreator : IDisposable
    {
        void AddEntityToIndex(EntityKey entityKey, Document doc);
		bool StartCreatingIndex();
    }
}
