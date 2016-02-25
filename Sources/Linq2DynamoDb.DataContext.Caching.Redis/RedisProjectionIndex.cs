using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    public partial class RedisTableCache
    {
        /// <summary>
        /// Creates a projection index. Just stores entities in a hash.
        /// </summary>
        private class RedisProjectionIndex : RedisIndex
        {
            private int _count;

            public RedisProjectionIndex(RedisTableCache parent, string indexKey, SearchConditions searchConditions) : base(parent, indexKey, searchConditions)
            {
            }

            public override void AddEntityToIndex(EntityKey entityKey, Document doc)
            {
                this.RedisTransaction.HashSet(this.IndexKeyInCache, this._count++, new CacheDocumentWrapper(doc).ToRedisValue());
            }

            /// <summary>
            /// Loads all entities from an index or throws, if something wasn't found
            /// </summary>
            public static Document[] LoadProjectionIndexEntities(RedisWrapper redis, string indexKey, string indexListKey)
            {
                var rawIndex = redis.GetHashFieldsWithRetries(indexKey);

                if (rawIndex.Length <= 0)
                {
                    redis.RemoveHashFieldsWithRetries(indexListKey, indexKey);
                    throw new RedisCacheException("Index wasn't found in cache");
                }

                var indexVersionField = new IndexVersionField();
                var wrappers =
                    (
                        from hashField in rawIndex
                        where !indexVersionField.TryInitialize(hashField)
                        select hashField.Value.ToObject<CacheDocumentWrapper>()
                    )
                    .ToList();

                // if the index is being rebuilt
                if (indexVersionField.IsIndexBeingRebuilt)
                {
                    throw new RedisCacheException("Index is being rebuilt");
                }

                return wrappers.Select(w => w.Document).ToArray();
            }
        }
    }
}
