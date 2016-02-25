using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    /// <summary>
    /// Encapsulates manipulation with Version field stored along with index in a Redis Hash
    /// </summary>
    internal class IndexVersionField
    {
        public const string Name = "IndexVersion";

        public static readonly long IsBeingRebuiltValue = long.MinValue + 1; // RedisValue implementation has a bug - it can't deserialize long.MinValue. So we're using long.MinValue+1
        public static readonly long ZeroVersionValue = 0;

        public static Condition IsBeingRebuiltCondition(RedisKey hashKey)
        {
            return Condition.HashEqual(hashKey, Name, IsBeingRebuiltValue);
        }

        public bool IsIndexBeingRebuilt { get; private set; }

        public IndexVersionField()
        {
            this.IsIndexBeingRebuilt = true;
        }

        /// <summary>
        /// Checks, whether a hash entry is the Version field. If yes, takes it's value and compares with 0.
        /// Version less than 0 means, that the index is being rebuilt.
        /// </summary>
        public bool TryInitialize(HashEntry hashEntry)
        {
            if (hashEntry.Name != Name)
            {
                return false;
            }
            long indexVersion = (long)hashEntry.Value;
            this.IsIndexBeingRebuilt = indexVersion < 0;
            return true;
        }
    }
}
