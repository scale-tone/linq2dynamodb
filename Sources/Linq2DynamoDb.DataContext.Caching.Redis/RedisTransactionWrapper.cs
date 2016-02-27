using System;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Utils;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    internal class RedisTransactionWrapper
    {
        public RedisTransactionWrapper(ITransaction redisTransaction, TimeSpan ttl, Action<string> onLog)
        {
            this._redisTransaction = redisTransaction;
            this._ttl = ttl;
            this._onLog = onLog;
        }

        /// <summary>
        /// Can be used for logging the list of keys, that will be affected by this transaction.
        /// </summary>
        public event Action<RedisKey> OnKeyAffected;

        public void Set(RedisKey key, RedisValue value)
        {
            this.WrapTaskWithLogging(this._redisTransaction.StringSetAsync(key, value, this._ttl));
            this.OnKeyAffected.FireSafely(key);
        }

        public void Remove(RedisKey key)
        {
            this.WrapTaskWithLogging(this._redisTransaction.KeyDeleteAsync(key));
            this.OnKeyAffected.FireSafely(key);
        }

        public void HashSet(RedisKey hashKey, RedisValue fieldName, RedisValue fieldValue)
        {
            this.WrapTaskWithLogging(this._redisTransaction.HashSetAsync(hashKey, fieldName, fieldValue));
            this.OnKeyAffected.FireSafely(hashKey);
        }

        public void HashRemove(RedisKey hashKey, RedisValue fieldName)
        {
            this.WrapTaskWithLogging(this._redisTransaction.HashDeleteAsync(hashKey, fieldName));
            this.OnKeyAffected.FireSafely(hashKey);
        }

        public void HashIncrement(RedisKey hashKey, RedisValue fieldName)
        {
            this.WrapTaskWithLogging(this._redisTransaction.HashIncrementAsync(hashKey, fieldName));
            this.OnKeyAffected.FireSafely(hashKey);
        }

        public void AddHashFieldExistsCondition(RedisKey hashKey, RedisValue fieldName)
        {
            this._redisTransaction.AddCondition(Condition.HashExists(hashKey, fieldName));
            this.OnKeyAffected.FireSafely(hashKey);
        }

        public void Execute()
        {
            // No sence in doing retries here: each exception clears the internal StackExchange.Redis command buffer.
            if (!this._redisTransaction.Execute())
            {
                throw new RedisCacheException("Failed to commit Redis Transaction");
            }
        }

        private readonly ITransaction _redisTransaction;
        private readonly TimeSpan _ttl;
        private readonly Action<string> _onLog;

        private void WrapTaskWithLogging(Task task)
        {
            task.ContinueWith(t => { this._onLog("RedisTransactionWrapper: " + t.Exception); }, TaskContinuationOptions.OnlyOnFaulted);
        }
    }
}
