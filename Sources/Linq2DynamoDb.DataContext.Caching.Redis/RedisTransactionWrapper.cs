using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        public void Set(RedisKey key, RedisValue value)
        {
            this.WrapTaskWithLogging(this._redisTransaction.StringSetAsync(key, value));
        }

        public void Remove(RedisKey key)
        {
            this.WrapTaskWithLogging(this._redisTransaction.KeyDeleteAsync(key));
        }

        public void HashSet(RedisKey hashKey, RedisValue fieldName, RedisValue fieldValue)
        {
            this.WrapTaskWithLogging(this._redisTransaction.HashSetAsync(hashKey, fieldName, fieldValue));
        }

        public void HashRemove(RedisKey hashKey, RedisValue fieldName)
        {
            this.WrapTaskWithLogging(this._redisTransaction.HashDeleteAsync(hashKey, fieldName));
        }

        public void HashIncrement(RedisKey hashKey, RedisValue fieldName)
        {
            this.WrapTaskWithLogging(this._redisTransaction.HashIncrementAsync(hashKey, fieldName));
        }

        public void AddHashFieldExistsCondition(RedisKey hashKey, RedisValue fieldName)
        {
            this._redisTransaction.AddCondition(Condition.HashExists(hashKey, fieldName));
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
