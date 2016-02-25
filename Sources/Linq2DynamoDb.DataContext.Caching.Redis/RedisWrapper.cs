using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;
using StackExchange.Redis.KeyspaceIsolation;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    /// <summary>
    /// Implements basic operations with Redis IDatabase.
    /// Provides retries logic, as it is a recommended approach to tolerate temporary network errors.
    /// </summary>
    internal class RedisWrapper
    {
        public RedisWrapper(ConnectionMultiplexer redisConn, int dbIndex, RedisKey keyPrefix, TimeSpan ttl, Action<string> onLog)
        {
            this._redisConn = redisConn;
            this._dbIndex = dbIndex;
            this._keyPrefix = keyPrefix;
            this._ttl = ttl;
            this._onLog = onLog;
        }

        public IEnumerable<T> GetWithRetries<T>(params RedisKey[] keys)
        {
            return this.DoWithRetries(redis =>
            {
                var values = redis.StringGet(keys);

                // returning iterator only if all entities succeeded to be loaded
                if ((values == null) || values.Any(v => v.IsNull))
                {
                    throw new RedisCacheException("The following keys not found in cache: " + keys.Aggregate(string.Empty, (s, k) => s + (string)k + ","));
                }

                return values.Select(v => v.ToObject<T>());
            });
        }

        public void SetWithRetries<T>(RedisKey key, T value, When when = When.Always)
        {
            this.DoWithRetries(redis =>
            {
                if (!redis.StringSet(key, value.ToRedisValue(), this._ttl, when))
                {
                    throw new RedisCacheException("Setting value for key {0} failed", key.ToString());
                }
            });
        }

        public void RemoveWithRetries(params RedisKey[] keys)
        {
            this.DoWithRetries(redis => redis.KeyDelete(keys));
        }

        public void SetHashWithRetries(RedisKey hashKey, RedisValue fieldName, RedisValue fieldValue)
        {
            this.DoWithRetries(redis => redis.HashSet(hashKey, fieldName, fieldValue));
        }

        public void CreateNewHashWithRetries(RedisKey hashKey, RedisValue fieldName, RedisValue fieldValue)
        {
            this.DoWithRetries(redis =>
            {
                redis.KeyDelete(hashKey);
                redis.HashSet(hashKey, fieldName, fieldValue);
                redis.KeyExpire(hashKey, this._ttl);
            });
        }

        public void RemoveHashFieldsWithRetries(RedisKey hashKey, params RedisValue[] fieldNames)
        {
            this.DoWithRetries(redis => redis.HashDelete(hashKey, fieldNames));
        }

        public long GetHashLengthWithRetries(RedisKey hashKey)
        {
            return this.DoWithRetries(redis => redis.HashLength(hashKey));
        }

        public HashEntry[] GetHashFieldsWithRetries(RedisKey hashKey)
        {
            return this.DoWithRetries(redis => redis.HashGetAll(hashKey));
        }

        public bool HashFieldExistsWithRetries(RedisKey hashKey, RedisValue fieldName)
        {
            return this.DoWithRetries(redis => redis.HashExists(hashKey, fieldName));
        }

        public RedisTransactionWrapper BeginTransaction(params Condition[] conditions)
        {
            var redis = this.GetDatabase();
            var trans = redis.CreateTransaction();

            foreach (var condition in conditions)
            {
                trans.AddCondition(condition);
            }

            return new RedisTransactionWrapper(trans, this._ttl, this._onLog);
        }

        #region Private Members

        private const int RetryCount = 3;

        private readonly ConnectionMultiplexer _redisConn;
        private readonly int _dbIndex;
        private readonly RedisKey _keyPrefix;
        private readonly TimeSpan _ttl;
        private readonly Action<string> _onLog;

        private T DoWithRetries<T>(Func<IDatabase, T> todo)
        {
            var redis = this._redisConn.GetDatabase(this._dbIndex).WithKeyPrefix(this._keyPrefix);
            Exception exception = new RedisCacheException("Unknown error");
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    return todo(redis);
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception;
        }

        private void DoWithRetries(Action<IDatabase> todo)
        {
            var redis = this._redisConn.GetDatabase(this._dbIndex).WithKeyPrefix(this._keyPrefix);
            Exception exception = new RedisCacheException("Unknown error");
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    todo(redis);
                    return;
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception;
        }

        private IDatabase GetDatabase()
        {
            return this._redisConn.GetDatabase(this._dbIndex).WithKeyPrefix(this._keyPrefix);
        }

        #endregion
    }
}
