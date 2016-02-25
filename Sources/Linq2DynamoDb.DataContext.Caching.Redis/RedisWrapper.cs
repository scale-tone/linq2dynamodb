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
        public const int RetryCount = 3;

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
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    var values = redis.StringGet(keys);

                    // returning iterator only if all entities succeeded to be loaded
                    if ((values == null) || values.Any(v => v.IsNull))
                    {
                        throw new RedisCacheException("The following keys not found in cache: " + keys.Aggregate(string.Empty, (s, k) => s + k + ","));
                    }

                    return values.Select(v => v.ToObject<T>());
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
        }

        public void SetWithRetries<T>(RedisKey key, T value, When when = When.Always)
        {
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    if (redis.StringSet(key, value.ToRedisValue(), this._ttl, when))
                    {
                        return;
                    }
                    throw new RedisCacheException("Setting value for key {0} failed", key.ToString());
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
        }

        public void RemoveWithRetries(params RedisKey[] keys)
        {
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    redis.KeyDelete(keys);
                    return;
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
        }

        public void SetHashWithRetries(RedisKey hashKey, RedisValue fieldName, RedisValue fieldValue, bool clearHashFirst = false)
        {
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    if (clearHashFirst)
                    {
                        redis.KeyDelete(hashKey);
                    }
                    redis.HashSet(hashKey, fieldName, fieldValue);
                    return;
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
        }

        public void RemoveHashFieldsWithRetries(RedisKey hashKey, params RedisValue[] fieldNames)
        {
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    redis.HashDelete(hashKey, fieldNames);
                    return;
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
        }

        public long GetHashLengthWithRetries(RedisKey hashKey)
        {
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    return redis.HashLength(hashKey);
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
        }

        public HashEntry[] GetHashFieldsWithRetries(RedisKey hashKey)
        {
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    return redis.HashGetAll(hashKey);
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
        }

        public bool HashFieldExistsWithRetries(RedisKey hashKey, RedisValue fieldName)
        {
            var redis = this.GetDatabase();
            Exception exception = null;
            for (int i = 0; i < RetryCount; i++)
            {
                try
                {
                    return redis.HashExists(hashKey, fieldName);
                }
                catch (TimeoutException ex)
                {
                    exception = ex;
                }
            }
            throw exception ?? new RedisCacheException("This should never happen");
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

        private readonly ConnectionMultiplexer _redisConn;
        private readonly int _dbIndex;
        private readonly RedisKey _keyPrefix;
        private readonly TimeSpan _ttl;
        private readonly Action<string> _onLog;

        private IDatabase GetDatabase()
        {
            return this._redisConn.GetDatabase(this._dbIndex).WithKeyPrefix(this._keyPrefix);
        }

        #endregion
    }
}
