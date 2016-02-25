using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    public partial class RedisTableCache
    {
        /// <summary>
        /// A lock object. Should be used as follows: using(cache.AcquireTableLock("some lock key")){...}
        /// </summary>
        private class TableLock : IDisposable
        {
            private readonly RedisTableCache _parent;
            private readonly string _lockKey;
            private bool _disposed;

            internal TableLock(RedisTableCache parent, string lockKey, TimeSpan lockTimeout)
            {
                this._parent = parent;
                this._lockKey = lockKey;
                this.LockTable(lockTimeout);
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    this.UnlockTable();
                }
                this._disposed = true;
            }

            /// <summary>
            /// Acquires a named lock around the table by storing a random value in cache
            /// </summary>
            private void LockTable(TimeSpan lockTimeout)
            {
                if (this._lockIds.ContainsKey(this._lockKey))
                {
                    throw new NotSupportedException("Recursive locks are not supported. Or maybe you're trying to use EnyimTableCache object from multiple threads?");
                }

                string cacheLockKey = this._parent.GetLockKeyInCache(this._lockKey);
                int cacheLockId = Rnd.Next();

                var timeStart = DateTime.Now;
                while (true)
                {
                    if (DateTime.Now - timeStart > lockTimeout)
                    {
                        break;
                    }

                    try
                    {
                        // Trying to create a new value in cache
                        this._parent._redis.SetWithRetries(cacheLockKey, cacheLockId, When.NotExists);
                        this._lockIds[this._lockKey] = cacheLockId;
                        return;
                    }
                    catch (Exception)
                    {
                        Thread.Sleep(10);
                    }
                }

                // If we failed to acquire a lock within CacheLockTimeoutInSeconds 
                // (this means, that another process crached), then we should forcibly acquire it

                this._parent.Log("Forcibly acquiring the table lock object {0} after {1} ms of waiting", this._lockKey, lockTimeout.TotalMilliseconds);

                this._parent._redis.SetWithRetries(cacheLockKey, cacheLockId);
                this._lockIds[this._lockKey] = cacheLockId;
            }

            /// <summary>
            /// Releases a named lock around the table
            /// </summary>
            private void UnlockTable()
            {
                int lockId;
                if (!this._lockIds.TryRemove(this._lockKey, out lockId))
                {
                    throw new InvalidOperationException(string.Format("The table lock {0} wasn't acquired, so it cannot be released. Check your code!", this._lockKey));
                }

                string cacheLockKey = this._parent.GetLockKeyInCache(this._lockKey);
                int cacheLockId;
                try
                {
                    cacheLockId = this._parent._redis.GetWithRetries<int>(cacheLockKey).Single();
                }
                catch (Exception)
                {
                    // The cache miss might happen here, if a cache server crashed.
                    // In this case we just silently return.
                    this._parent.Log("The table lock object {0} is missing in cache, but we don't care about that too much (probably, the cache node was restarted)", this._lockKey);
                    return;
                }

                if (cacheLockId != lockId)
                {
                    // This means, that another process has forcibly replaced our lock.
                    throw new InvalidOperationException(string.Format("The table lock {0} was forcibly acquired by another process", this._lockKey));
                }

                this._parent._redis.RemoveWithRetries(cacheLockKey);
            }

            /// <summary>
            /// Here all lock keys and their IDs are stored, for debugging purposes
            /// </summary>
            private readonly ConcurrentDictionary<string, int> _lockIds = new ConcurrentDictionary<string, int>();
        }
    }
}
