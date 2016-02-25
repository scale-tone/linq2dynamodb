using System;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    /// <summary>
    /// A lock object. Should be used as follows: using(cache.AcquireTableLock("some lock key")){...}
    /// </summary>
    internal class TableLock : IDisposable
    {
        private readonly RedisTableCache _cache;
        private readonly string _lockKey;
        private bool _disposed;

        internal TableLock(RedisTableCache repository, string lockKey, TimeSpan lockTimeout)
        {
            this._cache = repository;
            this._lockKey = lockKey;
            this._cache.LockTable(lockKey, lockTimeout);
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                this._cache.UnlockTable(this._lockKey);
            }
            this._disposed = true;
        }
    }
}
