using System;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    internal class RedisCacheException : Exception
    {
        public RedisCacheException(string message, params object[] values) : base(string.Format(message, values)) { }
    }
}
