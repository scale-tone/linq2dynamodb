using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{
    internal static class GeneralUtils
    {
        private const char KeySeparator = '|';

        public static RedisKey ToRedisKey(this EntityKey key)
        {
            string result = key.HashKey.ToString().ToBase64();
            if (key.RangeKey != null)
            {
                result += KeySeparator + key.RangeKey.ToString().ToBase64();
            }
            return result;
        }

        public static RedisValue ToRedisValue(this EntityKey key)
        {
            string result = key.HashKey.ToString().ToBase64();
            if (key.RangeKey != null)
            {
                result += KeySeparator + key.RangeKey.ToString().ToBase64();
            }
            return result;
        }

        public static EntityKey ToEntityKey(this RedisValue value)
        {
            if (value == RedisValue.Null)
            {
                return null;
            }

            var parts = value.ToString().Split(KeySeparator);

            Primitive hashKey = parts[0].FromBase64();
            Primitive rangeKey = null;
            if ((parts.Length > 1) && (!string.IsNullOrEmpty(parts[1])))
            {
                rangeKey = parts[1].FromBase64();
            }

            return new EntityKey(hashKey, rangeKey);
        }

        public static RedisValue ToRedisValue<T>(this T obj)
        {
            using (var memoryStream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(memoryStream, obj);
                return memoryStream.ToArray();
            }
        }

        public static T ToObject<T>(this RedisValue value)
        {
            if (value == RedisValue.Null)
            {
                return default(T);
            }
            using (var memoryStream = new MemoryStream(value))
            {
                return (T)new BinaryFormatter().Deserialize(memoryStream);
            }
        }
    }
}
