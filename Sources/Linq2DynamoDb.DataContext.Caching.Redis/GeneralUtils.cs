using System.IO;
#if NETSTANDARD1_6
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
#else
using System.Runtime.Serialization.Formatters.Binary;
#endif
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching.Redis
{

    public static class GeneralUtils
    {
        private const char KeySeparator = '|';

#if NETSTANDARD1_6
        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings
        {
            DateParseHandling = DateParseHandling.None,
            TypeNameHandling = TypeNameHandling.All,
            ContractResolver = new DefaultContractResolver { IgnoreSerializableInterface = true }
        };
#endif

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
#if NETSTANDARD1_6
            // using AWSSDK's JSON serializer for Documents and Newtonsoft.Json for everything else.
            // Newtonsoft.Json won't work for Documents anyway.
            var docWrapper = obj as CacheDocumentWrapper;
            if (docWrapper != null)
            {
                return docWrapper.Document.ToJson();
            }
            return JsonConvert.SerializeObject(obj, SerializerSettings);
#else
            // keep using BinaryFormatter, for not to break compatibility
            using (var memoryStream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(memoryStream, obj);
                return memoryStream.ToArray();
            }
#endif
        }

        public static T ToObject<T>(this RedisValue value)
        {
            if (value == RedisValue.Null)
            {
                return default(T);
            }

#if NETSTANDARD1_6
            // using AWSSDK's JSON serializer for Documents and Newtonsoft.Json for everything else.
            // Newtonsoft.Json won't work for Documents anyway.
            if (typeof(T) == typeof(CacheDocumentWrapper))
            {
                var doc = Document.FromJson(value);
                return (T)(object)new CacheDocumentWrapper(doc);
            }

            return JsonConvert.DeserializeObject<T>(value, SerializerSettings);
#else
            // keep using BinaryFormatter, for not to break compatibility
            using (var memoryStream = new MemoryStream(value))
            {
                return (T)new BinaryFormatter().Deserialize(memoryStream);
            }
#endif
        }
    }
}
