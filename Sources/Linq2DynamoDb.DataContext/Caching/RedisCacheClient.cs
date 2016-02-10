using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Utils;
using StackExchange.Redis;

namespace Linq2DynamoDb.DataContext.Caching
{
	public class RedisCacheClient : ICacheClient
	{
		private static readonly object s_syncObject = new object();
		private static IConnectionMultiplexer s_singConnectionMultiplexer;

		private static readonly Lazy<IConnectionMultiplexer> s_connectionMultiplexer = new Lazy<IConnectionMultiplexer>(ConnectToCluster);

		private static string[] s_initialRegistrationCallChain;

		private static IConnectionMultiplexer Multiplexer
		{
			get { return s_connectionMultiplexer.Value; }
		}

		private static IConnectionMultiplexer ConnectToCluster()
		{
			lock (s_syncObject)
			{
				if (s_singConnectionMultiplexer != null)
					throw new ApplicationException(
						string.Format(
							"Already connected to cluster. Connection established, object created, been there, done that. Initial call made through call chain: {0}",
							string.Join(", ", s_initialRegistrationCallChain)));

				s_initialRegistrationCallChain = (new StackTrace()
					.GetFrames() ?? new StackFrame[] { })
					.Select(x => x.GetMethod())
					.Select(x => x.Name)
					.Reverse()
					.ToArray();

				return s_singConnectionMultiplexer = ConnectionMultiplexer.Connect(RedisServer);
			}
		}

		public static string RedisServer { get; set; }
		public TimeSpan DefaultTimeToLive { get; set; }

		public RedisCacheClient(string connectionString, TimeSpan? ttl)
		{
			RedisServer = connectionString;
			DefaultTimeToLive = ttl ?? TimeSpan.FromMinutes(15);
		}

		public bool Remove(string key)
		{
			IDatabase db = Multiplexer.GetDatabase();
			return db.KeyDelete(key);
		}
		public bool TryRemove(string key)
		{
			try
			{
				return Remove(key);
			}
			catch
			{
				return false;
			}
		}

		public bool TryGetValue<T>(string key, out T value)
		{
			value = default(T);

			IDatabase db = Multiplexer.GetDatabase();
			var returnValue = db.StringGet(key);

			if (!returnValue.HasValue)
				return false;

			Base64Serializer<T> serializer = new Base64Serializer<T>();
			value = serializer.Deserialize(returnValue);
			return true;
		}

		public bool AddValue<T>(string key, T value)
		{
			string existing;
			if (TryGetValue(key, out existing))
				throw new ArgumentException(string.Format("Key '{0}' already exists in database", key));

			return SetValue(key, value);
		}

		public bool AddValue<T>(string key, T value, TimeSpan? timeToLive)
		{
			string existing;
			if (TryGetValue(key, out existing))
				throw new ArgumentException(string.Format("Key '{0}' already exists in database", key));

			return SetValue(key, value, timeToLive);
		}

		public bool AddValue<T>(string key, T value, DateTime? expiration)
		{
			string existing;
			if (TryGetValue(key, out existing))
				throw new ArgumentException(string.Format("Key '{0}' already exists in database", key));

			return SetValue(key, value, expiration);
		}

		public bool SetValue<T>(string key, T value)
		{
			IDatabase db = Multiplexer.GetDatabase();
			Base64Serializer<T> serializer = new Base64Serializer<T>();
			string serialized = serializer.Serialize(value);
			return db.StringSet(key, serialized);
		}

		public bool SetValue<T>(string key, T value, TimeSpan? timeToLive)
		{
			IDatabase db = Multiplexer.GetDatabase();
			Base64Serializer<T> serializer = new Base64Serializer<T>();
			string serialized = serializer.Serialize(value);
			return db.StringSet(key, serialized, timeToLive);
		}

		public bool SetValue<T>(string key, T value, DateTime? expiration)
		{
			var timeToLive = expiration == null
				? null
				: expiration - DateTime.UtcNow;

			return SetValue(key, value, timeToLive);
		}
		public bool ReplaceValue<T>(string key, T value)
		{
			string existing;
			if (!TryGetValue(key, out existing))
				throw new KeyNotFoundException(string.Format("Key '{0}' not found in database", key));

			return SetValue(key, value);
		}
		public bool ReplaceValue<T>(string key, T value, TimeSpan? timeToLive)
		{
			string existing;
			if (!TryGetValue(key, out existing))
				throw new KeyNotFoundException(string.Format("Key '{0}' not found in database", key));

			return SetValue(key, value, timeToLive);
		}
		public bool ReplaceValue<T>(string key, T value, DateTime? expiration)
		{
			string existing;
			if (!TryGetValue(key, out existing))
				throw new KeyNotFoundException(string.Format("Key '{0}' not found in database", key));

			return SetValue(key, value, expiration);
		}
	}
}
