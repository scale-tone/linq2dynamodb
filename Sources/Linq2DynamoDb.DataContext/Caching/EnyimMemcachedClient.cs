using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Enyim.Caching;
using Enyim.Caching.Memcached;

namespace Linq2DynamoDb.DataContext.Caching
{
	public class EnyimMemcachedClient : ICacheClient
	{
		MemcachedClient _cacheClient;
		public TimeSpan DefaultTimeToLive { get; set; }

		public EnyimMemcachedClient(MemcachedClient client, TimeSpan? defaultTtl = null)
		{
			_cacheClient = client;
			DefaultTimeToLive = defaultTtl ?? TimeSpan.FromMinutes(15);
		}

		//public string this[string key]
		//{
		//	get
		//	{
		//		throw new NotImplementedException();
		//	}
		//	set
		//	{
		//		throw new NotImplementedException();
		//	}
		//}

		//public string this[string key, DateTime expiration]
		//{
		//	set { throw new NotImplementedException(); }
		//}

		//public string this[string key, TimeSpan? timeToLive]
		//{
		//	set { throw new NotImplementedException(); }
		//}

		public bool Remove(string key)
		{
			return _cacheClient.Remove(key);
		}

		public bool TryRemove(string key)
		{
			var removeResult = this._cacheClient.ExecuteRemove(key);
			return (removeResult.InnerResult == null) ||
				   (removeResult.InnerResult.Exception == null);
		}

		public bool TryGetValue<T>(string key, out T value)
		{
			value = default(T);
			var result = _cacheClient.ExecuteGet<T>(key);
			if (result.Success)
				value = result.Value;
			
			return result.Success;
		}

		//public bool TryGetValue<T>(string key, out T value, out TimeSpan? timeToLive)
		//{
		//	throw new NotImplementedException();
		//}

		//public bool TryGetValue<T>(string key, out T value, out DateTime? expiration)
		//{
		//	throw new NotImplementedException();
		//}

		//public bool TryGetTimeToLive(string key, out TimeSpan? timetoLive)
		//{
		//	throw new NotImplementedException();
		//}

		public bool AddValue<T>(string key, T value)
		{
			return _cacheClient.Store(StoreMode.Add, key, value, DefaultTimeToLive);
		}

		public bool AddValue<T>(string key, T value, TimeSpan? timeToLive)
		{
			return _cacheClient.Store(StoreMode.Add, key, value, timeToLive ?? DefaultTimeToLive);
		}

		public bool AddValue<T>(string key, T value, DateTime? expiration)
		{
			return StoreWithExpiration(StoreMode.Add, key, value, expiration);
		}

		public bool SetValue<T>(string key, T value)
		{
			return _cacheClient.Store(StoreMode.Set, key, value, DefaultTimeToLive);
		}

		public bool SetValue<T>(string key, T value, TimeSpan? timeToLive)
		{
			return _cacheClient.Store(StoreMode.Set, key, value, timeToLive ?? DefaultTimeToLive);
		}

		public bool SetValue<T>(string key, T value, DateTime? expiration)
		{
			return StoreWithExpiration(StoreMode.Set, key, value, expiration);
		}

		public bool ReplaceValue<T>(string key, T value)
		{
			return _cacheClient.Store(StoreMode.Replace, key, value, DefaultTimeToLive);
		}

		public bool ReplaceValue<T>(string key, T value, TimeSpan? timeToLive)
		{
			return _cacheClient.Store(StoreMode.Replace, key, value, timeToLive ?? DefaultTimeToLive);
		}

		public bool ReplaceValue<T>(string key, T value, DateTime? expiration)
		{
			return StoreWithExpiration(StoreMode.Replace, key, value, expiration);
		}

		private bool StoreWithExpiration<T>(StoreMode mode, string key, T value, DateTime? expiration)
		{
			if (expiration.HasValue)
			{
				if (expiration.Value.Kind != DateTimeKind.Utc)
					expiration = expiration.Value.ToUniversalTime();
			}
			else
			{
				expiration = DateTime.UtcNow + DefaultTimeToLive;
			}
			return _cacheClient.Store(mode, key, value, expiration.Value);
		}

		//public bool SetTimeToLive(string key, TimeSpan? timetoLive)
		//{
		//	object value;
		//	if (!TryGetValue<object>(key, out value))
		//		return false;

		//	return ReplaceValue<object>(key, value, )
		//}
	}
}
