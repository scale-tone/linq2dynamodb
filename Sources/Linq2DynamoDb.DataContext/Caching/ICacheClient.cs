using System;

namespace Linq2DynamoDb.DataContext.Caching
{
	public interface ICacheClient
	{
		TimeSpan DefaultTimeToLive { get; set; }
		//TValue this[string key] { get; set; }

		//TValue this[string key, DateTime expiration] { set; }
		//TValue this[string key, TimeSpan? timeToLive] { set; }

		bool Remove(string key);
		bool TryRemove(string key);

		bool TryGetValue<TValue>(string key, out TValue value);
		//bool TryGetValue<TValue>(string key, out TValue value, out TimeSpan? timeToLive);
		//bool TryGetValue<TValue>(string key, out TValue value, out DateTime? expiration);
		//bool TryGetTimeToLive(string key, out TimeSpan? timetoLive);

		bool AddValue<TValue>(string key, TValue value);
		bool AddValue<TValue>(string key, TValue value, TimeSpan? timeToLive);
		bool AddValue<TValue>(string key, TValue value, DateTime? expiration);
		bool SetValue<TValue>(string key, TValue value);
		bool SetValue<TValue>(string key, TValue value, TimeSpan? timeToLive);
		bool SetValue<TValue>(string key, TValue value, DateTime? expiration);

		bool ReplaceValue<TValue>(string key, TValue value);
		bool ReplaceValue<TValue>(string key, TValue value, TimeSpan? timeToLive);
		bool ReplaceValue<TValue>(string key, TValue value, DateTime? expiration);

		//bool SetTimeToLive(string key, TimeSpan? timetoLive);
	}

	//public interface ICacheClient : ICacheClient<string, string>
	//{
	//}
}
