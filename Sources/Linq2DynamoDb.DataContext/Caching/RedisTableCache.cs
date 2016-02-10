using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Linq2DynamoDb.DataContext.Caching
{
	public class RedisTableCache : TableCache
	{
		public RedisTableCache(RedisCacheClient client)
			: base(client)
		{
		}
	}
}
