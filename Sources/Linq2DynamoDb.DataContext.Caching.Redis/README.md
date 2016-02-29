# Linq2DynamoDb.DataContext.Caching.Redis
Implements caching in Redis.

[Nuget:](http://www.nuget.org/packages/Linq2DynamoDb.DataContext.Caching.Redis)
```
PM> Install-Package Linq2DynamoDb.DataContext.Caching.Redis
```

Indexes are stored as Redis Hashes. 
Full index contains entity keys as fields, the entities themselves are stored as Redis Strings.
Projection (readonly) index contains entities as field values.

The list of all indexes is also stored as a Redis Hash. There's a limit to the total number of indexes. When the limit is reached, old indexes are dropped.

Each Redis key has a prefix like "{MyTableName}". So that in Redis Cluster scenario, all keys for the same table fall to the same Redis shard. This is important, because RedisTableCache uses Redis transactions, and Redis transactions cannot span across multiple shards.
You can override GetCacheKeyPrefix() method to change this behaviour.