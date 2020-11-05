![LINQ2DynamoDb](https://raw.githubusercontent.com/scale-tone/linq2dynamodb/master/logo-with-text.png)

A type-safe data context for AWS DynamoDB with LINQ and in-memory caching support. Allows to combine DynamoDB's durability with cache speed and read consistency

[Nuget:](http://www.nuget.org/packages/Linq2DynamoDb.DataContext)
```
PM> Install-Package Linq2DynamoDb.DataContext
PM> Install-Package Linq2DynamoDb.DataContext.Caching.MemcacheD
PM> Install-Package Linq2DynamoDb.DataContext.Caching.Redis
PM> Install-Package Linq2DynamoDb.AspNet.DataSource
```

<img alt="Nuget" src="https://img.shields.io/nuget/dt/Linq2DynamoDb.DataContext?label=Linq2DynamoDb.DataContext%20downloads">

Please, also take a look at the series of posts ([one](https://scale-tone.github.io/2016/03/13/dynamodb-elasticache-linq2dynamodb-odata-theory), [two](https://scale-tone.github.io/2016/03/17/dynamodb-elasticache-linq2dynamodb-odata-practice), [three](https://scale-tone.github.io/2016/03/19/dynamodb-elasticache-linq2dynamodb-ionic-practice), [four](https://scale-tone.github.io/2017/04/13/dynamodb-webapi-odata-practice)).

AWS DynamoDB is a cool, highly-available and highly-durable NoSQL database. Yet, because of its throughput capacity restrictions, it might get:
* unpredictably slow,
* unpredictably expensive.
AWS SDK for .Net (via its Amazon.DynamoDB.DataModel namespace) provides a cool type-safe way to store and retrieve .Net classes from/to DynamoDB. Yet:
* it's still not very .Net-friendly and doesn't fit well with some other common data technologies on .Net platform like LINQ and data binding,
* its objects cannot be directly cached in e.g. ElastiCache, because they're not serializable.

LINQ2DynamoDB tries to address all of those concerns. 

[Linq2DynamoDb.DataContext](https://github.com/scale-tone/linq2dynamodb/blob/master/Sources/Linq2DynamoDb.DataContext/DataContext.cs) translates LINQ queries into corresponding DynamoDB Get/Query/Scan operations (trying to choose the most effective one) and stores query results in an in-memory cache (currently [MemcacheD](https://github.com/scale-tone/linq2dynamodb/tree/master/Sources/Linq2DynamoDb.DataContext.Caching.MemcacheD) and [Redis](https://github.com/scale-tone/linq2dynamodb/tree/master/Sources/Linq2DynamoDb.DataContext.Caching.Redis) are supported). When data is modified, it's saved both to DynamoDB and to cache. This mitigates another issue of DynamoDB: inconsistent reads (Get/Query operations *can* be consistent for a double price, Scan operations cannot). If Linq2DynamoDb.DataContext succeeds to load the data from cache, that data will for sure be of the latest version.

A very common scenario for using DynamoDB is storing some kind of user profiles or other user-specific data in one big table with HashKey set to some UserID. Linq2DynamoDb.DataContext gracefully supports this case by allowing you to specify a predefined HashKey value for the entities. Then your DataContext instance represents (and correctly caches) a set of entities for a specific user (or whatever).

And now Linq2DynamoDb.DataContext [also supports OData](https://scale-tone.github.io/2017/04/13/dynamodb-webapi-odata-practice)!

Currently, Linq2DynamoDb.DataContext is itself based on AWS SDK's Amazon.DynamoDBv2.DocumentModel namespace and uses Document's change-tracking mechanism. We'll try to slowly move away from it in future.

There're, of course, still many other things to do. They're now recorded as issues, so, please, vote for them (and add your own proposed tasks/features).

Bugs? Suggestions? Complaints? We'd love to hear them!
Would like to contribute? You're welcome!
Any other help? Greatly appreciated!

