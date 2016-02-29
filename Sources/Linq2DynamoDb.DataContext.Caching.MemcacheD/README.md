# Linq2DynamoDb.DataContext.Caching.MemcacheD
Implements caching in MemcacheD.

[Nuget:](http://www.nuget.org/packages/Linq2DynamoDb.DataContext.Caching.MemcacheD)
```
PM> Install-Package Linq2DynamoDb.DataContext.Caching.MemcacheD
```

Each entity is stored as a separate cache value.
Each index is also stored as a separate cache value.
Each projection index is stored as a separate cache value and contains projections inside.