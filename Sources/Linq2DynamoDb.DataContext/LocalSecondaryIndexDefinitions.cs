using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Defines a set of Local Secondary Indexes to be created
    /// </summary>
    public class LocalSecondaryIndexDefinitions<TEntity> : List<Expression<Func<TEntity, object>>>
    {
        public LocalSecondaryIndexDefinitions(params Expression<Func<TEntity, object>>[] indexDefinitions)
        {
            this.AddRange(indexDefinitions);
        }
    }
}
