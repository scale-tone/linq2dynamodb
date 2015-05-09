using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Used in an Expression, that defines a Global Secondary Index to be created
    /// </summary>
    public class GlobalSecondaryIndexDefinition
    {
        public object HashKeyField { get; set; }
        public object RangeKeyField { get; set; }
        public long ReadCapacityUnits { get; set; }
        public long WriteCapacityUnits { get; set; }
    }

    /// <summary>
    /// Defines a set of Global Secondary Indexes to be created
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    public class GlobalSecondaryIndexDefinitions<TEntity> : List<Expression<Func<TEntity, GlobalSecondaryIndexDefinition>>>
    {
        public GlobalSecondaryIndexDefinitions(params Expression<Func<TEntity, GlobalSecondaryIndexDefinition>>[] globalIndexDefinitions)
        {
            this.AddRange(globalIndexDefinitions);
        }
    }
}
