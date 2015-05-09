using System;
using System.Collections.Generic;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    /// <summary>
    /// The list of requested columns extracted from the LINQ query
    /// </summary>
    internal class ColumnProjectionResult
    {
        /// <summary>
        /// List of column names to retrieve from DynamoDb
        /// </summary>
        internal List<string> AttributesToGet;

        /// <summary>
        /// This functor implements entity conversion, if select new {} was specified
        /// </summary>
        internal Delegate ProjectionFunc;
    }
}
