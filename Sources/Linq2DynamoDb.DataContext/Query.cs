using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// IQueryable implementation. This black magic is mostly copied from IQToolkit
    /// </summary>
    public class Query<TEntity> : IOrderedQueryable<TEntity>
    {
        protected readonly QueryProvider Provider;
        private readonly Expression _expression;

        public Query(QueryProvider provider)
        {
            this.Provider = provider;
            this._expression = Expression.Constant(this);
        }

        public Query(QueryProvider provider, Expression expression)
        {
            if (!typeof(IQueryable<TEntity>).GetTypeInfo().IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException("expression");
            }
            this.Provider = provider;
            this._expression = expression;
        }

        Expression IQueryable.Expression { get { return this._expression; } }

        Type IQueryable.ElementType { get { return typeof(TEntity); } }

        IQueryProvider IQueryable.Provider { get { return this.Provider; } }

        public IEnumerator<TEntity> GetEnumerator()
        {
            // note, that server is requeried every time a new enumerator is requested
            var enumerableResult = (IEnumerable<TEntity>)this.Provider.ExecuteQuery(this._expression);
            return enumerableResult.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            // note, that server is requeried every time a new enumerator is requested
            var enumerableResult = (IEnumerable)this.Provider.ExecuteQuery(this._expression);
            return enumerableResult.GetEnumerator();
        }
    }
}
