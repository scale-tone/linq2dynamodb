using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Expression = System.Linq.Expressions.Expression;

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

        internal async Task<List<TEntity>> ToListAsync()
        {
            var enumerableResult = await this.Provider.ExecuteQueryAsync(this._expression);
            return ((IEnumerable<TEntity>)enumerableResult).ToList();
        }

        internal void SetCustomFilterExpression(Amazon.DynamoDBv2.DocumentModel.Expression expression)
        {
            this.Provider.CustomFilterExpression = expression;
        }

        internal void SetConfigureQueryOperationCallback(Action<QueryOperationConfig> callback)
        {
            this.Provider.ConfigureQueryOperationCallback = callback;
        }

        internal void SetConfigureScanOperationCallback(Action<ScanOperationConfig> callback)
        {
            this.Provider.ConfigureScanOperationCallback = callback;
        }
    }
}
