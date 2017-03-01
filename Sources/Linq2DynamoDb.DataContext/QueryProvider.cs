using System;
using System.Collections;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.ExpressionUtils;
using Linq2DynamoDb.DataContext.Utils;
using Amazon.DynamoDBv2.DocumentModel;
using Expression = System.Linq.Expressions.Expression;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Implementation of IQueryProvider. Also does some magic of pre- and post-evaluating LINQ expressions.
    /// </summary>
    public class QueryProvider : IQueryProvider
    {
        private readonly TableDefinitionWrapper _tableWrapper;

        /// <summary>
        /// Allows to specify custom FilterExpression for DynamoDb queries and scans
        /// </summary>
        internal Amazon.DynamoDBv2.DocumentModel.Expression CustomFilterExpression { get; set; }

        /// <summary>
        /// A callback for customizing QUERY operation params before executing the QUERY.
        /// </summary>
        internal Action<QueryOperationConfig> ConfigureQueryOperationCallback { get; set; }

        /// <summary>
        /// A callback for customizing SCAN operation params before executing the SCAN.
        /// </summary>
        internal Action<ScanOperationConfig> ConfigureScanOperationCallback { get; set; }

        // There should be one static instance of SubtreeEvaluationVisitor per thread, as it should be 
        // able to detect recursions
        static readonly ThreadLocal<SubtreeEvaluationVisitor> SubtreeEvaluator = new ThreadLocal<SubtreeEvaluationVisitor>(() => new SubtreeEvaluationVisitor());

        static readonly ScalarMethodsVisitor ScalarMethodsVisitor = new ScalarMethodsVisitor();

        internal QueryProvider(TableDefinitionWrapper tableWrapper)
        {
            this._tableWrapper = tableWrapper;
        }

        /// <summary>
        /// Evaluates LINQ expression and queries DynamoDb table for results
        /// </summary>
        internal object ExecuteQuery(Expression expression)
        {
            var visitor = this.PreEvaluateExpressionAndGetQueryableMethodsVisitor(ref expression);

            // executing get/query/scan operation against DynamoDb table
            var result = this._tableWrapper.LoadEntities(visitor.TranslationResult, visitor.EntityType);

            return this.PostEvaluateExpression(expression, visitor, result);
        }

        /// <summary>
        /// Evaluates LINQ expression and queries DynamoDb table for results
        /// </summary>
        internal async Task<object> ExecuteQueryAsync(Expression expression)
        {
            var visitor = this.PreEvaluateExpressionAndGetQueryableMethodsVisitor(ref expression);

            // executing get/query/scan operation against DynamoDb table
            var result = await this._tableWrapper.LoadEntitiesAsync(visitor.TranslationResult, visitor.EntityType);

            return this.PostEvaluateExpression(expression, visitor, result);
        }

        #region IQueryProvider implementation. Copied from IQToolkit

        IQueryable<TEntity> IQueryProvider.CreateQuery<TEntity>(Expression expression)
        {
            return new Query<TEntity>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = ReflectionUtils.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(Query<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /// <summary>
        /// This method is called by Queryable's SingleValue-methods (First(), Single(), Any() etc.)
        /// </summary>
        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            return (TResult)this.ExecuteQuery(expression);
        }

        object IQueryProvider.Execute(Expression expression)
        {
            return this.ExecuteQuery(expression);
        }

        #endregion

        private QueryableMethodsVisitor PreEvaluateExpressionAndGetQueryableMethodsVisitor(ref Expression expression)
        {
            // replacing Count(predicate) with Where(predicate).Count()
            expression = ScalarMethodsVisitor.Visit(expression);

            // pre-executing everything, that can be executed locally
            expression = SubtreeEvaluator.Value.EvaluateSubtree(expression);

            // traversing the expression to find out the type of entities to be returned
            var entityTypeExtractor = new EntityTypeExtractionVisitor();
            entityTypeExtractor.Visit(expression);

            if (entityTypeExtractor.TableEntityType == null)
            {
                throw new InvalidOperationException("Failed to extract the table entity type from the query");
            }

            var entityType = entityTypeExtractor.EntityType ?? entityTypeExtractor.TableEntityType;

            // translating the query into set of conditions and converting the expression at the same time
            // (all Queryable method calls will be replaced by a param of IQueryable<T> type)
            var visitor = new QueryableMethodsVisitor(entityType, entityTypeExtractor.TableEntityType);
            expression = visitor.Visit(expression);

            visitor.TranslationResult.CustomFilterExpression = this.CustomFilterExpression;
            visitor.TranslationResult.ConfigureQueryOperationCallback = this.ConfigureQueryOperationCallback;
            visitor.TranslationResult.ConfigureScanOperationCallback = this.ConfigureScanOperationCallback;

            return visitor;
        }

        private object PostEvaluateExpression(Expression expression, QueryableMethodsVisitor visitor, object result)
        {
            // trying to support other (mostly Enumerable's single-entity) operations 
            var enumerableResult = result as IEnumerable;
            if (enumerableResult != null)
            {
                var queryableResult = enumerableResult.AsQueryable();

                var lambda = Expression.Lambda(expression, visitor.EnumerableParameterExp);

                // Here the default methods for IEnumerable<T> are called.
                // This allows to support First(), Last(), Any() etc.
                try
                {
                    result = lambda.Compile().DynamicInvoke(queryableResult);
                }
                catch (TargetInvocationException ex)
                {
                    throw ex.InnerException ?? ex;
                }
            }
            return result;
        }
    }
}
