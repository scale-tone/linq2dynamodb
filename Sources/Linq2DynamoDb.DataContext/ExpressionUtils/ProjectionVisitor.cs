using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;
using Expression = System.Linq.Expressions.Expression;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    /// <summary>
    /// Parses a projection expression and gets a list of columns to get from DynamoDb.
    /// Also constructs a projection lambda.
    /// </summary>
    internal class ProjectionVisitor : ExpressionVisitorBase
    {
        private readonly Type _tableEntityType;

        /// <summary>
        /// This expression represents the Document, to which the projection function should be applied
        /// </summary>
        private static readonly ParameterExpression TableEntityExpression = Expression.Parameter(typeof(Document));

        /// <summary>
        /// MethodInfo of the method, that will be used for getting column values from underlying Document
        /// </summary>
        private static readonly MethodInfo GetColumnValueByNameMethodInfo = ((Func<Document, string, Type, Type, object>)GetColumnValueByName).GetMethodInfo();

        /// <summary>
        /// The list of columns to get from DynamoDb
        /// </summary>
        private List<string> _attributesToGet;

        public ProjectionVisitor(Type tableEntityType)
        {
            this._tableEntityType = tableEntityType;
        }

        internal ColumnProjectionResult ProjectColumns(Expression expression)
        {
            this._attributesToGet = new List<string>();

            var projectionExpression = this.Visit(expression);

            return new ColumnProjectionResult
            {
                AttributesToGet = this._attributesToGet,
                ProjectionFunc = Expression.Lambda(projectionExpression, TableEntityExpression).Compile()
            };
        }

        protected override Expression VisitMember(MemberExpression memberExp)
        {
            if (memberExp.Expression != null && memberExp.Expression.NodeType == ExpressionType.Parameter)
            {
                this._attributesToGet.Add(memberExp.Member.Name);

                // this is an expression of getting a property value by it's name and type
                var projectionExpression = Expression.Call
                (
                    GetColumnValueByNameMethodInfo,
                    TableEntityExpression,
                    Expression.Constant(memberExp.Member.Name),
                    Expression.Constant(memberExp.Type),
                    Expression.Constant(this._tableEntityType)
                );

                return Expression.Convert(projectionExpression, memberExp.Type);
            }

            return base.VisitMember(memberExp);
        }

        protected override Expression VisitParameter(ParameterExpression parameterExp)
        {
            if (parameterExp.NodeType == ExpressionType.Parameter)
            {
                // We get here when processing $select in Linq2DynamoDb.WebApi.OData. 
                // For some reason, the resulting expression also checks the entity for null.
                return TableEntityExpression;
            }

            return base.VisitParameter(parameterExp);
        }

        /// <summary>
        /// A utility for getting Document field's value. Used within conversion expression
        /// </summary>
        private static object GetColumnValueByName(Document doc, string name, Type type, Type entityType)
        {
            DynamoDBEntry entry;
            doc.TryGetValue(name, out entry);

            // we also support AWS SDK convertors
            var converter = DynamoDbConversionUtils.DynamoDbPropertyConverter(entityType, name);
            return converter == null ? entry.ToObject(type) : converter.FromEntry(entry);
        }
    }
}
