using System;
using System.Linq;
using System.Linq.Expressions;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    /// <summary>
    /// Extracts the entity type from expression
    /// </summary>
    internal class EntityTypeExtractionVisitor : ExpressionVisitorBase
    {
        public Type EntityType;
        public Type TableEntityType;

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExp)
        {
            this.Visit(methodCallExp.Object);
            foreach (var arg in methodCallExp.Arguments)
            {
                this.Visit(arg);
            }

            if 
            (
                (methodCallExp.Method.DeclaringType == typeof (Queryable))
                &&
                (methodCallExp.Method.Name == "Select")
            )
            {
                // extracting entity type from projection lambda
                var lambda = (LambdaExpression)StripQuotes(methodCallExp.Arguments[1]);
                this.EntityType = lambda.ReturnType;
            }
            return methodCallExp;
        }

        protected override Expression VisitConstant(ConstantExpression constantExp)
        {
            var iQueryable = constantExp.Value as IQueryable;
            if (iQueryable != null)
            {
                // extracting table entity type from each IQueryable found in expression
                this.TableEntityType = iQueryable.ElementType;
            }
            return constantExp;
        }
    }
}
