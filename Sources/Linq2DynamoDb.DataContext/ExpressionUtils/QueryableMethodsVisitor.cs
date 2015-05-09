using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    /// <summary>
    /// Collects query conditions and also modifies the expression to be ready for post-evaluation
    /// </summary>
    internal class QueryableMethodsVisitor : ExpressionVisitorBase
    {
        #region Results

        public readonly TranslationResult TranslationResult;
        public readonly ParameterExpression EnumerableParameterExp;

        #endregion

        private readonly Type _tableEntityType;

        public QueryableMethodsVisitor(Type entityType, Type tableEntityType)
        {
            this._tableEntityType = tableEntityType;
            this.EnumerableParameterExp = Expression.Parameter(typeof(IQueryable<>).MakeGenericType(entityType));

            this.TranslationResult = new TranslationResult(tableEntityType.Name);
        }

        protected void VisitWhereCall(Expression whereBodyExp)
        {
            var whereVisitor = new WhereVisitor(this._tableEntityType);

            whereVisitor.Visit(whereBodyExp);

            // converting conditions
            if
            (
                (whereVisitor.FieldNames.Count != whereVisitor.ScanOperators.Count)
                ||
                (whereVisitor.ScanOperators.Count != whereVisitor.FieldValues.Count)
            )
            {
                throw new InvalidOperationException("Failed to convert the query to a list of search conditions");
            }

            for (int i = 0; i < whereVisitor.FieldNames.Count; i++)
            {
                this.TranslationResult.Conditions.AddCondition
                (
                    whereVisitor.FieldNames[i],
                    new SearchCondition(whereVisitor.ScanOperators[i], whereVisitor.FieldValues[i])
                );
            }
        }

        protected void VisitProjectionCall(Expression projectionBodyExp)
        {
            var projectionResult = new ProjectionVisitor().ProjectColumns(projectionBodyExp);

            this.TranslationResult.ProjectionFunc = projectionResult.ProjectionFunc;
            this.TranslationResult.AttributesToGet = projectionResult.AttributesToGet;
        }

        protected void VisitOrderByCall(Expression orderByExp, bool orderByDesc)
        {
            var propertyExpression = (MemberExpression)orderByExp;

            this.TranslationResult.OrderByColumn = propertyExpression.Member.Name;
            this.TranslationResult.OrderByDesc = orderByDesc;
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExp)
        {
            // depth-first search
            var newObject = this.Visit(methodCallExp.Object);
            var newArgs = this.VisitMethodArgs(methodCallExp.Arguments);

            if (methodCallExp.Method.DeclaringType == typeof(Queryable))
            {
                // we replace methods, which are supported, by _enumerableParameterExp parameter
                switch (methodCallExp.Method.Name)
                {
                    case "Where":
                    {
                        var lambda = (LambdaExpression)StripQuotes(methodCallExp.Arguments[1]);
                        this.VisitWhereCall(lambda.Body);
                        return this.EnumerableParameterExp;
                    }
                    case "Select":
                    {
                        var lambda = (LambdaExpression) StripQuotes(methodCallExp.Arguments[1]);
                        if (lambda.Body.NodeType != ExpressionType.Parameter)
                        {
                            this.VisitProjectionCall(lambda.Body);
                        }
                        return this.EnumerableParameterExp;
                    }
                    case "OrderBy":
                    case "OrderByDescending":
                    {
                        var lambda = (LambdaExpression)StripQuotes(methodCallExp.Arguments[1]);
                        this.VisitOrderByCall(lambda.Body, methodCallExp.Method.Name == "OrderByDescending");
                        return this.EnumerableParameterExp;
                    }
                }
            }

            // recreating the call, if params were changed
            if (newObject != methodCallExp.Object || newArgs != methodCallExp.Arguments)
            {
                return Expression.Call(newObject, methodCallExp.Method, newArgs);
            }
            return methodCallExp;
        }

        protected virtual ReadOnlyCollection<Expression> VisitMethodArgs(ReadOnlyCollection<Expression> original)
        {
            List<Expression> list = null;
            for (int i = 0, n = original.Count; i < n; i++)
            {
                var p = this.Visit(original[i]);
                if (list != null)
                {
                    list.Add(p);
                }
                else if (p != original[i])
                {
                    list = new List<Expression>(n);
                    for (int j = 0; j < i; j++)
                    {
                        list.Add(original[j]);
                    }
                    list.Add(p);
                }
            }
            return list != null ? list.AsReadOnly() : original;
        }

        protected override Expression VisitConstant(ConstantExpression constantExp)
        {
            if ((constantExp.Value as IQueryable) != null)
            {
                return this.EnumerableParameterExp;
            }
            return constantExp;
        }
    }
}
