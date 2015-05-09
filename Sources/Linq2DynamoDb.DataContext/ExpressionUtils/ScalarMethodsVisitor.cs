using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    /// <summary> 
    /// Replaces calls like Count(predicate) with calls like Where(predicate).Count()
    /// </summary> 
    internal class ScalarMethodsVisitor : ExpressionVisitorBase
    {
        protected override Expression VisitMethodCall(MethodCallExpression methodCallExp)
        {
            if 
            (
                (methodCallExp.Method.DeclaringType == typeof (Queryable))
                &&
                (methodCallExp.Arguments.Count == 2) // all these methods have 2 parameters
            )
            {
                var tableType = methodCallExp.Arguments[0].Type;

                // we only support queries around DataTable<TEntity>
                if (tableType.GetGenericTypeDefinition() == typeof (DataTable<>))
                {
                    var entityType = tableType.GetGenericArguments()[0];

                    string methodName = methodCallExp.Method.Name;
                    switch (methodName)
                    {
                        case "Count":
                        case "Any":
                        case "Single":
                        case "SingleOrDefault":
                        case "First":
                        case "FirstOrDefault":
                        case "Last":
                        case "LastOrDefault":
                        {
                            return Expression.Call
                            (
                                GetScalarPredicateMethodInfoFunctor(methodName, entityType),
                                Expression.Call
                                (
                                    GetWhehreMethodInfoFunctor(entityType),
                                    methodCallExp.Arguments
                                )
                            );
                        }
                    }
                }
            }

            return base.VisitMethodCall(methodCallExp);
        }

        #region MethodInfo functors

        private static readonly Func<Type, MethodInfo> GetWhehreMethodInfoFunctor = ((Func<Type, MethodInfo>)GetWhereMethodInfo).Memoize();
        private static MethodInfo GetWhereMethodInfo(Type entityType)
        {
            var whereMethodInfo =
            (
                from mi in typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                where mi.Name == "Where"
                let paramInfos = mi.GetParameters()
                where paramInfos.Length == 2
                where (paramInfos[0].ParameterType.GetGenericTypeDefinition() == typeof(IQueryable<>))
                let expressionGenericArgs = paramInfos[1].ParameterType.GetGenericArguments()
                where expressionGenericArgs.Length == 1
                where expressionGenericArgs[0].GetGenericTypeDefinition() == typeof(Func<,>)
                select mi
            )
            .Single();

            return whereMethodInfo.MakeGenericMethod(entityType);
        }

        private static readonly Func<string, Type, MethodInfo> GetScalarPredicateMethodInfoFunctor = ((Func<string, Type, MethodInfo>)GetScalarPredicateMethodInfo).Memoize();
        private static MethodInfo GetScalarPredicateMethodInfo(string methodName, Type entityType)
        {
            var anyMethodInfo =
            (
                from mi in typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                where mi.Name == methodName
                let paramInfos = mi.GetParameters()
                where paramInfos.Length == 1
                select mi
            )
            .Single();

            return anyMethodInfo.MakeGenericMethod(entityType);
        }

        #endregion
    }
}
