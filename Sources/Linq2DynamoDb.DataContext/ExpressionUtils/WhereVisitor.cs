using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;
using Expression = System.Linq.Expressions.Expression;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    using System.Reflection;

    /// <summary>
    /// Visits the Where clause and collects conditions
    /// </summary>
    internal class WhereVisitor : ExpressionVisitor
    {
        public readonly List<string> FieldNames = new List<string>();
        public readonly List<ScanOperator> ScanOperators = new List<ScanOperator>();
        public readonly List<DynamoDBEntry[]> FieldValues = new List<DynamoDBEntry[]>();

        private readonly Type _tableEntityType;

        private bool _compareToMethodUsed = false;

        public WhereVisitor(Type tableEntityType)
        {
            this._tableEntityType = tableEntityType;
        }

        protected override Expression VisitUnary(UnaryExpression unaryExp)
        {
            // support for enum fields (they're converted to int by LINQ for some reason)
            if (unaryExp.NodeType == ExpressionType.Convert)
            {
                var propertyExp = unaryExp.Operand as MemberExpression;
                if (propertyExp == null)
                {
                    throw new NotSupportedException(string.Format("The expression '{0}' is not supported", unaryExp.Operand));
                }
                this.VisitMember(propertyExp);
                return unaryExp;
            }

            // supporting NotContains operator
            if (unaryExp.NodeType == ExpressionType.Not)
            {
                var methodCallExp = unaryExp.Operand as MethodCallExpression;
                if 
                (
                    (methodCallExp != null)
                    &&
                    (methodCallExp.Method.Name == "Contains")
                )
                {
                    var propertyExp = methodCallExp.Object as MemberExpression;
                    if (propertyExp == null)
                    {
                        throw new NotSupportedException(string.Format("The method '{0}' is not supported", methodCallExp.Method.Name));
                    }

                    this.ScanOperators.Add(ScanOperator.NotContains);
                    this.VisitConstant((ConstantExpression)methodCallExp.Arguments[0]);
                    this.VisitMember(propertyExp);
                    return unaryExp;
                }
            }

            throw new NotSupportedException("Unary operators are not supported");
        }

        protected override Expression VisitBinary(BinaryExpression binaryExp)
        {
            this.Visit(binaryExp.Left);

            switch (binaryExp.NodeType)
            {
            case ExpressionType.And:
            case ExpressionType.AndAlso:

                if
                (
                    (binaryExp.Left.NodeType == ExpressionType.Constant)
                    ||
                    (binaryExp.Right.NodeType == ExpressionType.Constant)
                )
                {
                    throw new NotSupportedException("Logical operations with constants are not supported");
                }

            break;
            case ExpressionType.Equal:
                this.ScanOperators.Add(ScanOperator.Equal);
            break;
            case ExpressionType.NotEqual:
                this.ScanOperators.Add(ScanOperator.NotEqual);
            break;
            case ExpressionType.LessThan:
                this.ScanOperators.Add(ScanOperator.LessThan);
            break;
            case ExpressionType.LessThanOrEqual:
                this.ScanOperators.Add(ScanOperator.LessThanOrEqual);
            break;
            case ExpressionType.GreaterThan:
                this.ScanOperators.Add(ScanOperator.GreaterThan);
            break;
            case ExpressionType.GreaterThanOrEqual:
                this.ScanOperators.Add(ScanOperator.GreaterThanOrEqual);
            break;
            default:
                throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", binaryExp.NodeType));
            }

            this.Visit(binaryExp.Right);

            return binaryExp;
        }

        protected override Expression VisitMethodCall(MethodCallExpression methodCallExp)
        {
            var constantExp = methodCallExp.Object as ConstantExpression;
            if (constantExp != null)
            {
                // support for IN operator 
                this.VisitListMethodCall(methodCallExp);
                return methodCallExp;
            }

            var propertyExp = methodCallExp.Object as MemberExpression;
            if (propertyExp == null)
            {
                this.VisitStaticMethodCall(methodCallExp);
                return methodCallExp;
            }

            switch (methodCallExp.Method.Name)
            {
                // string field comparison support
                case "CompareTo":
                    this._compareToMethodUsed = true;
                break;
                case "Contains":
                    this.ScanOperators.Add(ScanOperator.Contains);
                break;
                case "StartsWith":
                    this.ScanOperators.Add(ScanOperator.BeginsWith);
                break;
                default:
                    throw new NotSupportedException(string.Format("The method '{0}' is not supported", methodCallExp.Method.Name));
            }

            this.VisitConstant((ConstantExpression)methodCallExp.Arguments[0]);
            this.VisitMember(propertyExp);
            return methodCallExp;
        }

        private void VisitStaticMethodCall(MethodCallExpression methodCallExp)
        {
            if
            (
                (methodCallExp.Method.Name == "Contains") 
                && 
                (methodCallExp.Arguments.Count() == 2)
            )
            {
                this.VisitMember((MemberExpression)methodCallExp.Arguments[1]);
                this.VisitArrayConstant((ConstantExpression)methodCallExp.Arguments[0]);
                return;
            }
            throw new NotSupportedException(string.Format("The static method '{0}' is not supported", methodCallExp.Method.Name));
        }

        private void VisitListMethodCall(MethodCallExpression methodCallExp)
        {
            if 
            (
                (methodCallExp.Method.Name == "Contains")
                &&
                (methodCallExp.Object != null)
            )
            {
                this.VisitMember((MemberExpression)methodCallExp.Arguments[0]);
                this.VisitArrayConstant((ConstantExpression)methodCallExp.Object);
                return;
            }
            throw new NotSupportedException(string.Format("The method '{0}' is not supported", methodCallExp.Method.Name));
        }

        private void VisitArrayConstant(ConstantExpression constantExp)
        {
            this.ScanOperators.Add(ScanOperator.In);

            var elementType = ReflectionUtils.GetElementType(constantExp.Type);

            this.FieldValues.Add
            (
                (
                    from
                        object v in (IEnumerable)constantExp.Value
                    select
                        v.ToDynamoDbEntry(elementType)
                )
                .ToArray()
            );
        }

        protected override Expression VisitConstant(ConstantExpression constantExp)
        {
            if ((this._compareToMethodUsed) && (constantExp.Type == typeof(int)))
            {
                // string field comparison support
                if ((int) constantExp.Value != 0)
                {
                    throw new NotSupportedException("Result of CompareTo() method should always be compared to 0");
                }
            }
            else
            {
                this.FieldValues.Add
                (
                    new[] { constantExp.Value == null ? null : constantExp.Value.ToDynamoDbEntry(constantExp.Type) }
                );
            }
            return constantExp;
        }

        protected override Expression VisitMember(MemberExpression memberExp)
        {
            if
            (
                (memberExp.Expression != null)
                &&
                (memberExp.Expression.NodeType == ExpressionType.Parameter)
                &&
                (memberExp.Expression.Type == this._tableEntityType)
            )
            {
                this.FieldNames.Add(memberExp.Member.Name);
                return memberExp;
            }
            throw new NotSupportedException(string.Format("The member '{0}' is not a member of {1} table", memberExp.Member.Name, _tableEntityType.Name));
        }
    }
}
