using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    using System.Reflection;

    /// <summary> 
    /// Evaluates & replaces sub-trees when first candidate is reached (top-down) 
    /// </summary> 
    internal class SubtreeEvaluationVisitor : ExpressionVisitorBase
    {
        private HashSet<Expression> _candidates;
        private bool _isAlreadyEvaluating;

        internal Expression EvaluateSubtree(Expression exp)
        {
            if (this._isAlreadyEvaluating)
            {
                // preventing a StackOverflowException
                throw new NotSupportedException("The query caused a recursive query. This is not supported.");
            }
            this._isAlreadyEvaluating = true;
            try
            {
                this._candidates = new Nominator().Nominate(exp);
                return this.Visit(exp);
            }
            finally
            {
                this._isAlreadyEvaluating = false;
            }
        }

        public override Expression Visit(Expression exp)
        {
            if (exp == null)
            {
                return null;
            }
            if (this._candidates.Contains(exp))
            {
                return this.Evaluate(exp);
            }
            return base.Visit(exp);
        }

        private Expression Evaluate(Expression e)
        {
            if (e.NodeType == ExpressionType.Constant)
            {
                return e;
            }

            var delegateToCall = Expression.Lambda(e).Compile();

            return Expression.Constant(delegateToCall.DynamicInvoke(null), e.Type);
        }

        /// <summary>
        /// This overload fixes an issue when using select new SomeProjectionType {...}
        /// </summary>
        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if (node.NewExpression.NodeType == ExpressionType.New)
            {
                return node;
            }
            return base.VisitMemberInit(node);
        }

        /// <summary> 
        /// Performs bottom-up analysis to determine which nodes can possibly 
        /// be part of an evaluated sub-tree. 
        /// </summary> 
        private class Nominator : ExpressionVisitor
        {
            private readonly HashSet<Expression> _candidates = new HashSet<Expression>();
            private bool _cannotBeEvaluated;

            internal HashSet<Expression> Nominate(Expression expression)
            {
                this.Visit(expression);
                return this._candidates;
            }

            public override Expression Visit(Expression expression)
            {
                if (expression == null)
                {
                    return null;
                }

                bool saveCannotBeEvaluated = this._cannotBeEvaluated;
                this._cannotBeEvaluated = false;

                base.Visit(expression);

                if (!this._cannotBeEvaluated)
                {
                    if (this.CanBeEvaluatedLocally(expression))
                    {
                        this._candidates.Add(expression);
                    }
                    else
                    {
                        this._cannotBeEvaluated = true;
                    }
                }

                this._cannotBeEvaluated |= saveCannotBeEvaluated;

                return expression;
            }

            private bool CanBeEvaluatedLocally(Expression expression)
            {
                var expressionTypeInfo = expression.Type.GetTypeInfo();

                // preventing recursion around Expression.Constant(DataTable<TEntity>)
                bool typeIsDataTable =
                    (
                        expressionTypeInfo.IsGenericType
                        &&
                        expressionTypeInfo.GetGenericTypeDefinition() == typeof (DataTable<>)
                    );

                return
                    (
                        (expression.NodeType != ExpressionType.Parameter)
                        &&
                        (!typeIsDataTable)
                    );
            }
        }
    }
}
