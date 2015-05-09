using System.Linq.Expressions;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    /// <summary>
    /// Base class for all expression visitors
    /// </summary>
    internal class ExpressionVisitorBase : ExpressionVisitor
    {
        /// <summary>
        /// Does some magic of unwrapping lambda calls
        /// </summary>
        protected static Expression StripQuotes(Expression exp)
        {
            while (exp.NodeType == ExpressionType.Quote)
            {
                exp = ((UnaryExpression)exp).Operand;
            }
            return exp;
        }
    }
}
