using System;
using System.Collections.Generic;
using System.Text;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    /// <summary>
    /// Represents the result of parsing LINQ expression
    /// </summary>
    internal class TranslationResult
    {
        /// <summary>
        /// Search conditions extracted from Where clause
        /// </summary>
        internal SearchConditions Conditions { get; set; }

        /// <summary>
        /// List of column names to retrieve from DynamoDb
        /// </summary>
        internal List<string> AttributesToGet { get; set; }

        /// <summary>
        /// This functor implements entity conversion, if select new {} was specified
        /// </summary>
        internal Delegate ProjectionFunc { get; set; }

        /// <summary>
        /// Column to order by
        /// </summary>
        internal string OrderByColumn { get; set; }

        /// <summary>
        /// Sort direction
        /// </summary>
        internal bool OrderByDesc { get; set; }

        /// <summary>
        /// Indicates that Count() method was specified
        /// TODO: not yet supported. Default Enumerable.Count() is used currently.
        /// </summary>
        internal bool CountRequested { get; set; }

        /// <summary>
        /// Groups properties and callbacks that are used for customizing the GET/QUERY/SCAN operations
        /// </summary>
        internal CustomizationHooks CustomizationHooks { get; set; }

        internal TranslationResult(string tableNameForLoggingPurposes)
        {
            this._tableNameForLoggingPurposes = tableNameForLoggingPurposes;
            this.Conditions = new SearchConditions();
            this.CustomizationHooks = new CustomizationHooks();
        }

        private readonly string _tableNameForLoggingPurposes;

        public override string ToString()
        {
            var sb = new StringBuilder();

            if (this.CountRequested)
            {
                sb.Append("SELECT COUNT(*)");
            }
            else if (this.AttributesToGet == null)
            {
                sb.Append("SELECT *");
            }
            else
            {
                sb.Append("SELECT ");
                sb.Append(string.Join(",", this.AttributesToGet));
            }

            sb.Append(" FROM ");
            sb.Append(this._tableNameForLoggingPurposes);

            string whereClause = this.Conditions.ToString();
            if (!string.IsNullOrEmpty(whereClause))
            {
                sb.Append(" WHERE ");
                sb.Append(whereClause);
            }

            if((this.CustomizationHooks.CustomFilterExpression != null) && (!string.IsNullOrEmpty(this.CustomizationHooks.CustomFilterExpression.ExpressionStatement)))
            {
                sb.Append(" FILTER EXPRESSION ");
                sb.Append(this.CustomizationHooks.CustomFilterExpression.ExpressionStatement);
            }

            if (!string.IsNullOrEmpty(this.OrderByColumn))
            {
                sb.Append(" ORDER BY ");
                sb.Append(this.OrderByColumn);

                if (this.OrderByDesc)
                {
                    sb.Append(" DESC");
                }
            }

            return sb.ToString();
        }
    }
}
