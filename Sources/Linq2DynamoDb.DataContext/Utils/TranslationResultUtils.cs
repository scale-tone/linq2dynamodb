using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Linq2DynamoDb.DataContext.ExpressionUtils;

namespace Linq2DynamoDb.DataContext.Utils
{
    /// <summary>
    /// Utilities for converting the general list of conditions into get/query/scan operations
    /// </summary>
    internal static class TranslationResultUtils
    {
        /// <summary>
        /// Tries to extract entity key from the list of conditions
        /// </summary>
        internal static EntityKey TryGetEntityKeyForTable(this TranslationResult translationResult, Table tableDefinition)
        {
            Primitive hashKeyValue;
            if (!translationResult.Conditions.TryGetValueForKey(tableDefinition.HashKeys[0], out hashKeyValue))
            {
                return null;
            }

            if (hashKeyValue == null)
            {
                throw new InvalidOperationException("Hash key value should not be null");
            }

            // if there's a range key in the table
            if (tableDefinition.RangeKeys.Any())
            {
                Primitive rangeKeyValue;
                if(!translationResult.Conditions.TryGetValueForKey(tableDefinition.RangeKeys[0], out rangeKeyValue))
                {
                    return null;
                }

                //TODO: check, that hash and range keys really cannot be null
                if (rangeKeyValue == null)
                {
                    throw new NotSupportedException("Range key value should not be null");
                }

                // if any other conditions except hash and range key specified
                if (translationResult.Conditions.Count > 2)
                {
                    throw new NotSupportedException("When requesting a single entity by it's hash key and range key, no need to specify additional conditions");
                }

                return new EntityKey(hashKeyValue, rangeKeyValue);
            }

            // if any other conditions except hash key specified
            if (translationResult.Conditions.Count > 1)
            {
                throw new NotSupportedException("When requesting a single entity by it's hash key, no need to specify additional conditions");
            }

            return new EntityKey(hashKeyValue);
        }

        /// <summary>
        /// Tries to make up a query request from the list of conditions using table keys and local secondary indexes
        /// </summary>
        internal static bool TryGetQueryFilterForTable(this TranslationResult translationResult, Table tableDefinition, out QueryFilter resultFilter, out string indexName)
        {
            resultFilter = null;
            indexName = null;

            Primitive hashKeyValue;
            if (!translationResult.Conditions.TryGetValueForKey(tableDefinition.HashKeys[0], out hashKeyValue))
            {
                return false;
            }
            if (hashKeyValue == null)
            {
                throw new NotSupportedException("Hash key value should not be null");
            }

            resultFilter = new QueryFilter(tableDefinition.HashKeys[0], QueryOperator.Equal, hashKeyValue);

            // Copying the list of conditions. without HashKey condition, which is already matched.
            var conditions = translationResult.Conditions.ExcludeField(tableDefinition.HashKeys[0]);

            if (conditions.Count <= 0)
            {
                return true;
            }

            // first trying to search by range key
            if
            (
                (tableDefinition.RangeKeys.Count == 1)
                &&
                (TryMatchFieldWithCondition(tableDefinition.RangeKeys[0], resultFilter, conditions))
            )
            {
                return TryPutRemainingConditionsToQueryFilter(resultFilter, conditions);
            }

            // now trying to use local secondary indexes
            foreach (var index in tableDefinition.LocalSecondaryIndexes.Values)
            {
                // a local secondary index should always have range key
                string indexedField = index.KeySchema.Single(kse => kse.KeyType == "RANGE").AttributeName;

                if (TryMatchFieldWithCondition(indexedField, resultFilter, conditions))
                {
                    indexName = index.IndexName;
                    return TryPutRemainingConditionsToQueryFilter(resultFilter, conditions);
                }
            }

            return false;
        }

        /// <summary>
        /// Tries to make up a query request from the list of conditions using a global secondary index
        /// </summary>
        internal static bool TryGetQueryFilterForGlobalSeconaryIndex(this TranslationResult translationResult, GlobalSecondaryIndexDescription indexDescription, out QueryFilter resultFilter)
        {
            resultFilter = null;

            string hashKeyFieldName = indexDescription.KeySchema.Single(ks => ks.KeyType == "HASH").AttributeName;

            Primitive hashKeyValue;
            if (!translationResult.Conditions.TryGetValueForKey(hashKeyFieldName, out hashKeyValue))
            {
                return false;
            }
            if (hashKeyValue == null)
            {
                throw new NotSupportedException("Hash key value should not be null");
            }

            resultFilter = new QueryFilter(hashKeyFieldName, QueryOperator.Equal, hashKeyValue);

            // Copying the list of conditions. without HashKey condition, which is already matched.
            var conditions = translationResult.Conditions.ExcludeField(hashKeyFieldName);

            if (conditions.Count <= 0)
            {
                return true;
            }

            var rangeKeyElement = indexDescription.KeySchema.SingleOrDefault(ks => ks.KeyType == "RANGE");
            if (rangeKeyElement == null)
            {
                return false;
            }

            // first trying to search by range key
            if(TryMatchFieldWithCondition(rangeKeyElement.AttributeName, resultFilter, conditions))
            {
                return TryPutRemainingConditionsToQueryFilter(resultFilter, conditions);
            }

            return false;
        }

        /// <summary>
        /// Tries to make up a batch get operation for translation result
        /// </summary>
        internal static DocumentBatchGet GetBatchGetOperationForTable(this TranslationResult translationResult, Table tableDefinition)
        {
            var conditions = translationResult.Conditions;

            // if there's a range key in the table
            if (tableDefinition.RangeKeys.Any())
            {
                // HashKey should be exactly specified
                Primitive hashKeyValue;
                if (!conditions.TryGetValueForKey(tableDefinition.HashKeys[0], out hashKeyValue))
                {
                    return null;
                }
                if (hashKeyValue == null)
                {
                    throw new NotSupportedException("Hash key value should not be null");
                }

                return GetBatchGetOperationForSearchConditions(tableDefinition, conditions.ExcludeField(tableDefinition.HashKeys[0]), tableDefinition.RangeKeys.First(), hashKeyValue);
            }
            else
            {
                return GetBatchGetOperationForSearchConditions(tableDefinition, conditions, tableDefinition.HashKeys.First(), null);
            }
        }

        /// <summary>
        /// Tries to make up a batch get operation from SearchConditions
        /// </summary>
        private static DocumentBatchGet GetBatchGetOperationForSearchConditions(Table tableDefinition, SearchConditions conditions, string keyFieldName, Primitive hashKeyValue)
        {
            // there should be only one IN operator for key field
            if
            (!(
                (conditions.Count == 1)
                &&
                (conditions.Keys.First() == keyFieldName)
            ))
            {
                return null;
            }

            var conditionList = conditions.Values.First();
            if
            (!(
                (conditionList.Count == 1)
                &&
                (conditionList.First().Operator == ScanOperator.In)
            ))
            {
                return null;
            }

            var result = tableDefinition.CreateBatchGet();
            foreach (var value in conditionList.First().Values)
            {
                if (hashKeyValue == null)
                {
                    result.AddKey((Primitive)value);
                }
                else
                {
                    result.AddKey(hashKeyValue, (Primitive)value);
                }
            }
            return result;
        }

        /// <summary>
        /// Tries to fit all remaining SearchConditions to QueryFilter
        /// </summary>
        private static bool TryPutRemainingConditionsToQueryFilter(QueryFilter resultFilter, SearchConditions conditions)
        {
            return conditions
                .Keys.ToArray()
                .All(fieldName => TryMatchFieldWithCondition(fieldName, resultFilter, conditions));
        }

        private static bool TryMatchFieldWithCondition(string fieldName, QueryFilter resultFilter, SearchConditions conditions)
        {
            List<SearchCondition> conditionList;
            if (!conditions.TryGetValue(fieldName, out conditionList))
            {
                return false;
            }

            switch (conditionList.Count)
            {
                case 2: // checking for the between operator
                {
                    var lessThanOrEqualCondition = conditionList.SingleOrDefault(c => c.Operator == ScanOperator.LessThanOrEqual);
                    var greaterThanOrEqualCondition = conditionList.SingleOrDefault(c => c.Operator == ScanOperator.GreaterThanOrEqual);

                    if ((lessThanOrEqualCondition == null) || (greaterThanOrEqualCondition == null))
                    {
                        throw new InvalidOperationException("Multiple conditions for the same field are only supported for the BETWEEN case");
                    }

                    if
                    (
                        (lessThanOrEqualCondition.Values.Length != 1)
                        ||
                        (greaterThanOrEqualCondition.Values.Length != 1)
                    )
                    {
                        return false;
                    }

                    resultFilter.AddCondition(fieldName, QueryOperator.Between, greaterThanOrEqualCondition.Values[0], lessThanOrEqualCondition.Values[0]);
                }
                break;
                case 1:
                {
                    SearchCondition condition = conditionList[0];

                    // here we need to convert operators, as AWS SDK's default conversion is buggy
                    QueryOperator queryOperator;
                    if 
                    (
                        (!TryConvertScanOperatorToQueryOperator(condition.Operator, out queryOperator))
                        ||
                        (condition.Values.Length != 1)
                    )
                    {
                        return false;
                    }

                    resultFilter.AddCondition(fieldName, queryOperator, condition.Values[0]);
                }
                break;
                default:
                    throw new InvalidOperationException(string.Format("Too many conditions for field {0}", fieldName));
            }

            // removing the matched condition
            conditions.Remove(fieldName);

            return true;
        }

        private static bool TryConvertScanOperatorToQueryOperator(ScanOperator scanOperator, out QueryOperator result)
        {
            switch (scanOperator)
            {
                case ScanOperator.Equal:
                    result = QueryOperator.Equal;
                break;
                case ScanOperator.GreaterThan:
                    result = QueryOperator.GreaterThan;
                break;
                case ScanOperator.GreaterThanOrEqual:
                    result = QueryOperator.GreaterThanOrEqual;
                break;
                case ScanOperator.LessThan:
                    result = QueryOperator.LessThan;
                break;
                case ScanOperator.LessThanOrEqual:
                    result = QueryOperator.LessThanOrEqual;
                break;
                case ScanOperator.BeginsWith:
                    result = QueryOperator.BeginsWith;
                break;
                default:
                {
                    result = QueryOperator.Equal;
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Prepares parameters for a scan operation from the list of conditions
        /// </summary>
        internal static ScanFilter GetScanFilterForTable(this TranslationResult translationResult, Table tableDefinition)
        {
            // the last thing to do is to make a full scan
            var scanFilter = new ScanFilter();

            //TODO: check for BETWEEN operator
            foreach (var condition in translationResult.Conditions.Flatten())
            {
                bool conditionForThisFieldExistsAlready = scanFilter.ToConditions().ContainsKey(condition.Item1);
                if (conditionForThisFieldExistsAlready)
                {
                    throw new InvalidOperationException(string.Format("Multiple conditions for the same {0} field are not supported by AWS SDK. As a workaround, please, use a custom FilterExpression.", condition.Item1));
                }

                if 
                (
                    (condition.Item2.Values.Length == 1)
                    &&
                    (condition.Item2.Values[0] == null)
                )
                {
                    switch (condition.Item2.Operator)
                    {
                    case ScanOperator.Equal:
                        scanFilter.AddCondition(condition.Item1, ScanOperator.IsNull );
                    break;
                    case ScanOperator.NotEqual:
                        scanFilter.AddCondition(condition.Item1, ScanOperator.IsNotNull);
                    break;
                    default:
                        throw new InvalidOperationException(string.Format("You cannot use {0} operator with null value", condition.Item2.Operator));
                    }
                }
                else
                {
                    scanFilter.AddCondition(condition.Item1, condition.Item2.Operator, condition.Item2.Values);
                }
            }

            return scanFilter;
        }
    }
}
