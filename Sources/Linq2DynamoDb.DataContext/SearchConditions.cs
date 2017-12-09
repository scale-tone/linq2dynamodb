using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;
using Expression = System.Linq.Expressions.Expression;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Represents a set of search conditions. Used to define an index in cache
    /// </summary>
    [Serializable]
    public class SearchConditions : Dictionary<string, List<SearchCondition>>
    {
        /// <summary>
        /// Required for deserialization to work properly, don't remove.
        /// </summary>
        public SearchConditions()
        {
        }

        /// <summary>
        /// This key is used to indentify the index in cache
        /// </summary>
        public string Key
        {
            get
            {
                var sb = new StringBuilder();

                int i = 0;
                foreach (var kv in this.Flatten())
                {
                    if (i++ > 0)
                    {
                        sb.Append(" AND ");
                    }

                    sb.Append(kv.Item1);
                    sb.Append(" ");
                    sb.Append(kv.Item2.Operator);
                    sb.Append(" ");

                    DynamoDbEntriesAsString(sb, kv.Item2.Values);
                }

                return sb.ToString();
            }
        }

        /// <summary>
        /// Adds a condition for the field
        /// </summary>
        public void AddCondition(string fieldName, SearchCondition condition)
        {
            List<SearchCondition> list;
            if (!this.TryGetValue(fieldName, out list))
            {
                list = new List<SearchCondition>();
                this.Add(fieldName, list);
            }
            list.Add(condition);
        }

        /// <summary>
        /// Returns conditions as a flat list
        /// </summary>
        public IEnumerable<Tuple<string, SearchCondition>> Flatten()
        {
            return 
                from kv in this 
                    from c in kv.Value 
                        select new Tuple<string, SearchCondition>(kv.Key, c);
        }

        /// <summary>
        /// Creates a copy of these conditions without a specified field
        /// </summary>
        public SearchConditions ExcludeField(string fieldName)
        {
            var clone = new SearchConditions();
            foreach (var kv in this.Where(kv => kv.Key != fieldName))
            {
                clone.Add(kv.Key, kv.Value);
            }
            return clone;
        }

        /// <summary>
        /// Tries to get an exact value for a key field
        /// </summary>
        public bool TryGetValueForKey(string keyName, out Primitive value)
        {
            value = null;

            List<SearchCondition> list;
            if (!this.TryGetValue(keyName, out list))
            {
                return false;
            }

            Debug.Assert(list != null);

            if ((list.Count != 1) || (list[0].Operator != ScanOperator.Equal))
            {
                return false;
            }
            value = (Primitive)list[0].Values[0];
            return true;
        }

        public override string ToString()
        {
            return this.Key;
        }

        private static void DynamoDbEntriesAsString(StringBuilder sb, DynamoDBEntry[] entries)
        {
            if (entries.Length == 1)
            {
                DynamoDbEntryAsString(sb, entries[0]);
            }
            else if (entries.Length != 0)
            {
                sb.Append("(");
                for (int i = 0; i < entries.Length; i++ )
                {
                    if (i != 0)
                    {
                        sb.Append(",");
                    }
                    DynamoDbEntryAsString(sb, entries[i]);
                }
                sb.Append(")");
            }
        }

        private static void DynamoDbEntryAsString(StringBuilder sb, DynamoDBEntry entry)
        {
            var list = entry as PrimitiveList;
            if (list == null)
            {
                sb.Append(entry == null ? "NULL" : entry.AsString());
                return;
            }

            string s = list.AsListOfPrimitive().Aggregate
                (
                    string.Empty, 
                    (c, p) => c == string.Empty ? p.AsString() : c + "," + p.AsString()
                );

            sb.Append("(");
            sb.Append(s);
            sb.Append(")");
        }

        /// <summary>
        /// Checks if a Document satisfies the list of conditions for this index
        /// </summary>
        public bool MatchesSearchConditions(Document doc, Type entityType)
        {
            return GetPredicate(entityType)(doc);
        }

        #region Creating and caching predicates

        /// <summary>
        /// The maximum number of cached predicates per entity type
        /// If the number is exceeded, some predicates are removed (and recompiled next time)
        /// </summary>
        private const int MaxCachedConditionsPredicates = 300;

        /// <summary>
        /// A Random to be used for keeping the predicates cache slim 
        /// </summary>
        private static readonly Random Random = new Random(DateTime.Now.Millisecond);

        /// <summary>
        /// Cached predicates for each possible entity type and search condition
        /// </summary>
        private readonly static ConcurrentDictionary<Type, ConcurrentDictionary<string, Func<Document, bool>>> PredicatesDictionary = new ConcurrentDictionary<Type, ConcurrentDictionary<string, Func<Document, bool>>>();

        /// <summary>
        /// Gets from cache or compiles a predicate for the specified list of conditions and the specified entity type
        /// </summary>
        private Func<Document, bool> GetPredicate(Type entityType)
        {
            // if no conditions specified - then just returning a predicate, that always returns true
            if (this.Count == 0)
            {
                return doc => true;
            }

            var predicatesDic = PredicatesDictionary.GetOrAdd(entityType, new ConcurrentDictionary<string, Func<Document, bool>>());
            var result = predicatesDic.GetOrAdd(this.Key, _ => this.CreatePredicate(entityType));

            TryKeepPredicatesDictionarySlim(predicatesDic);

            return result;
        }

        /// <summary>
        /// Creates a lambda expression, that checks a Document to satisfy the list of conditions.
        /// Then compiles it and returns a predicate.
        /// </summary>
        private Func<Document, bool> CreatePredicate(Type entityType)
        {
            // parameter, that represents input Document
            var docParameter = Expression.Parameter(typeof(Document));

            Expression predicateExp = null;

            foreach (var condition in this.Flatten())
            {
                string fieldName = condition.Item1;

                var fieldPropertyInfo = entityType.GetProperty(fieldName);
                if (fieldPropertyInfo == null)
                {
                    throw new InvalidOperationException(string.Format("An entity type {0} doesn't contain a property with name {1}, which was specified in search condition", entityType.Name, fieldName));
                }

                var fieldType = fieldPropertyInfo.PropertyType;
                var fieldBaseType = fieldType.GetTypeInfo().BaseType;
                var fieldValues = condition.Item2.Values;

                // operation of getting a Document property value by it's name 
                Expression getFieldExp = Expression.Property(docParameter, "Item", Expression.Constant(fieldName));

                if (fieldBaseType == typeof(Enum))
                {
                    // need to convert enums to ints
                    getFieldExp = Expression.Convert(getFieldExp, typeof(int));
                }

                // operation of converting the property to fieldType
                var operand1 = Expression.Convert(getFieldExp, fieldType);

                Expression conditionExp;
                if (condition.Item2.Operator == ScanOperator.In)
                {
                    // special support for IN operator

                    var valueList = new ArrayList();
                    foreach (var fieldValue in fieldValues)
                    {
                        valueList.Add(fieldValue.ToObject(fieldType));
                    }

                    conditionExp = Expression.Call
                        (
                            Expression.Constant(valueList),
                            "Contains",
                            new Type[0],
                            Expression.Convert(operand1, typeof(object))
                        );
                }
                else
                {
                    Expression valueExp = Expression.Constant(fieldValues[0]);

                    if (fieldBaseType == typeof(Enum))
                    {
                        // need to convert enums to ints
                        valueExp = Expression.Convert(valueExp, typeof(int));
                    }

                    // operation of converting the fieldValue to fieldType
                    var operand2 = Expression.Convert(valueExp, fieldType);

                    // now getting a predicate for current field
                    conditionExp = ScanOperatorToExpression(condition.Item2.Operator, operand1, operand2);
                }

                // attaching it to other predicates
                predicateExp = predicateExp == null ? conditionExp : Expression.AndAlso(predicateExp, conditionExp);
            }

            Debug.Assert(predicateExp != null);
            // compiling the lambda into a predicate
            return (Func<Document, bool>)Expression.Lambda(predicateExp, docParameter).Compile();
        }

        private static Expression ScanOperatorToExpression(ScanOperator scanOperator, Expression operand1, Expression operand2)
        {
            switch (scanOperator)
            {
                case ScanOperator.Equal:
                    return Expression.Equal(operand1, operand2);
                case ScanOperator.NotEqual:
                    return Expression.NotEqual(operand1, operand2);
                case ScanOperator.LessThan:
                    if (operand1.Type == typeof(string))
                    {
                        return Expression.LessThan(Expression.Call(StringCompareMethodInfo, operand1, operand2, Expression.Constant(StringComparison.Ordinal)), Expression.Constant(0));
                    }
                    return Expression.LessThan(operand1, operand2);
                case ScanOperator.LessThanOrEqual:
                    if (operand1.Type == typeof(string))
                    {
                        return Expression.LessThanOrEqual(Expression.Call(StringCompareMethodInfo, operand1, operand2, Expression.Constant(StringComparison.Ordinal)), Expression.Constant(0));
                    }
                    return Expression.LessThanOrEqual(operand1, operand2);
                case ScanOperator.GreaterThan:
                    if (operand1.Type == typeof(string))
                    {
                        return Expression.GreaterThan(Expression.Call(StringCompareMethodInfo, operand1, operand2, Expression.Constant(StringComparison.Ordinal)), Expression.Constant(0));
                    }
                    return Expression.GreaterThan(operand1, operand2);
                case ScanOperator.GreaterThanOrEqual:
                    if (operand1.Type == typeof(string))
                    {
                        return Expression.GreaterThanOrEqual(Expression.Call(StringCompareMethodInfo, operand1, operand2, Expression.Constant(StringComparison.Ordinal)), Expression.Constant(0));
                    }
                    return Expression.GreaterThanOrEqual(operand1, operand2);
                case ScanOperator.BeginsWith:
                    if (operand1.Type == typeof(string))
                    {
                        return Expression.Call(operand1, StartsWithMethodInfo, operand2);
                    }
                    break;
                case ScanOperator.Contains:
                    if (operand1.Type == typeof(string))
                    {
                        return Expression.Call(operand1, ContainsMethodInfo, operand2);
                    }
                    break;
                case ScanOperator.NotContains:
                    if (operand1.Type == typeof(string))
                    {
                        return Expression.Not(Expression.Call(operand1, ContainsMethodInfo, operand2));
                    }
                    break;
            }
            throw new NotSupportedException(string.Format("Condition operator {0} is not supported", scanOperator));
        }

        /// <summary>
        /// Removes a random predicate from cache, if there're too many of them
        /// </summary>
        private static void TryKeepPredicatesDictionarySlim(ConcurrentDictionary<string, Func<Document, bool>> predicatesDic)
        {
            int count = predicatesDic.Count;
            if (count <= MaxCachedConditionsPredicates)
            {
                return;
            }
            try
            {
                string keyToRemove = predicatesDic.Keys.ElementAt(Random.Next(count));
                Func<Document, bool> func;
                predicatesDic.TryRemove(keyToRemove, out func);
            }
            catch (ArgumentOutOfRangeException) // protecting ourselves from key being removed concurrently
            {
            }
        }

        private static readonly MethodInfo StringCompareMethodInfo = ((Func<string, string, StringComparison, int>)string.Compare).GetMethodInfo();
        private static readonly MethodInfo StartsWithMethodInfo = ((Func<string, bool>)"".StartsWith).GetMethodInfo();
        private static readonly MethodInfo ContainsMethodInfo = ((Func<string, bool>)"".Contains).GetMethodInfo();

        #endregion

#if !NETSTANDARD1_6
        #region ISerializable custom implementation is needed, because enyim uses BinaryFormatter by default, which is stupid enough

        public SearchConditions(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        #endregion
#endif
    }
}
