using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Represents a set of search conditions. Used to define an index in cache
    /// </summary>
    [Serializable]
    public class SearchConditions : Dictionary<string, List<SearchCondition>>
    {
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

        #region ISerializable custom implementation is needed, because enyim uses BinaryFormatter by default, which is stupid enough

        public SearchConditions(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        #endregion
    }
}
