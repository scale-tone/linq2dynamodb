using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Caching;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Represents a ScanOperator and a value
    /// </summary>
    [Serializable]
    public class SearchCondition : ISerializable
    {
        public ScanOperator Operator { get; set; }
        public DynamoDBEntry[] Values { get; set; }

        public SearchCondition(ScanOperator op, params DynamoDBEntry[] values)
        {
            this.Operator = op;
            this.Values = values;
        }

        #region ISerializable custom implementation is needed, because enyim uses BinaryFormatter by default, which is stupid enough

// ReSharper disable UnusedParameter.Local
        private SearchCondition(SerializationInfo info, StreamingContext context)
// ReSharper restore UnusedParameter.Local
        {
            var valuesList = new List<CacheDynamoDbEntryWrapper>();

            var en = info.GetEnumerator();
            while (en.MoveNext())
            {
                if (en.Name == "Operator")
                {
                    this.Operator = (ScanOperator)en.Value;
                }
                else
                {
                    valuesList.Add((CacheDynamoDbEntryWrapper)en.Value);
                }
            }

            this.Values = valuesList.Select(wr => wr.Entry).ToArray();
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("Operator", this.Operator, typeof(ScanOperator));

            int i = 0;
            foreach (var value in this.Values)
            {
                info.AddValue("Value" + i++.ToString(), new CacheDynamoDbEntryWrapper(value), typeof(CacheDynamoDbEntryWrapper));
            }
        }

        #endregion
    }
}
