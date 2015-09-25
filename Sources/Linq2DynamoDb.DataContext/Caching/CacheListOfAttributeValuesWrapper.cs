using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.Model;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Custom serialization for List of AttributeValues
    /// </summary>
    [Serializable]
    internal class CacheListOfAttributeValuesWrapper : ISerializable
    {
        public List<AttributeValue> List { get; private set; }

        public CacheListOfAttributeValuesWrapper(List<AttributeValue> list)
        {
            this.List = list;
        }

        #region ISerializable implementation

        private CacheListOfAttributeValuesWrapper(SerializationInfo info, StreamingContext context)
        {
            this.List = new List<AttributeValue>();

            var en = info.GetEnumerator();
            while (en.MoveNext())
            {
                this.List.Add(((CacheAttributeValueWrapper)en.Value).AttributeValue);
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            int i = 0;
            foreach (var value in this.List)
            {
                info.AddValue(i++.ToString(), new CacheAttributeValueWrapper(value), typeof(CacheAttributeValueWrapper));
            }
        }

        #endregion
    }
}
