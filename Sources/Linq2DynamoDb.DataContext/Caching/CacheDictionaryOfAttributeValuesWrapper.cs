using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.Model;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Custom serialization for Dictionary of AttributeValues
    /// </summary>
    [Serializable]
    internal class CacheDictionaryOfAttributeValuesWrapper : ISerializable
    {
        public Dictionary<string, AttributeValue> Dictionary { get; private set; }

        public CacheDictionaryOfAttributeValuesWrapper(Dictionary<string, AttributeValue> dic)
        {
            this.Dictionary = dic;
        }

        #region ISerializable implementation

        private CacheDictionaryOfAttributeValuesWrapper(SerializationInfo info, StreamingContext context)
        {
            this.Dictionary = new Dictionary<string, AttributeValue>();

            var en = info.GetEnumerator();
            while (en.MoveNext())
            {
                this.Dictionary[en.Name] = ((CacheAttributeValueWrapper) en.Value).AttributeValue;
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            foreach (var pair in this.Dictionary)
            {
                info.AddValue(pair.Key, new CacheAttributeValueWrapper(pair.Value));
            }
        }

        #endregion
    }
}
