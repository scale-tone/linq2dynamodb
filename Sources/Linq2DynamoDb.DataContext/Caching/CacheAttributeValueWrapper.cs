using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.Model;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Custom serialization for AttributeValue (which is not serializable itself)
    /// </summary>
    [Serializable]
    internal class CacheAttributeValueWrapper : ISerializable
    {
        public AttributeValue AttributeValue { get; private set; }

        public CacheAttributeValueWrapper(AttributeValue attributeValue)
        {
            this.AttributeValue = attributeValue;
        }

        #region ISerializable implementation

        private CacheAttributeValueWrapper(SerializationInfo info, StreamingContext context)
        {
            this.AttributeValue = new AttributeValue();

            var en = info.GetEnumerator();
            while (en.MoveNext())
            {
                switch (en.Name)
                {
                    case "S":
                        this.AttributeValue.S = (string)en.Value;
                        break;
                    case "SS":
                        this.AttributeValue.SS = (List<string>)en.Value;
                        break;
                    case "N":
                        this.AttributeValue.N = (string)en.Value;
                        break;
                    case "NS":
                        this.AttributeValue.NS = (List<string>)en.Value;
                        break;
                    case "B":
                        this.AttributeValue.B = (MemoryStream)en.Value;
                        break;
                    case "BS":
                        this.AttributeValue.BS = (List<MemoryStream>)en.Value;
                        break;
                    case "M":
                        this.AttributeValue.M = ((CacheDictionaryOfAttributeValuesWrapper)en.Value).Dictionary;
                        break;
                    case "L":
                        this.AttributeValue.L = ((CacheListOfAttributeValuesWrapper)en.Value).List;
                        break;
                }
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (this.AttributeValue.S != null)
            {
                info.AddValue("S", this.AttributeValue.S, typeof(string));
            }
            if ((this.AttributeValue.SS != null) && (this.AttributeValue.SS.Count > 0))
            {
                info.AddValue("SS", this.AttributeValue.SS, typeof(List<string>));
            }
            if (this.AttributeValue.N != null)
            {
                info.AddValue("N", this.AttributeValue.N, typeof(string));
            }
            if ((this.AttributeValue.NS != null) && (this.AttributeValue.NS.Count > 0))
            {
                info.AddValue("NS", this.AttributeValue.NS, typeof(List<string>));
            }
            if (this.AttributeValue.B != null)
            {
                info.AddValue("B", this.AttributeValue.B, typeof(MemoryStream));
            }
            if ((this.AttributeValue.BS != null) && (this.AttributeValue.BS.Count > 0))
            {
                info.AddValue("BS", this.AttributeValue.BS, typeof(List<MemoryStream>));
            }
            if ((this.AttributeValue.M != null) && (this.AttributeValue.M.Count > 0))
            {
                info.AddValue("M", new CacheDictionaryOfAttributeValuesWrapper(this.AttributeValue.M), typeof(CacheDictionaryOfAttributeValuesWrapper));
            }
            if ((this.AttributeValue.L != null) && (this.AttributeValue.L.Count > 0))
            {
                info.AddValue("L", new CacheListOfAttributeValuesWrapper(this.AttributeValue.L), typeof(CacheListOfAttributeValuesWrapper));
            }
        }

        #endregion
    }
}
