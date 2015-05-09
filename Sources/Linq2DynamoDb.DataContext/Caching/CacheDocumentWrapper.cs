using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// A completely inefficient way to serialize/deserialize Document for caching
    /// TODO: make it more efficient
    /// </summary>
    [Serializable]
    public class CacheDocumentWrapper : ISerializable
    {
        public Document Document { get; private set; }

        public CacheDocumentWrapper(Document doc)
        {
            this.Document = doc;
        }

        #region ISerializable custom implementation is needed, because enyim uses BinaryFormatter by default, which is stupid enough 

        private CacheDocumentWrapper(SerializationInfo info, StreamingContext context)
        {
            var dic = new Dictionary<string, AttributeValue>();

            var en = info.GetEnumerator();
            while (en.MoveNext())
            {
                dic.Add(en.Name, ((CacheAttributeValueWrapper)en.Value).AttributeValue);
            }

            this.Document = Document.FromAttributeMap(dic);
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            foreach (var field in this.Document.ToAttributeMap())
            {
                info.AddValue(field.Key, new CacheAttributeValueWrapper(field.Value), typeof(CacheAttributeValueWrapper));
            }
        }

        #endregion
    }
}
