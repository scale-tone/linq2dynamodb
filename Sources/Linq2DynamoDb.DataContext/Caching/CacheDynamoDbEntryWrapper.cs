using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Custom serialization for DynamoDbEntry (which is not serializable itself)
    /// </summary>
    [Serializable]
    public class CacheDynamoDbEntryWrapper : ISerializable
    {
        public DynamoDBEntry Entry { get; set; }

        public CacheDynamoDbEntryWrapper(DynamoDBEntry entry)
        {
            this.Entry = entry;
        }

        #region ISerializable implementation

// ReSharper disable UnusedParameter.Local
        private CacheDynamoDbEntryWrapper(SerializationInfo info, StreamingContext context)
// ReSharper restore UnusedParameter.Local
        {
            var primitiveList = new List<Primitive>();

            var en = info.GetEnumerator();
            while (en.MoveNext())
            {
                if (en.Name == "Primitive")
                {
                    this.Entry = ((CachePrimitiveWrapper)en.Value).Primitive;
                    return;
                }

                primitiveList.Add(((CachePrimitiveWrapper)en.Value).Primitive);
            }

            this.Entry = primitiveList;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            var primitive = this.Entry as Primitive;

            if (primitive == null)
            {
                var primitiveList = this.Entry as PrimitiveList;
                Debug.Assert(primitiveList != null);

                int i = 0;
                foreach (var curPrimitive in primitiveList.AsListOfPrimitive())
                {
                    info.AddValue(i++.ToString(), new CachePrimitiveWrapper(curPrimitive), typeof(CachePrimitiveWrapper));
                }
            }
            else
            {
                info.AddValue("Primitive", new CachePrimitiveWrapper(primitive), typeof(CachePrimitiveWrapper));
            }
        }

        #endregion
    }
}
