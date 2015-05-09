using System;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext.Caching
{
    /// <summary>
    /// Custom serialization for Primitive (which is not serializable itself)
    /// </summary>
    [Serializable]
    public class CachePrimitiveWrapper : ISerializable
    {
        public Primitive Primitive { get; private set; }

        public CachePrimitiveWrapper(Primitive primitive)
        {
            this.Primitive = primitive;
        }

        #region ISerializable implementation

        private CachePrimitiveWrapper(SerializationInfo info, StreamingContext context)
        {
            this.Primitive = new Primitive
            {
                Type = (DynamoDBEntryType)info.GetValue("Type", typeof(DynamoDBEntryType)),
                Value = info.GetValue("Value", typeof(object))
            };
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("Type", this.Primitive.Type, typeof(DynamoDBEntryType));
            info.AddValue("Value", this.Primitive.Value);
        }

        #endregion
    }
}
