using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Caching;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Represents a key (hashKey and optionally rangeKey) of a DynamoDb entity
    /// </summary>
    [Serializable]
    public class EntityKey : IEquatable<EntityKey>, ISerializable
    {
        public Primitive HashKey { get; private set; }
        public Primitive RangeKey { get; private set; }

        public EntityKey(Primitive hashKey, Primitive rangeKey)
        {
            this.HashKey = hashKey;
            this.RangeKey = rangeKey;
        }

        public EntityKey(Primitive hashKey)
        {
            this.HashKey = hashKey;
        }

        public IDictionary<string, DynamoDBEntry> AsDictionary(Table tableDefinition)
        {
            if (this.RangeKey == null)
            {
                return new Dictionary<string, DynamoDBEntry>
                {
                    {tableDefinition.HashKeys[0], this.HashKey}
                };
            }
            return new Dictionary<string, DynamoDBEntry>
            {
                {tableDefinition.HashKeys[0], this.HashKey},
                {tableDefinition.RangeKeys[0], this.RangeKey}
            };
        }

        public bool Equals(EntityKey that)
        {
            if (!this.HashKey.Equals(that.HashKey))
            {
                return false;
            }
            if (this.RangeKey == null)
            {
                return (that.RangeKey == null);
            }
            return this.RangeKey.Equals(that.RangeKey);
        }

        public override bool Equals(object that)
        {
            if (!(that is EntityKey))
            {
                return false;
            }
            return this.Equals((EntityKey)that);
        }

        public override int GetHashCode()
        {
            // AWS SDK's Primitive.GetHashCode() implementation is stupid (returns random numbers)
            // so we'd better not use it here

            if (this.RangeKey == null)
            {
                return this.HashKey.AsString().GetHashCode();
            }
            return this.HashKey.AsString().GetHashCode() ^ this.RangeKey.AsString().GetHashCode();
        }

        public override string ToString()
        {
            if (this.RangeKey == null)
            {
                return this.HashKey;
            }
            return this.HashKey + ":" + this.RangeKey;
        }

        #region ISerializable implementation (used for caching, default serialization doesn't work, because Primitive is not serializable)

        private EntityKey(SerializationInfo info, StreamingContext context)
        {
            var en = info.GetEnumerator();
            while (en.MoveNext())
            {
                switch (en.Name)
                {
                case "HashKey":
                    this.HashKey = ((CachePrimitiveWrapper) en.Value).Primitive;
                break;
                case "RangeKey":
                    this.RangeKey = ((CachePrimitiveWrapper)en.Value).Primitive;
                break;
                }
            }
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("HashKey", new CachePrimitiveWrapper(this.HashKey), typeof(CachePrimitiveWrapper));
            if (this.RangeKey != null)
            {
                info.AddValue("RangeKey", new CachePrimitiveWrapper(this.RangeKey), typeof (CachePrimitiveWrapper));
            }
        }

        #endregion
    }
}
