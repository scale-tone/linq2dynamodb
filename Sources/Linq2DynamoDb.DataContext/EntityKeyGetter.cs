using System;
using System.Collections.Generic;
using System.Reflection;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// There're multiple key getter implementations, but they all implement this interface
    /// </summary>
    public interface IEntityKeyGetter
    {
        string[] KeyNames { get; }
        Type HashKeyType { get; }
        EntityKey GetKey(object entity);
        EntityKey GetKey(Document doc);
        IDictionary<string, DynamoDBEntry> GetKeyDictionary(EntityKey key);
    }

    /// <summary>
    /// Implements a quick getting of entity's key. Caches all reflection stuff for that.
    /// </summary>
    internal static class EntityKeyGetter
    {
        internal static IEntityKeyGetter CreateInstance(Table tableDefinition, Type entityType, Type hashKeyType, Primitive predefinedHashKeyValue)
        {
            if (predefinedHashKeyValue != null)
            {
                if (tableDefinition.RangeKeys.Count <= 0)
                {
                    throw new InvalidOperationException("When specifying constant hash key values, the table must always have a range key");
                }

                var rangePropInfo = entityType.GetProperty(tableDefinition.RangeKeys[0], BindingFlags.Instance | BindingFlags.Public);
                if (rangePropInfo == null)
                {
                    throw new InvalidOperationException(string.Format("Entity type {0} doesn't contain range key property {1}", entityType.Name, tableDefinition.RangeKeys[0]));
                }

                return new ConstantHashRangeEntityKeyGetter(tableDefinition.HashKeys[0], hashKeyType, predefinedHashKeyValue, rangePropInfo);
            }
            if (tableDefinition.RangeKeys.Count > 0)
            {
                var hashPropInfo = entityType.GetProperty(tableDefinition.HashKeys[0], BindingFlags.Instance | BindingFlags.Public);
                var rangePropInfo = entityType.GetProperty(tableDefinition.RangeKeys[0], BindingFlags.Instance | BindingFlags.Public);
                
                if (hashPropInfo == null)
                {
                    throw new InvalidOperationException(string.Format("Entity type {0} doesn't contain hash key property {1}", entityType.Name, tableDefinition.HashKeys[0]));
                }
                if (rangePropInfo == null)
                {
                    throw new InvalidOperationException(string.Format("Entity type {0} doesn't contain range key property {1}", entityType.Name, tableDefinition.RangeKeys[0]));
                }

                return new HashRangeEntityKeyGetter(hashPropInfo, rangePropInfo);
            }
            else
            {
                var hashPropInfo = entityType.GetProperty(tableDefinition.HashKeys[0], BindingFlags.Instance | BindingFlags.Public);

                if (hashPropInfo == null)
                {
                    throw new InvalidOperationException(string.Format("Entity type {0} doesn't contain hash key property {1}", entityType.Name, tableDefinition.HashKeys[0]));
                }

                return new HashEntityKeyGetter(hashPropInfo);
            }
        }

        /// <summary>
        /// Extracts a HashKey from an entity
        /// </summary>
        private class HashEntityKeyGetter : IEntityKeyGetter
        {
            private readonly PropertyInfo _hashKeyPropInfo;

            public HashEntityKeyGetter(PropertyInfo hashKeyPropInfo)
            {
                this._hashKeyPropInfo = hashKeyPropInfo;
                this.KeyNames = new []{this._hashKeyPropInfo.Name};
            }

            public string[] KeyNames { get; private set; }

            public Type HashKeyType { get { return this._hashKeyPropInfo.PropertyType; } }
            public EntityKey GetKey(object entity)
            {
                try
                {
                    object hashValue = this._hashKeyPropInfo.GetValue(entity, null);

                    if (hashValue == null)
                    {
                        throw new InvalidOperationException(string.Format("A property {0} representing a HashKey should not be null", this._hashKeyPropInfo.Name));
                    }

                    return new EntityKey(hashValue.ToPrimitive(this._hashKeyPropInfo.PropertyType));
                }
                catch (InvalidOperationException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to get entity key", ex);
                }
            }

            public EntityKey GetKey(Document doc)
            {
                try
                {
                    var hashKeyEntry = doc[this._hashKeyPropInfo.Name];

                    if (hashKeyEntry == null)
                    {
                        throw new InvalidOperationException(string.Format("A property {0} representing a HashKey should not be null", this._hashKeyPropInfo.Name));
                    }

                    return new EntityKey(hashKeyEntry.AsPrimitive());
                }
                catch (InvalidOperationException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to get entity key", ex);
                }
            }

            public IDictionary<string, DynamoDBEntry> GetKeyDictionary(EntityKey key)
            {
                return new Dictionary<string, DynamoDBEntry>
                {
                    {this._hashKeyPropInfo.Name, key.HashKey}
                };
            }
        }

        /// <summary>
        /// Extracts a HashKey-RangeKey pair from an entity
        /// </summary>
        private class HashRangeEntityKeyGetter : IEntityKeyGetter
        {
            private readonly PropertyInfo _hashKeyPropInfo;
            private readonly PropertyInfo _rangeKeyPropInfo;

            public HashRangeEntityKeyGetter(PropertyInfo hashKeyPropInfo, PropertyInfo rangeKeyPropInfo)
            {
                this._hashKeyPropInfo = hashKeyPropInfo;
                this._rangeKeyPropInfo = rangeKeyPropInfo;
                this.KeyNames = new[] { this._hashKeyPropInfo.Name, this._rangeKeyPropInfo.Name };
            }

            public string[] KeyNames { get; private set; }

            public Type HashKeyType { get { return this._hashKeyPropInfo.PropertyType; } }

            public EntityKey GetKey(object entity)
            {
                try
                {
                    object hashValue = this._hashKeyPropInfo.GetValue(entity, null);

                if (hashValue == null)
                {
                    throw new InvalidOperationException(string.Format("A property {0} representing a HashKey should not be null", this._hashKeyPropInfo.Name));
                }

                    object rangeValue = this._rangeKeyPropInfo.GetValue(entity, null);

                    if (rangeValue == null)
                    {
                        throw new InvalidOperationException(string.Format("A property {0} representing a RangeKey should not be null", this._rangeKeyPropInfo.Name));
                    }

                    return new EntityKey
                    (
                        hashValue.ToPrimitive(this._hashKeyPropInfo.PropertyType),
                        rangeValue.ToPrimitive(this._rangeKeyPropInfo.PropertyType)
                    );
                }
                catch (InvalidOperationException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to get entity key", ex);
                }
            }

            public EntityKey GetKey(Document doc)
            {
                try
                {
                    var hashKeyEntry = doc[this._hashKeyPropInfo.Name];

                    if (hashKeyEntry == null)
                    {
                        throw new InvalidOperationException(string.Format("A property {0} representing a HashKey should not be null", this._hashKeyPropInfo.Name));
                    }

                    var rangeKeyEntry = doc[this._rangeKeyPropInfo.Name];

                    if (rangeKeyEntry == null)
                    {
                        throw new InvalidOperationException(string.Format("A property {0} representing a RangeKey should not be null", this._rangeKeyPropInfo.Name));
                    }

                    return new EntityKey(hashKeyEntry.AsPrimitive(), rangeKeyEntry.AsPrimitive());
                }
                catch (InvalidOperationException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to get entity key", ex);
                }
            }

            public IDictionary<string, DynamoDBEntry> GetKeyDictionary(EntityKey key)
            {
                return new Dictionary<string, DynamoDBEntry>
                {
                    {this._hashKeyPropInfo.Name, key.HashKey},
                    {this._rangeKeyPropInfo.Name, key.RangeKey}
                };
            }
        }

        /// <summary>
        /// Takes a predefined HashKey and combines it with RangeKey extracted from an entity
        /// </summary>
        private class ConstantHashRangeEntityKeyGetter : IEntityKeyGetter
        {
            private readonly string _hashKeyName;
            private readonly Type _hashKeyType;
            private readonly Primitive _hashKeyValue;

            private readonly PropertyInfo _rangeKeyPropInfo;

            public ConstantHashRangeEntityKeyGetter(string hashKeyName, Type hashKeyType, Primitive hashKeyValue, PropertyInfo rangeKeyPropInfo)
            {
                this._hashKeyName = hashKeyName;
                this._hashKeyType = hashKeyType;
                this._hashKeyValue = hashKeyValue;
                this._rangeKeyPropInfo = rangeKeyPropInfo;

                this.KeyNames = new[] { this._hashKeyName, this._rangeKeyPropInfo.Name };
            }

            public string[] KeyNames { get; private set; }

            public Type HashKeyType { get { return this._hashKeyType; } }

            public EntityKey GetKey(object entity)
            {
                try
                {
                    object rangeValue = this._rangeKeyPropInfo.GetValue(entity, null);

                    if (rangeValue == null)
                    {
                        throw new InvalidOperationException(string.Format("A property {0} representing a RangeKey should not be null", this._rangeKeyPropInfo.Name));
                    }

                    return new EntityKey
                        (
                            this._hashKeyValue,
                            rangeValue.ToPrimitive(this._rangeKeyPropInfo.PropertyType)
                        ); 
                }
                catch (InvalidOperationException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to get entity key", ex);
                }
            }

            public EntityKey GetKey(Document doc)
            {
                try
                {
                    var rangeKeyEntry = doc[this._rangeKeyPropInfo.Name];

                    if (rangeKeyEntry == null)
                    {
                        throw new InvalidOperationException(string.Format("A property {0} representing a RangeKey should not be null", this._rangeKeyPropInfo.Name));
                    }

                    return new EntityKey(this._hashKeyValue, rangeKeyEntry.AsPrimitive());
                }
                catch (InvalidOperationException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException("Failed to get entity key", ex);
                }
            }

            public IDictionary<string, DynamoDBEntry> GetKeyDictionary(EntityKey key)
            {
                return new Dictionary<string, DynamoDBEntry>
                {
                    {this._hashKeyName, key.HashKey},
                    {this._rangeKeyPropInfo.Name, key.RangeKey}
                };
            }
        }
    }
}
