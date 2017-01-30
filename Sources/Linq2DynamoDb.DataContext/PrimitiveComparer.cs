using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Used for sorting a list of documents by a field of specified type
    /// </summary>
    public class PrimitiveComparer : IComparer<Primitive>
    {
        private readonly Type _fieldType;
        private readonly IComparer _comparer;

        protected PrimitiveComparer(Type fieldType)
        {
            this._fieldType = fieldType;

            // creating a default Comparer<T> instance for the specified field type
            var comparerType = typeof (Comparer<>).MakeGenericType(this._fieldType);
            var defaultPropInfo = comparerType.GetTypeInfo().GetProperty("Default");
            this._comparer = (IComparer)defaultPropInfo.GetValue(null, null);
        }

        protected PrimitiveComparer(Type entityType, string propertyName)
            :
            this(entityType.GetProperty(propertyName).PropertyType)
        {
        }

        public int Compare(Primitive x, Primitive y)
        {
            return this._comparer.Compare(x.ToObject(this._fieldType), y.ToObject(this._fieldType));
        }

        public static IComparer<Primitive> GetComparer(Type entityType, string propertyName)
        {
            return GetPrimitiveComparerForProperty(entityType, propertyName);
        }

        private readonly static Func<Type, string, IComparer<Primitive>> GetPrimitiveComparerForProperty =
            ((Func<Type, string, IComparer<Primitive>>)((t, n) => new PrimitiveComparer(t, n)))
            .Memoize();
    }
}
