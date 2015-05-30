using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Expression = System.Linq.Expressions.Expression;

namespace Linq2DynamoDb.DataContext.Utils
{
    public static class DynamoDbConversionUtils
    {
        /// <summary>
        /// Converts a document to the specified type
        /// </summary>
        public static object ToObject(this Document doc, Type entityType)
        {
            var entity = Activator.CreateInstance(entityType);
            // no use in caching the list of properties - tested that
            var entityProps = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance).ToDictionary(pi => pi.Name);

            foreach (var record in doc)
            {
                PropertyInfo propInfo;
                if (!entityProps.TryGetValue(record.Key, out propInfo))
                {
                    continue;
                }

                // AWSSDK converters are also supported
                var converter = DynamoDbPropertyConverter(entityType, propInfo.Name);

                propInfo.SetValue(entity, converter == null ? record.Value.ToObject(propInfo.PropertyType) : converter.FromEntry(record.Value));
            }

            return entity;
        }

        /// <summary>
        /// Converts a value of the specified type to Primitive using AWS SDK's conversion operators
        /// </summary>
        public static DynamoDBEntry ToDynamoDbEntry(this object value, Type valueType)
        {
            return value == null ? null : ToDynamoDbEntryConvertor(valueType)(value);
        }

        /// <summary>
        /// Converts a value of the specified type to Primitive using AWS SDK's conversion operators
        /// </summary>
        public static Primitive ToPrimitive(this object value, Type valueType)
        {
            return value == null ? null : ToPrimitiveConvertor(valueType)(value);
        }

        /// <summary>
        /// Converts a Primitive to a value of specified type using AWS SDK's conversion operators
        /// </summary>
        public static object ToObject(this Primitive value, Type valueType)
        {
            return value == null ? ReflectionUtils.DefaultValue(valueType)() : FromPrimitiveConvertor(valueType)(value);
        }

        /// <summary>
        /// Converts a DynamoDbEntry to a value of specified type using AWS SDK's conversion operators
        /// </summary>
        public static object ToObject(this DynamoDBEntry value, Type valueType)
        {
            return value == null ? ReflectionUtils.DefaultValue(valueType)() : FromDynamoDbEntryConvertor(valueType)(value);
        }

        #region Entity -> Document converter functor

        /// <summary>
        /// Gets a functor, that returns a Document containing all public properties of an object of specified type
        /// </summary>
        public static readonly Func<Type, Func<object, Document>> ToDocumentConverter =
            ((Func<Type, Func<object, Document>>)GetEntityToDocumentConvertorFunctor).Memoize();

        private static Func<object, Document> GetEntityToDocumentConvertorFunctor(Type entityType)
        {
            // creating a functor, that will fill the document with property values
            Action<object, Document> fillDocumentFunc = (entity, doc) => {};

            foreach (var propInfo in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                var capturedPropInfo = propInfo;

                // checking if the property should not be saved to DynamoDb
                if (IsDynamoDbIgnore(entityType, capturedPropInfo.Name))
                {
                    continue;
                }

                // we also support AWS SDK converters
                var converter = DynamoDbPropertyConverter(entityType, capturedPropInfo.Name);
                
                if (converter == null)
                {
                    // using default convertion for the field
                    fillDocumentFunc +=
                        (entity, doc) => doc[capturedPropInfo.Name] =
                            capturedPropInfo.GetValue(entity, null).ToDynamoDbEntry(capturedPropInfo.PropertyType);
                }
                else
                {
                    // using converter
                    fillDocumentFunc +=
                        (entity, doc) => doc[capturedPropInfo.Name] = 
                            converter.ToEntry(capturedPropInfo.GetValue(entity, null));
                }
            }

            return entity =>
            {
                var doc = new Document();
                fillDocumentFunc(entity, doc);
                return doc;
            };
        }

        /// <summary>
        /// Checks if a property is marked with DynamoDbIgnoreAttribute
        /// </summary>
        private static bool IsDynamoDbIgnore(Type entityType, string propertyName)
        {
            var propInfo = entityType.GetProperty(propertyName);

            var propertyAttribute = propInfo.GetCustomAttributes(typeof(DynamoDBIgnoreAttribute), true)
                        .OfType<DynamoDBIgnoreAttribute>()
                        .SingleOrDefault();

            return (propertyAttribute != null);
        }

        #endregion

        #region DynamoDb property converter functor

        public static readonly Func<Type, string, IPropertyConverter> DynamoDbPropertyConverter =
            ((Func<Type, string, IPropertyConverter>)GetPropertyConverter).Memoize();

        private static IPropertyConverter GetPropertyConverter(Type entityType, string propertyName)
        {
            var propInfo = entityType.GetProperty(propertyName);

            var propertyAttribute = propInfo.GetCustomAttributes(typeof(DynamoDBAttribute), true)
                        .OfType<DynamoDBPropertyAttribute>()
                        .SingleOrDefault();

            if
            (
                (propertyAttribute == null)
                ||
                (propertyAttribute.Converter == null)
            )
            {
                return null;
            }

            return Activator.CreateInstance(propertyAttribute.Converter) as IPropertyConverter;
        }

        #endregion
        #region ToPrimitive functor

        /// <summary>
        /// A functor, that returns a functor for converting an object into Primitive
        /// </summary>
        private static readonly Func<Type, Func<object, Primitive>> ToPrimitiveConvertor = 
            ((Func<Type, Func<object, Primitive>>)GetToPrimitiveConversionFunctor).Memoize();

        /// <summary>
        /// Creates a functor for converting an object of the specified type to Primitive
        /// </summary>
        private static Func<object, Primitive> GetToPrimitiveConversionFunctor(Type valueType)
        {
            // parameter, that represents input value
            var valueParameter = Expression.Parameter(typeof(object));

            // first converting to a valueType (or to int, if it's an enum)
            var conversionExp = Expression.Convert(valueParameter, valueType.BaseType == typeof (Enum) ? typeof(int) : valueType);
            // then to Primitive
            conversionExp = Expression.Convert(conversionExp, typeof(Primitive));

            return (Func<object, Primitive>)Expression.Lambda(conversionExp, valueParameter).Compile();
        }

        #endregion

        #region ToDynamoDbEntry functor

        /// <summary>
        /// A functor, that returns a functor for converting an object into DynamoDBEntry
        /// </summary>
        private static readonly Func<Type, Func<object, DynamoDBEntry>> ToDynamoDbEntryConvertor =
            ((Func<Type, Func<object, DynamoDBEntry>>)GetToDynamoDbEntryConversionFunctor).Memoize();

        /// <summary>
        /// Creates a functor for converting an object of the specified type to DynamoDBEntry
        /// </summary>
        private static Func<object, DynamoDBEntry> GetToDynamoDbEntryConversionFunctor(Type valueType)
        {
            // parameter, that represents input value
            var valueParameter = Expression.Parameter(typeof(object));

            if (valueType.IsPrimitive())
            {
                // first converting to a valueType (or to int, if it's an enum)
                var primitiveConversionExp = Expression.Convert(valueParameter, valueType.BaseType == typeof(Enum) ? typeof(int) : valueType);
                // then to Primitive | Since AWSSDK 2.3.2.0, conversion to DynamoDbEntry type now yields object of internal type UnconvertedDynamoDBEntry which is not derived from Primitive thus breaking Linq2DynamoDb
                primitiveConversionExp = Expression.Convert(primitiveConversionExp, typeof(Primitive));

                return (Func<object, DynamoDBEntry>)Expression.Lambda(primitiveConversionExp, valueParameter).Compile();
            }

            // now trying to create a routine for converting a collection

            Type elementType = null;
            if (valueType.IsArray)
            {
                elementType = valueType.GetElementType();
            }
            else if (valueType.ImplementsInterface(typeof (ICollection<>)))
            {
                elementType = valueType.GetGenericArguments()[0];
            }

            if 
            (
                (elementType == null)
                ||
                (!elementType.IsPrimitive())
            )
            {
                throw new InvalidCastException(string.Format("Cannot convert type {0} to DynamoDbEntry", valueType));
            }

            var toPrimitiveListMethodInfo = ((Func<object, Type, PrimitiveList>)ToPrimitiveList).Method;
            var conversionExp = Expression.Call(toPrimitiveListMethodInfo, valueParameter, Expression.Constant(elementType));
            return (Func<object, DynamoDBEntry>)Expression.Lambda(conversionExp, valueParameter).Compile();
        }

        /// <summary>
        /// Converts an ICollection to a PrimitiveList
        /// Used by GetToDynamoDbEntryConversionFunctor()!
        /// </summary>
        private static PrimitiveList ToPrimitiveList(object coll, Type elementType)
        {
            var primitiveList = new PrimitiveList();
            foreach (var item in (ICollection)coll)
            {
                primitiveList.Entries.Add(item.ToPrimitive(elementType));
            }
            return primitiveList;
        }

        #endregion

        #region FromPrimitive functor

        /// <summary>
        /// A functor, that returns a functor for converting a Primitive into an object of specified type
        /// </summary>
        private static readonly Func<Type, Func<Primitive, object>> FromPrimitiveConvertor =
            ((Func<Type, Func<Primitive, object>>)GetFromPrimitiveConversionFunctor).Memoize();

        /// <summary>
        /// Creates a functor for converting a Primitive into an object of the specified type
        /// </summary>
        private static Func<Primitive, object> GetFromPrimitiveConversionFunctor(Type valueType)
        {
            // parameter, that represents input value
            var valueParameter = Expression.Parameter(typeof(Primitive));

            UnaryExpression conversionExp;
            if (valueType.BaseType == typeof (Enum))
            {
                // first to int, then to valueType
                conversionExp = Expression.Convert(valueParameter, typeof(int));
                conversionExp = Expression.Convert(conversionExp, valueType);
            }
            else
            {
                // to valueType
                conversionExp = Expression.Convert(valueParameter, valueType);
            }

            // now converting to object
            conversionExp = Expression.Convert(conversionExp, typeof(object));

            return (Func<Primitive, object>)Expression.Lambda(conversionExp, valueParameter).Compile();
        }

        #endregion

        #region FromDynamoDbEntry functor

        /// <summary>
        /// A functor, that returns a functor for converting a DynamoDBEntry into an object
        /// </summary>
        private static readonly Func<Type, Func<DynamoDBEntry, object>> FromDynamoDbEntryConvertor =
            ((Func<Type, Func<DynamoDBEntry, object>>)GetFromDynamoDbEntryConversionFunctor).Memoize();

        /// <summary>
        /// Creates a functor for converting a DynamoDBEntry to an object of specified type
        /// </summary>
        private static Func<DynamoDBEntry, object> GetFromDynamoDbEntryConversionFunctor(Type valueType)
        {
            // parameter, that represents input value
            var valueParameter = Expression.Parameter(typeof(DynamoDBEntry));

            if (valueType.IsPrimitive())
            {
                Expression conversionExp;

                if (valueType.BaseType == typeof (Enum))
                {
                    conversionExp = Expression.Convert(valueParameter, typeof(int));
                    conversionExp = Expression.Convert(conversionExp, valueType);
                }
                else
                {
                    conversionExp = Expression.Convert(valueParameter, valueType);
                }
                conversionExp = Expression.Convert(conversionExp, typeof(object));

                return (Func<DynamoDBEntry, object>)Expression.Lambda(conversionExp, valueParameter).Compile();
            }

            MethodCallExpression fillListExp;

            if (valueType.IsArray) // supporting array fields as well
            {
                var elementType = valueType.GetElementType();

                fillListExp = Expression.Call
                (
                    typeof(DynamoDbConversionUtils),
                    ((Func<PrimitiveList, Type, object>)FillArrayFromPrimitiveList<object>).Method.Name,
                    new[] { elementType },
                    Expression.Convert(valueParameter, typeof(PrimitiveList)),
                    Expression.Constant(elementType, typeof(Type))
                );
            }
            else
            {
                var elementType = valueType.GetGenericArguments()[0];

                // preferring IList interface, as AWS SDK does (don't know why)
                if (valueType.ImplementsInterface(typeof(IList)))
                {
                    fillListExp = Expression.Call
                    (
                        ((Func<IList, PrimitiveList, Type, object>)FillListFromPrimitiveList).Method,
                        Expression.New(valueType),
                        Expression.Convert(valueParameter, typeof(PrimitiveList)),
                        Expression.Constant(elementType, typeof(Type))
                    );
                }
                else
                {
                    fillListExp = Expression.Call
                    (
                        typeof(DynamoDbConversionUtils),
                        ((Func<ICollection<object>, PrimitiveList, Type, object>)FillCollectionFromPrimitiveList).Method.Name,
                        new[] { elementType },
                        Expression.New(valueType),
                        Expression.Convert(valueParameter, typeof(PrimitiveList)),
                        Expression.Constant(elementType, typeof(Type))
                    );
                }
            }

            return (Func<DynamoDBEntry, object>)Expression.Lambda(fillListExp, valueParameter).Compile();
        }

        /// <summary>
        /// Converts a PrimitiveList into an array of elements of elementType
        /// </summary>
        private static object FillArrayFromPrimitiveList<T>(PrimitiveList primitiveList, Type elementType)
        {
            var result = primitiveList
                .AsListOfPrimitive()
                .Select(pr => pr.ToObject(elementType))
                .Cast<T>()
                .ToArray();

            return result;
        }

        /// <summary>
        /// Fills an IList with contents of PrimitiveList.
        /// Used by GetFromDynamoDbEntryConversionFunctor()!
        /// </summary>
        private static object FillListFromPrimitiveList(IList list, PrimitiveList primitiveList, Type elementType)
        {
            foreach (var primitive in primitiveList.Entries)
            {
                list.Add(primitive.ToObject(elementType));
            }
            return list;
        }

        /// <summary>
        /// Fills an ICollection[T] with contents of PrimitiveList.
        /// Used by GetFromDynamoDbEntryConversionFunctor()!
        /// </summary>
        private static object FillCollectionFromPrimitiveList<T>(ICollection<T> coll, PrimitiveList primitiveList, Type elementType)
        {
            var items = primitiveList.Entries.Select(primitive => primitive.ToObject(elementType)).Cast<T>();
            foreach (var item in items)
            {
                coll.Add(item);
            }
            return coll;
        }

        #endregion

    }
}
