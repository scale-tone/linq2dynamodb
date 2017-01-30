using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Linq2DynamoDb.DataContext.Utils
{
    public static class ReflectionUtils
    {
        /// <summary>
        /// Creates a typed array from an untyped IList
        /// </summary>
        public static object ToArray(this IList list, Type elementType)
        {
            var result = Array.CreateInstance(elementType, list.Count);
            for (int i = 0; i < list.Count; i++)
            {
                result.SetValue(list[i], i);
            }

            return result;
        }

        #region default(T) functor

        /// <summary>
        /// A functor, that returns a functor, that returns a default value of Type
        /// </summary>
        public static readonly Func<Type, Func<object>> DefaultValue = ((Func<Type, Func<object>>)GetDefaultValueFunctor).Memoize();

        /// <summary>
        /// Creates a functor for returning a default value of valueType
        /// </summary>
        private static Func<object> GetDefaultValueFunctor(Type valueType)
        {
            var exp = Expression.Convert(Expression.Default(valueType), typeof(object));
            return (Func<object>)Expression.Lambda(exp).Compile();
        }

        #endregion

        /// <summary>
        /// Counts elements in an IEnumerable
        /// </summary>
        public static int Count(this IEnumerable thatEnumerable, Type entityType)
        {
            var countMethodInfo = GetCountMethodInfoFunctor(entityType);
            return (int)countMethodInfo.Invoke(null, new object[]{thatEnumerable});
        }

        #region Functors for Count

        private static readonly Func<Type, MethodInfo> GetCountMethodInfoFunctor = ((Func<Type, MethodInfo>)GetCountMethodInfo).Memoize();
        private static MethodInfo GetCountMethodInfo(Type entityType)
        {
            // this was the only way to match the right method
            var countMethodInfo =
            (
                from mi in typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                let paramInfos = mi.GetParameters()
                where
                    (mi.Name == "Count")
                    &&
                    (paramInfos.Length == 1)
                    &&
                    (paramInfos[0].ParameterType.Name == "IEnumerable`1")
                select mi
             )
             .Single();

            return countMethodInfo.MakeGenericMethod(entityType);
        }

        #endregion

        /// <summary>
        /// Sorts an IEnumerable by some field
        /// </summary>
        public static IEnumerable OrderBy(this IEnumerable thatEnumerable, Type entityType, string orderByFieldName, bool orderByDesc)
        {
            var orderByMethodInfo = GetOrderByMethodInfoFunctor(entityType, orderByFieldName, orderByDesc);
            var keySelector = GetKeySelectorFunctor(entityType, orderByFieldName);

            return (IEnumerable)orderByMethodInfo.Invoke(null, new object[]{ thatEnumerable, keySelector});
        }

        #region Functors for OrderBy

        private static readonly Func<Type, string, bool, MethodInfo> GetOrderByMethodInfoFunctor = ((Func<Type, string, bool, MethodInfo>)GetOrderByMethodInfo).Memoize();
        private static MethodInfo GetOrderByMethodInfo(Type entityType, string orderByFieldName, bool orderByDesc)
        {
            // this was the only way to match the right method
            var orderByMethodInfo =
            (
                from mi in typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                let paramInfos = mi.GetParameters()
                where
                    (mi.Name == (orderByDesc ? "OrderByDescending" : "OrderBy"))
                    &&
                    (paramInfos.Length == 2)
                    &&
                    (paramInfos[0].ParameterType.Name == "IEnumerable`1")
                    &&
                    (paramInfos[1].ParameterType.Name == "Func`2")
                select mi
             )
             .Single();

            var propInfo = entityType.GetProperty(orderByFieldName);
            if (propInfo == null) // if OrderBy() method is called for a collection of primitive types
            {
                return orderByMethodInfo.MakeGenericMethod(entityType, entityType);
            }
            return orderByMethodInfo.MakeGenericMethod(entityType, propInfo.PropertyType);
        }

        private static readonly Func<Type, string, Delegate> GetKeySelectorFunctor = ((Func<Type, string, Delegate>)GetKeySelector).Memoize();
        private static Delegate GetKeySelector(Type entityType, string orderByFieldName)
        {
            var entityParam = Expression.Parameter(entityType);

            var propInfo = entityType.GetProperty(orderByFieldName);
            if (propInfo == null) // if OrderBy() method is called for a collection of primitive types
            {
                // returning a lambda like this: s => s
                return Expression.Lambda(entityParam, entityParam).Compile();
            }

            var propExp = Expression.Property(entityParam, orderByFieldName);
            return Expression.Lambda(propExp, entityParam).Compile();
        }

        #endregion

        /// <summary>
        /// Gets the type of element in an IEnumerable[T] by using some reflection magic
        /// </summary>
        public static Type GetElementType(Type collectionType)
        {
            Type iEnumerableType = FindIEnumerable(collectionType);
            if (iEnumerableType == null)
            {
                return collectionType;
            }
            return iEnumerableType.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type collectionType)
        {
            if (collectionType == null || collectionType == typeof(string))
            {
                return null;
            }

            if (collectionType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(collectionType.GetElementType());
            }

            var typeInfo = collectionType.GetTypeInfo();
            if (typeInfo.IsGenericType)
            {
                foreach
                (
                    var iEnumerableType in collectionType.GetGenericArguments()
                        .Select(genericArg => typeof(IEnumerable<>).MakeGenericType(genericArg))
                        .Where(iEnumerableType => iEnumerableType.IsAssignableFrom(collectionType))
                )
                {
                    return iEnumerableType;
                }
            }

            foreach
            (
                var iEnumerableType in collectionType
                    .GetInterfaces()
                    .Select(FindIEnumerable)
                    .Where(iEnumerableType => iEnumerableType != null)
            )
            {
                return iEnumerableType;
            }

            if ((typeInfo.BaseType != null) && (typeInfo.BaseType != typeof(object)))
            {
                return FindIEnumerable(typeInfo.BaseType);
            }

            return null;
        }

        public static bool ImplementsInterface(this Type targetType, Type interfaceType)
        {
            var typeInfo = interfaceType.GetTypeInfo();
            if (!typeInfo.IsInterface)
            {
                throw new ArgumentOutOfRangeException("interfaceType", "Type is not an interface");
            }

            foreach (var inter in targetType.GetInterfaces())
            {
                if (inter == interfaceType)
                {
                    return true;
                }

                var interTypeInfo = inter.GetTypeInfo();
                if (!typeInfo.IsGenericTypeDefinition || !interTypeInfo.IsGenericType)
                {
                    continue;
                }

                if (interTypeInfo.GetGenericTypeDefinition() == interfaceType)
                {
                    return true;
                }
            }
            return false;
        }

        public static bool IsPrimitive(this Type type)
        {
            return 
            (
                type.IsAssignableFrom(typeof(Boolean)) ||
                type.IsAssignableFrom(typeof(Byte)) ||
                type.IsAssignableFrom(typeof(Char)) ||
                type.IsAssignableFrom(typeof(DateTime)) ||
                type.IsAssignableFrom(typeof(Decimal)) ||
                type.IsAssignableFrom(typeof(Double)) ||
                type.IsAssignableFrom(typeof(int)) ||
                type.IsAssignableFrom(typeof(long)) ||
                type.IsAssignableFrom(typeof(SByte)) ||
                type.IsAssignableFrom(typeof(short)) ||
                type.IsAssignableFrom(typeof(Single)) ||
                type.IsAssignableFrom(typeof(String)) ||
                type.IsAssignableFrom(typeof(uint)) ||
                type.IsAssignableFrom(typeof(ulong)) ||
                type.IsAssignableFrom(typeof(ushort)) ||
                type.IsAssignableFrom(typeof(Guid)) ||
                type.IsAssignableFrom(typeof(byte[])) ||
                type.IsAssignableFrom(typeof(MemoryStream)) ||
                type.GetTypeInfo().BaseType == typeof(Enum)
            );
        }

        public static string ToAttributeType(this Type type)
        {
            if 
            (
                (!type.IsPrimitive())
                ||
                (type == typeof(byte[]))
                ||
                (type == typeof(MemoryStream))
            )
            {
                throw new NotSupportedException(string.Format("{0} is not a primitive type", type));
            }

            return
            (
                (type == typeof(string))
                ||
                (type == typeof(DateTime))
                ||
                (type == typeof(Guid))
                ||
                (type == typeof(Char))
            )
            ?
            "S" : "N";
        }
    }
}
