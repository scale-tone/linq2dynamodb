using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Linq2DynamoDb.DataContext.Utils
{
    public static class GeneralUtils
    {
        /// <summary>
        /// Safely runs an async method synchronously
        /// </summary>
        public static void SafelyRunSynchronously(this Func<Task> asyncMethod)
        {
            // Wrapping awaitable method's call with another task, because otherwise
            // the calling thread might get blocked by await's implementation
            Task.Factory.StartNew
            (
                () => asyncMethod().Wait(),
                TaskCreationOptions.LongRunning // this is crucial, as we need to tell TPL not to try executing the task body synchronously
            )
            .Wait();
        }

        /// <summary>
        /// Safely runs an async method synchronously
        /// </summary>
        public static void SafelyRunSynchronously<TParam>(this Func<TParam, Task> asyncMethod, TParam param)
        {
            // Wrapping awaitable method's call with another task, because otherwise
            // the calling thread might get blocked by await's implementation
            Task.Factory.StartNew
            (
                () => asyncMethod(param).Wait(),
                TaskCreationOptions.LongRunning // this is crucial, as we need to tell TPL not to try executing the task body synchronously
            )
            .Wait();
        }

        /// <summary>
        /// Gracefully fires an event
        /// </summary>
        public static void FireSafely(this Action handler)
        {
            if (handler != null)
            {
                handler();
            }
        }

        /// <summary>
        /// Gracefully fires an event
        /// </summary>
        public static void FireSafely<T>(this Action<T> handler, T param1)
        {
            if (handler != null)
            {
                handler(param1);
            }
        }

        /// <summary>
        /// Gracefully fires an event
        /// </summary>
        public static void FireSafely<T1, T2>(this Action<T1, T2> handler, T1 param1, T2 param2)
        {
            if (handler != null)
            {
                handler(param1, param2);
            }
        }

        /// <summary>
        /// Gracefully fires an event
        /// </summary>
        public static void FireSafely<T1, T2, T3>(this Action<T1, T2, T3> handler, T1 param1, T2 param2, T3 param3)
        {
            if (handler != null)
            {
                handler(param1, param2, param3);
            }
        }

        /// <summary>
        /// String to Base64 string
        /// </summary>
        public static string ToBase64(this string value)
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(value));
        }

        /// <summary>
        /// String from Base64 string
        /// </summary>
        public static string FromBase64(this string value)
        {
            return Encoding.UTF8.GetString(Convert.FromBase64String(value));
        }

        /// <summary>
        /// Implements memoization 
        /// </summary>
        public static Func<TKey, TResult> Memoize<TKey, TResult>(this Func<TKey, TResult> func)
        {
            var cache = new ConcurrentDictionary<TKey, TResult>();
            return key =>
            {
                Debug.Assert(ObjectOverridesGetHashCodeAndEquals(key));

                return cache.GetOrAdd(key, func);
            };
        }

        /// <summary>
        /// Implements memoization 
        /// </summary>
        public static Func<T1, T2, TResult> Memoize<T1, T2, TResult>(this Func<T1, T2, TResult> func)
        {
            var memoizedFunc = new Func<Tuple<T1, T2>, TResult>(tuple => func(tuple.Item1, tuple.Item2)).Memoize();
            return (t1, t2) =>
            {
                Debug.Assert(ObjectOverridesGetHashCodeAndEquals(t1));
                Debug.Assert(ObjectOverridesGetHashCodeAndEquals(t2));

                return memoizedFunc(new Tuple<T1, T2>(t1, t2));
            };
        }

        /// <summary>
        /// Implements memoization 
        /// </summary>
        public static Func<T1, T2, T3, TResult> Memoize<T1, T2, T3, TResult>(this Func<T1, T2, T3, TResult> func)
        {
            var memoizedFunc = new Func<Tuple<T1, T2, T3>, TResult>(tuple => func(tuple.Item1, tuple.Item2, tuple.Item3)).Memoize();
            return (t1, t2, t3) =>
            {
                Debug.Assert(ObjectOverridesGetHashCodeAndEquals(t1));
                Debug.Assert(ObjectOverridesGetHashCodeAndEquals(t2));
                Debug.Assert(ObjectOverridesGetHashCodeAndEquals(t3));

                return memoizedFunc(new Tuple<T1, T2, T3>(t1, t2, t3));
            };
        }

        /// <summary>
        /// Checks, that an object overrides GetHashCode() and Equals(), which is crucial for memoization to work
        /// </summary>
        private static bool ObjectOverridesGetHashCodeAndEquals(object obj)
        {
            return
            (
                (((Func<int>)obj.GetHashCode).Method != ObjectGetHashCode)
                &&
                (((Predicate<object>)obj.Equals).Method != ObjectEquals)
            );
        }
        private static readonly MethodInfo ObjectGetHashCode = ((Func<int>)new object().GetHashCode).Method;
        private static readonly MethodInfo ObjectEquals = ((Predicate<object>)new object().Equals).Method;

    }
}
