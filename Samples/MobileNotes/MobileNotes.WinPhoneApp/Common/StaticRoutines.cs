using System;

namespace MobileNotes.WinPhoneApp.Common
{
    public static class StaticRoutines
    {
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
        public static void FireSafely<T>(this Action<T> handler, Func<T> paramFactory1)
        {
            if (handler != null)
            {
                handler(paramFactory1());
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
        public static void FireSafely<T1, T2>(this Action<T1, T2> handler, Func<T1> paramFactory1, Func<T2> paramFactory2)
        {
            if (handler != null)
            {
                handler(paramFactory1(), paramFactory2());
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
    }
}
