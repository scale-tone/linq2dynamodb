using System;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Allows for creating transparent proxies around entities.
    /// Entities should derive from this base class.
    /// </summary>
    public class EntityBase
#if !NETSTANDARD1_6
        : MarshalByRefObject
#endif
    {
        /// <summary>
        /// Internal fake property.
        /// Is used to implement Equals() with transparent proxies
        /// </summary>
// ReSharper disable UnusedAutoPropertyAccessor.Local
        internal object UnderlyingDocument { get; private set; }
// ReSharper restore UnusedAutoPropertyAccessor.Local
    }
}
