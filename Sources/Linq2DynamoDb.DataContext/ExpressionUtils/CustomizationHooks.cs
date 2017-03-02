using System;

namespace Linq2DynamoDb.DataContext.ExpressionUtils
{
    using Amazon.DynamoDBv2.DocumentModel;

    /// <summary>
    /// Groups properties and callbacks that are used for customizing the GET/QUERY/SCAN operations
    /// </summary>
    internal class CustomizationHooks
    {
        /// <summary>
        /// A custom FilterExpression passed from outside. To be used with QUERY/SCAN operations.
        /// </summary>
        internal Expression CustomFilterExpression { get; set; }

        /// <summary>
        /// A callback for customizing the GET operation params before executing the GET.
        /// </summary>
        internal Action<GetItemOperationConfig> ConfigureGetOperationCallback { get; set; }

        /// <summary>
        /// A callback for customizing the QUERY operation params before executing the QUERY.
        /// </summary>
        internal Action<QueryOperationConfig> ConfigureQueryOperationCallback { get; set; }

        /// <summary>
        /// A callback for customizing the SCAN operation params before executing the SCAN.
        /// </summary>
        internal Action<ScanOperationConfig> ConfigureScanOperationCallback { get; set; }

        /// <summary>
        /// A callback for customizing the Batch GET operation params before executing the Batch GET.
        /// </summary>
        internal Action<DocumentBatchGet> ConfigureBatchGetOperationCallback { get; set; }
    }
}
