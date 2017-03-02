using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Linq2DynamoDb.DataContext.Utils;

#if AWSSDK_1_5
using IAmazonDynamoDB = Amazon.DynamoDBv2.AmazonDynamoDB;
#else
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
#endif

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// A set of async extensions for DataContext
    /// </summary>
    public static class DataContextExtensions
    {
        /// <summary>
        /// Allows to customize the Batch GET operation config before executing the Batch GET.
        /// </summary>
        public static IQueryable<T> ConfigureBatchGetOperation<T>(this IQueryable<T> source, Action<DocumentBatchGet> callback)
        {
            source.AsQuery().UpdateCustomizationHooks(hooks => hooks.ConfigureBatchGetOperationCallback = callback);
            return source;
        }

        /// <summary>
        /// Allows to customize the GET operation config before executing the GET.
        /// </summary>
        public static IQueryable<T> ConfigureGetOperation<T>(this IQueryable<T> source, Action<GetItemOperationConfig> callback)
        {
            source.AsQuery().UpdateCustomizationHooks(hooks => hooks.ConfigureGetOperationCallback = callback);
            return source;
        }

        /// <summary>
        /// Allows to customize the QUERY operation config before executing the QUERY.
        /// </summary>
        public static IQueryable<T> ConfigureQueryOperation<T>(this IQueryable<T> source, Action<QueryOperationConfig> callback)
        {
            source.AsQuery().UpdateCustomizationHooks(hooks => hooks.ConfigureQueryOperationCallback = callback);
            return source;
        }

        /// <summary>
        /// Allows to customize the SCAN operation config before executing the SCAN.
        /// </summary>
        public static IQueryable<T> ConfigureScanOperation<T>(this IQueryable<T> source, Action<ScanOperationConfig> callback)
        {
            source.AsQuery().UpdateCustomizationHooks(hooks => hooks.ConfigureScanOperationCallback = callback);
            return source;
        }

        /// <summary>
        /// Allows to specify custom FilterExpression for DynamoDb queries and scans
        /// </summary>
        public static IQueryable<T> WithFilterExpression<T>(this IQueryable<T> source, Amazon.DynamoDBv2.DocumentModel.Expression expression)
        {
            source.AsQuery().UpdateCustomizationHooks(hooks => hooks.CustomFilterExpression = expression);
            return source;
        }

        /// <summary>
        /// Asynchronously executes the query and returns results as a List
        /// </summary>
        public static Task<List<T>> ToListAsync<T>(this IQueryable<T> source)
        {
            return source.AsQuery().ToListAsync();
        }

        /// <summary>
        /// Asyncronously saves all modifications to DynamoDb and to cache, if one is used
        /// </summary>
        public static async Task SubmitChangesAsync(this DataContext context)
        {
            await Task.WhenAll(context.TableWrappers.Values.Select(t => t.SubmitChangesAsync()));
        }

        /// <summary>
        /// Creates a table with specified capacities, hash and range keys and indexes.
        /// If it doesn't exist yet.
        /// </summary>
        public static void CreateTableIfNotExists<TEntity>(this DataContext context, CreateTableArgs<TEntity> args)
        {
            GeneralUtils.SafelyRunSynchronously(context.CreateTableIfNotExistsAsync, args);
        }

        /// <summary>
        /// Asynchronously Creates a table with specified capacities, hash and range keys and indexes.
        /// If it doesn't exist yet.
        /// </summary>
        public static async Task CreateTableIfNotExistsAsync<TEntity>(this DataContext context, CreateTableArgs<TEntity> args)
        {
            var entityType = typeof(TEntity);
            string tableName = context.GetTableNameForType(entityType);

            // checking if the table exists
            if
            (
                await Task.Run(() =>
                {
                    Table t;
                    return Table.TryLoadTable(context.Client, tableName, out t);
                })
            )
            {
                return;
            }

            context.Log("Table {0} doesn't exist - about to create it...", tableName);

            // creating the table asyncronously
            await context.Client.CreateTableAsync(args.GetCreateTableRequest(tableName));

            context.Log("Waiting for the table {0} to be created...", tableName);

            // waiting till the table is created
            await TillTableIsCreatedAsync(context.Client, tableName);

            context.Log("Table {0} created successfully!", tableName);

            // now adding initial entities
            if (args.GetInitialEntitiesFunc != null)
            {
                context.Log("About to fill table {0} with initial entities...", tableName);

                Exception initialFillException = null;
                try
                {
                    var table = context.GetTable<TEntity>();
                    foreach (var entity in args.GetInitialEntitiesFunc())
                    {
                        table.InsertOnSubmit(entity);
                    }
                    await context.SubmitChangesAsync();

                    context.Log("Table {0} successfully filled with initial entities.", tableName);
                }
                catch (Exception ex)
                {
                    initialFillException = ex;
                }

                // if we failed to add initial data, then removing the table
                if (initialFillException != null)
                {
                    context.Log("An error occured while filling table {0} with initial entities. So, removing the table...", tableName);

                    await DeleteTableAsync<TEntity>(context);

                    context.Log("Table {0} removed.", tableName);

                    throw initialFillException;
                }
            }
        }

        /// <summary>
        /// Deletes a table asynchonously
        /// </summary>
        public static async Task DeleteTableAsync<TEntity>(this DataContext context)
        {
            var entityType = typeof(TEntity);
            string tableName = context.GetTableNameForType(entityType);

            await context.Client.DeleteTableAsync(new DeleteTableRequest { TableName = tableName });

            await TillTableIsDeletedAsync(context.Client, tableName);
        }

        /// <summary>
        /// Deletes a table
        /// </summary>
        public static void DeleteTable<TEntity>(this DataContext context)
        {
            GeneralUtils.SafelyRunSynchronously(context.DeleteTableAsync<TEntity>);
        }

        /// <summary>
        /// Asynchronously waits till a table is created
        /// TODO: add a timeout
        /// </summary>
        private static async Task TillTableIsCreatedAsync(IAmazonDynamoDB client, string tableName)
        {
            string status = string.Empty;
            do
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                try
                {
#if AWSSDK_1_5
                    status = client.DescribeTable
                    (
                        new DescribeTableRequest { TableName = tableName }
                    )
                    .DescribeTableResult.Table.TableStatus;
#else
                    var response = await client.DescribeTableAsync
                    (
                        new DescribeTableRequest { TableName = tableName }
                    );
                    status = response.Table.TableStatus;
#endif
                }
                catch (ResourceNotFoundException)
                {
                }
            }
            while (status != "ACTIVE");
        }

        /// <summary>
        /// Asynchronously waits till a table is deleted
        /// TODO: add a timeout
        /// </summary>
        private static async Task TillTableIsDeletedAsync(IAmazonDynamoDB client, string tableName)
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(1));
                try
                {
                    await client.DescribeTableAsync(new DescribeTableRequest { TableName = tableName });
                }
                catch (ResourceNotFoundException)
                {
                    break;
                }
            }
        }

        private static Query<T> AsQuery<T>(this IQueryable<T> source)
        {
            var query = source as Query<T>;
            if (query == null)
            {
                throw new InvalidOperationException("This extension method works only with Linq2DynamoDB.DataTable. Please, don't try to use it for other IQueryables.");
            }
            return query;
        }
    }
}
