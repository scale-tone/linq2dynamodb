using System;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Linq2DynamoDb.DataContext.Utils
{
#if AWSSDK_1_5

    internal static class AwsSdk15SupportUtils
    {
        internal static Task ExecuteAsync(this DocumentBatchWrite batch)
        {
            return Task.Factory.FromAsync(batch.BeginExecute, batch.EndExecute, null);
        }

        internal static Task<CreateTableResponse> CreateTableAsync(this Amazon.DynamoDBv2.AmazonDynamoDB client, CreateTableRequest request)
        {
            return Task.Factory.FromAsync(client.BeginCreateTable, (Func<IAsyncResult, CreateTableResponse>) client.EndCreateTable, request, null);
        }

        internal static Task<DeleteTableResponse> DeleteTableAsync(this Amazon.DynamoDBv2.AmazonDynamoDB client, DeleteTableRequest request)
        {
            return Task.Factory.FromAsync(client.BeginDeleteTable, (Func<IAsyncResult, DeleteTableResponse>)client.EndDeleteTable, request, null);
        }
    }

#endif
}
