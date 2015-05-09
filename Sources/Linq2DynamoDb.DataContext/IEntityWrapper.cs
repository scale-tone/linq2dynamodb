using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext
{
    public interface IEntityWrapper
    {
        object Entity { get; }
        Document GetDocumentIfDirty();
        void Commit();
    }
}
