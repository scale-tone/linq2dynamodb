
namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// This untyped interface is used by DynamoDbDataSource control for ASP.Net to make single-entity updates.
    /// And this is the only untyped way to operate with DataTable[T]
    /// </summary>    
    public interface ITableCudOperations
    {
        void CreateEntity(object entity);
        void UpdateEntity(object newEntity, object oldEntity);
        void DeleteEntity(object entity);

        /// <summary>
        /// Used in UpdatableDataContext to access the underlying TableDefinitionWrapper
        /// </summary>
        TableDefinitionWrapper TableWrapper { get; }
    }
}
