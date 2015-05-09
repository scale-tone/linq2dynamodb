using System;
using System.Collections;
using System.Collections.Generic;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using Linq2DynamoDb.DataContext;

namespace Linq2DynamoDb.AspNet.DataSource
{
    /// <summary>
    /// A DataSourceView implementation for DynamoDbDataSource.
    /// Instantiates a DataContext and translates CRUD operations to it.
    /// </summary>
    public class DynamoDbDataSourceView : ContextDataSourceView
    {
        public DynamoDbDataSourceView(DynamoDbDataSource owner, HttpContext context) : base(owner, string.Empty, context)
        {
            this._dataSource = owner;
        }

        protected override void HandleValidationErrors(IDictionary<string, Exception> errors, DataSourceOperation operation)
        {
        }

        protected override ContextDataSourceContextData CreateContext(DataSourceOperation operation)
        {
            var tablePropertyInfo = this.ContextType.GetProperty(this.EntitySetName);

            var result = new ContextDataSourceContextData(Activator.CreateInstance(this.ContextType));
            result.EntitySet = tablePropertyInfo.GetValue(result.Context, null);

            return result;
        }

        protected override IEnumerable ExecuteSelect(DataSourceSelectArguments arguments)
        {
            var loadedEntities =  base.ExecuteSelect(arguments);

            if (!this._dataSource.GenerateEmptyRowOnTop)
            {
                return loadedEntities;
            }

            if (arguments.MaximumRows > 0)
            {
                throw new InvalidOperationException("GenerateEmptyRowOnTop is not supported together with paging!");
            }

            return Yield(Activator.CreateInstance(this.EntityType), loadedEntities);
        }

        protected override int DeleteObject(object oldEntity)
        {
            ((ITableCudOperations)this.EntitySet).DeleteEntity(oldEntity);
            return 1;
        }

        protected override int UpdateObject(object oldEntity, object newEntity)
        {
            ((ITableCudOperations)this.EntitySet).UpdateEntity(newEntity, oldEntity);
            return 1;
        }

        protected override int InsertObject(object newEntity)
        {
            ((ITableCudOperations)this.EntitySet).CreateEntity(newEntity);
            return 1;
        }

        private static IEnumerable Yield(object obj, IEnumerable thatEnumerable)
        {
            yield return obj;
            foreach (var item in thatEnumerable)
            {
                yield return item;
            }
        }

        private readonly DynamoDbDataSource _dataSource;
    }
}
