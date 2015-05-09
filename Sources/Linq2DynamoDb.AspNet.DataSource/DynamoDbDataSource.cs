using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.UI.WebControls;

namespace Linq2DynamoDb.AspNet.DataSource
{
    public class DynamoDbDataSource : ContextDataSource
    {
        /// <summary>
        /// The name of table property in the DataContext should be specified
        /// </summary>
        public string TableName
        {
            get { return this.EntitySetName; } 
            set { this.EntitySetName = value; }
        }

        /// <summary>
        /// If set to true, a new empty entity is always added at the top of records, to enable adding new entities
        /// </summary>
        public bool GenerateEmptyRowOnTop { get; set; }

        protected override QueryableDataSourceView CreateQueryableView()
        {
            return new DynamoDbDataSourceView(this, this.Context);
        }
    }
}
