#if !NETSTANDARD1_6
using System.Collections.Generic;
using System.ComponentModel;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// What this BindingList's child does - is just handling item's adding and removing
    /// </summary>
    internal class EntityBindingList<TEntity> : BindingList<TEntity>
    {
        private readonly DataTable<TEntity> _table; 

        internal EntityBindingList(DataTable<TEntity> table, IList<TEntity> loadedEntities) : base(loadedEntities) 
        {
            this._table = table;
        }

        protected override object AddNewCore()
        {
            object newEntity = base.AddNewCore();
            this._table.InsertOnSubmit((TEntity)newEntity);
            return newEntity;
        }

        protected override void RemoveItem(int index)
        {
            TEntity removedEntity = this[index];
            base.RemoveItem(index);
            this._table.RemoveOnSubmit(removedEntity);
        }
    }
}
#endif