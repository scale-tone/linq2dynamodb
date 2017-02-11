using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    using System.Threading.Tasks;

    /// <summary>
    /// Represents a table in DynamoDb
    /// </summary>
    public class DataTable<TEntity> : Query<TEntity>, ITableCudOperations
#if !NETSTANDARD1_6
        , IListSource
#endif
    {
        private readonly TableDefinitionWrapper _tableWrapper;

        internal DataTable(TableDefinitionWrapper tableWrapper) : base(new QueryProvider(tableWrapper))
        {
            this._tableWrapper = tableWrapper;
        }

        public void InsertOnSubmit(TEntity entity)
        {
            this._tableWrapper.AddNewEntity(entity);
        }

        public void RemoveOnSubmit(TEntity entity)
        {
            this._tableWrapper.RemoveEntity(entity);
        }

        /// <summary>
        /// Returns a single entity by it's keys. Very useful in ASP.Net MVC.
        /// The keys should be passed in the right order: HashKey, then RangeKey (if any)
        /// </summary>
        public TEntity Find(params object[] keyValues)
        {
            var enumerableResult = (IEnumerable<TEntity>)this._tableWrapper.Find(keyValues);
            return enumerableResult.Single();
        }

        /// <summary>
        /// Asyncronously returns a single entity by it's keys. Very useful in ASP.Net MVC.
        /// The keys should be passed in the right order: HashKey, then RangeKey (if any)
        /// </summary>
        public async Task<TEntity> FindAsync(params object[] keyValues)
        {
            var enumerableResult = (IEnumerable<TEntity>) await this._tableWrapper.FindAsync(keyValues);
            return enumerableResult.Single();
        }

        /// <summary>
        /// Acquires a table-wide named lock and returns a disposable object, that represents it.
        /// The cache implementation might throw a NotSupportedException.
        /// </summary>
        public IDisposable AcquireTableLock(string lockKey, TimeSpan lockTimeout)
        {
            return this._tableWrapper.Cache.AcquireTableLock(lockKey, lockTimeout);
        }

        /// <summary>
        /// Acquires a table-wide named lock, which will be repeased automatically after the next submit 
        /// (no matter, if it succeeds or fails).
        /// The cache implementation might throw a NotSupportedException.
        /// </summary>
        public void AcquireTableLockTillSubmit(string lockKey, TimeSpan lockTimeout)
        {
            var tableLock = this._tableWrapper.Cache.AcquireTableLock(lockKey, lockTimeout);
            this._tableWrapper.ThingsToDoUponSubmit += _ => tableLock.Dispose();
        }

#region ITableCudOperations

        void ITableCudOperations.CreateEntity(object entity)
        {
            this._tableWrapper.AddNewEntity(entity);
            GeneralUtils.SafelyRunSynchronously(this._tableWrapper.SubmitChangesAsync);
        }

        void ITableCudOperations.UpdateEntity(object newEntity, object oldEntity)
        {
            this._tableWrapper.AddUpdatedEntity(newEntity, oldEntity);
            GeneralUtils.SafelyRunSynchronously(this._tableWrapper.SubmitChangesAsync);
        }

        void ITableCudOperations.DeleteEntity(object entity)
        {
            this._tableWrapper.RemoveEntity(entity);
            GeneralUtils.SafelyRunSynchronously(this._tableWrapper.SubmitChangesAsync);
        }

        TableDefinitionWrapper ITableCudOperations.TableWrapper
        {
            get { return this._tableWrapper; }
        }

#endregion

#region IListSource implementation
#if !NETSTANDARD1_6
        private EntityBindingList<TEntity> _bindingList;

        public IList GetList()
        {
            if (this._bindingList == null)
            {
                var queryResult = (IEnumerable<TEntity>)this.Provider.ExecuteQuery(((IQueryable<TEntity>)this).Expression);

                // here is the actual query execution happens
                var queryResultList = queryResult.ToList();

                this._bindingList = new EntityBindingList<TEntity>(this, queryResultList);
            }
            return this._bindingList;
        }

        public bool ContainsListCollection { get { return false; } }
#endif
#endregion
    }
}
