using System;
using System.Collections;
using System.Collections.Generic;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    using System.Reflection;

    /// <summary>
    /// Base class for all readers
    /// </summary>
    internal abstract class ReaderBase<TEntity> : ISupervisableEnumerable, IEnumerable<TEntity>, IEnumerator<TEntity>
    {
        protected ReaderBase(TableDefinitionWrapper table, Func<Document, TEntity> projectionFunc)
        {
            this.Table = table;
            this.ProjectionFunc = projectionFunc;

#if !NETSTANDARD1_6
            // choosing a wrapper for the entity depending on whether it's inherited from EntityBase
            if (typeof (EntityBase).IsAssignableFrom(this.EntityType))
            {
                // then using a TransparentProxy
                this._entityWrapperCreator = doc => new EntityProxy(doc, this.EntityType, this.Table.KeyNames);
                return;
            }
#endif
            // unobtrusive mode - using EntityWrapper
            this._entityWrapperCreator = doc => new EntityWrapper(doc, this.EntityType, this.Table.ToDocumentConversionFunctor, this.Table.EntityKeyGetter);
        }

#region ISupervisableEnumerable implementation

        public event Action<Document, IEntityWrapper> EntityDocumentEnumerated;
        public event Action EnumerationFinished;

        protected void FireEnumerationFinished()
        {
            this.EnumerationFinished.FireSafely();
        }

#endregion

#region IEnumerable implementation

        public IEnumerator<TEntity> GetEnumerator()
        {
            if (this._isEnumerated)
            {
                throw new InvalidOperationException("Multiple enumeration is not supported");
            }
            this._isEnumerated = true;
            return this;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

#endregion

#region IEnumerator implementation

        public TEntity Current { get; protected set; }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        public abstract bool MoveNext();

        public void Reset() {}
        public void Dispose() {}

#endregion

        /// <summary>
        /// Chooses a proper wrapper around the document.
        /// Based on entity type, it might be an EntityProxy or EntityWrapper.
        /// Also sets the Current property.
        /// </summary>
        protected void SetCurrent(Document doc)
        {
            if (this.ProjectionFunc == null) // if no projection func is specified
            {
                // then choosing a wrapper and registering it in the list of loaded entities
                var wrapper = this._entityWrapperCreator(doc);
                this.EntityDocumentEnumerated.FireSafely(doc, wrapper);
                this.Current = (TEntity)wrapper.Entity;
            }
            else
            {
                // Otherwise creating a read-only anonymous object from the document.
                // There's no need to register projected documents in the context, as they're read-only by nature

                this.EntityDocumentEnumerated.FireSafely(doc, null);
                this.Current = this.ProjectionFunc(doc);
            }
        }


        protected TableDefinitionWrapper Table;
        protected readonly Type EntityType = typeof(TEntity);
        protected Func<Document, TEntity> ProjectionFunc;

        private bool _isEnumerated;

        private readonly Func<Document, IEntityWrapper> _entityWrapperCreator;
    }
}