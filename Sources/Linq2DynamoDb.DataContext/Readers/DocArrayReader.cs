using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext
{
    public abstract partial class TableDefinitionWrapperBase
    {
        /// <summary>
        /// Creates a DocArrayReader instance from cache result and 
        /// specified projection functor
        /// </summary>
        private IEnumerable CreateDocArrayReader(IEnumerable<Document> docs, Type resultEntityType, Delegate projectionFunc)
        {
            var reader = (ISupervisableEnumerable)Activator.CreateInstance
            (
                typeof(DocArrayReader<>).MakeGenericType(resultEntityType),
                new object[] { this, docs, projectionFunc }
            );

            this.InitReader(reader);
            return reader;
        }

        /// <summary>
        /// Enumerates the array of Documents, converting them into transparent proxies
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        private class DocArrayReader<TEntity> : ReaderBase<TEntity>
        {
            /// <summary>
            /// ctor used for iterating through an array taken from cache
            /// </summary>
            public DocArrayReader(TableDefinitionWrapper table, IEnumerable<Document> docs, Func<Document, TEntity> projectionFunc)
                :
                base(table, projectionFunc)
            {
                this._docsEnumerator = docs.GetEnumerator();
            }

            #region IEnumerator implementation

            public override bool MoveNext()
            {
                if (this._enumerationFinished)
                {
                    return false;
                }

                if (!this._docsEnumerator.MoveNext())
                {
                    this._enumerationFinished = true;
                    // firing the event only once
                    base.FireEnumerationFinished();
                    return false;
                }

                base.SetCurrent(this._docsEnumerator.Current);
                
                return true;
            }

            #endregion

            private readonly IEnumerator<Document> _docsEnumerator;
            private bool _enumerationFinished;
        }
    }
}
