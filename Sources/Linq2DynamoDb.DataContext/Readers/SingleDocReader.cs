using System;
using System.Collections;
using System.Reflection;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext
{
    public abstract partial class TableDefinitionWrapperBase
    {
        /// <summary>
        /// Creates an enumerator for a single entity or an empty enumerator
        /// </summary>
        private IEnumerable CreateSingleDocReader(Document doc, Type resultEntityType, Delegate projectionFunc)
        {
            var reader = (ISupervisableEnumerable)Activator.CreateInstance
            (
                typeof(SingleDocReader<>).MakeGenericType(resultEntityType),
                new object[] { this, doc, projectionFunc }
            );

            this.InitReader(reader);
            return reader;
        }

        /// <summary>
        /// Enumerates a single Document.
        /// Also supports projections.
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        private class SingleDocReader<TEntity> : ReaderBase<TEntity>
        {
            /// <summary>
            /// ctor, that iterates through a single document
            /// </summary>
            public SingleDocReader(TableDefinitionWrapper table, Document singleDoc, Func<Document, TEntity> projectionFunc)
                :
                base(table, projectionFunc)
            {
                this._singleDoc = singleDoc;
            }

            #region IEnumerator implementation

            public override bool MoveNext()
            {
                if (this._singleDoc == null)
                {
                    if (!this._enumerationFinished)
                    {
                        this._enumerationFinished = true;
                        base.FireEnumerationFinished();
                    }
                    return false;
                }

                base.SetCurrent(this._singleDoc);

                this._singleDoc = null;
                return true;
            }

            #endregion

            private Document _singleDoc;
            private bool _enumerationFinished;
        }
    }
}
