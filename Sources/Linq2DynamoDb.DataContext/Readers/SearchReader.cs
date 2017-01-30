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
        /// Creates a SearchReader instance from DynamoDb get/query/scan result and 
        /// specified projection functor
        /// </summary>
        private IEnumerable CreateReader(object queryResult, Type resultEntityType, Delegate projectionFunc)
        {
            var reader = (ISupervisableEnumerable)Activator.CreateInstance
            (
                typeof(SearchReader<>).MakeGenericType(resultEntityType),
                BindingFlags.Instance | BindingFlags.NonPublic, null,
                new [] { this, queryResult, projectionFunc },
                null
            );

            this.InitReader(reader);
            return reader;
        }

        /// <summary>
        /// Implements enumeration of DynamoDb's search result.
        /// Also supports projections. 
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        private class SearchReader<TEntity> : ReaderBase<TEntity>
        {
            /// <summary>
            /// ctor used for iterating through a search result
            /// </summary>
            private SearchReader(TableDefinitionWrapper table, Search search, Func<Document, TEntity> projectionFunc)
                :
                base(table, projectionFunc)
            {
                this._search = search;
            }

            #region IEnumerator implementation

            public override bool MoveNext()
            {
                bool notFinishedYet = this.SearchResultModeMoveNext();
                if ((!notFinishedYet) && (!this._enumerationFinished))
                {
                    // firing the event only once
                    this._enumerationFinished = true;
                    base.FireEnumerationFinished();
                }
                return notFinishedYet;
            }

            #endregion

            /// <summary>
            /// Iterates through a DynamoDb's search result
            /// </summary>
            /// <returns></returns>
            private bool SearchResultModeMoveNext()
            {
                if
                (
                    (this._currentBatch == null)
                    ||
                    (this._currentBatchIndex >= this._currentBatch.Count)
                )
                {
                    // read though results until we reach the end or get at least one document.
                    do
                    {
                        if (this._search.IsDone)
                        {
                            return false;
                        }
                        this._currentBatch = this._search.GetNextSetAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                    }
                    while (this._currentBatch.Count == 0);

                    this._currentBatchIndex = 0;
                }

                base.SetCurrent(this._currentBatch[this._currentBatchIndex++]);

                return true;
            }

            private readonly Search _search;
            private List<Document> _currentBatch;
            private int _currentBatchIndex;
            private bool _enumerationFinished;
        }
    }
}
