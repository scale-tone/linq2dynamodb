using System;
using System.ComponentModel;
using System.Linq.Expressions;
using System.Threading;

namespace MobileNotes.WinPhoneApp.ViewModels
{
    /// <summary>
    /// Base class for all ViewModels
    /// </summary>
    public abstract class BaseViewModel : INotifyPropertyChanged, IDisposable
    {
        #region ctor

        protected SynchronizationContext SyncContext;

        protected BaseViewModel()
        {
            // saving the main thread's context for future use (we assume, that ViewModels are always created in main thread).
            this.SyncContext = SynchronizationContext.Current;
        }

        #endregion

        #region INotifyPropertyChanged

        public event PropertyChangedEventHandler PropertyChanged;

        /// <summary>
        /// A thread-safe implementation of NotifyPropertyChanged
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        protected void NotifyPropertyChanged<T>(Expression<Func<T>> expression)
        {
            var handler = this.PropertyChanged;
            if (handler == null)
            {
                return;
            }

            var body = expression.Body as MemberExpression;

            if (body == null)
            {
                throw new ArgumentException("expression");
            }

            if (this.SyncContext == SynchronizationContext.Current)
            {
                // if we're in main thread - then going on synchronously
                handler(this, new PropertyChangedEventArgs(body.Member.Name));
            }
            else
            {
                // otherwise invoking through sync context
                this.SyncContext.Post(_ => handler(this, new PropertyChangedEventArgs(body.Member.Name)), handler);
            }
        }

        #endregion

        #region IDisposable

        protected volatile bool IsDisposed;

        /// <summary>
        /// Invoked when this object is being removed from the application
        /// and will be subject to garbage collection.
        /// </summary>
        public void Dispose()
        {
            this.IsDisposed = true;

            OnDispose();
        }

        /// <summary>
        /// Child classes can override this method to perform 
        /// clean-up logic, such as removing event handlers.
        /// </summary>
        protected virtual void OnDispose()
        {
        }

        #endregion
    }
}
