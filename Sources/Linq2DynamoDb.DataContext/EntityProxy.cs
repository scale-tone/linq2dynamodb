#if !NETSTANDARD1_6
using System;
using System.Linq;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Proxies;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Implements a remoting proxy around a Document object.
    /// Property reads and writes are redirected to the underlying Document.
    /// Property values are converted on demand, and also this allows us to use
    /// Document's internal self-tracking mechanism.
    /// </summary>
    internal class EntityProxy : RealProxy, IEntityWrapper
    {
        private readonly Type _entityType;
        private readonly string[] _keyNames;
        private readonly Document _document;

        public EntityProxy(Document doc, Type entityType, string[] keyNames) : base(entityType)
        {
            this._document = doc;
            this._entityType = entityType;
            this._keyNames = keyNames;
        }

        public override IMessage Invoke(IMessage msg)
        {
            var methodCall = (IMethodCallMessage)msg;
            var methodInfo = (MethodInfo)methodCall.MethodBase;

            object result;

            // Black magic of implementing Equals() goes here
            if (methodCall.MethodName == "get_UnderlyingDocument")
            {
                return new ReturnMessage(this._document, null, 0, methodCall.LogicalCallContext, methodCall);
            }
            if (methodCall.MethodName == "Equals")
            {
                // Equals() calls are redirected to the underlying Document
                result = false;

                var that = methodCall.InArgs[0] as EntityBase;
                if (that != null)
                {
                    var thatDocument = that.UnderlyingDocument;

                    result = this._document.Equals(thatDocument);
                }

                return new ReturnMessage(result, null, 0, methodCall.LogicalCallContext, methodCall);
            }
            
            // redirecting all property touches to the underlying Document
            if (methodCall.MethodName.StartsWith("get_"))
            {
                string propertyName = methodCall.MethodName.Substring(4);

                if (methodInfo.ReturnParameter == null)
                {
                    throw new InvalidOperationException(string.Format("Getter for property {0} should have a return value!", propertyName));
                }

                try
                {
                    DynamoDBEntry entry;
                    this._document.TryGetValue(propertyName, out entry);

                    // we also support AWS SDK convertors
                    var converter = DynamoDbConversionUtils.DynamoDbPropertyConverter(this._entityType, propertyName);
                    result = converter == null 
                        ? 
                        entry.ToObject(methodInfo.ReturnParameter.ParameterType) 
                        : 
                        converter.FromEntry(entry);

                    return new ReturnMessage(result, null, 0, methodCall.LogicalCallContext, methodCall);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(string.Format("Failed to get property {0}", propertyName), ex);
                }
            }
            if (methodCall.MethodName.StartsWith("set_"))
            {
                string propertyName = methodCall.MethodName.Substring(4);

                if (this._keyNames.Contains(propertyName))
                {
                    throw new InvalidOperationException("Key properties cannot be edited");
                }

                try
                {
                    // we also support AWS SDK convertors
                    var converter = DynamoDbConversionUtils.DynamoDbPropertyConverter(this._entityType, propertyName);

                    this._document[propertyName] = converter == null
                        ?
                        methodCall.InArgs[0].ToDynamoDbEntry(methodInfo.GetParameters()[0].ParameterType)
                        :
                        converter.ToEntry(methodCall.InArgs[0]);

                    return new ReturnMessage(null, null, 0, methodCall.LogicalCallContext, methodCall);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(string.Format("Failed to set property {0}", propertyName), ex);
                }
            }
            
            if (methodCall.MethodName == "GetType")
            {
                return new ReturnMessage(this._entityType, null, 0, methodCall.LogicalCallContext, methodCall);
            }
            
            // Redirecting the rest of calls to Document as well, just in case
            result = methodInfo.Invoke(this._document, methodCall.InArgs);
            return new ReturnMessage(result, null, 0, methodCall.LogicalCallContext, methodCall);
        }

        public Document GetDocumentIfDirty()
        {
            return this._document.IsDirty() ? this._document : null;
        }

        public object Entity { get { return this.GetTransparentProxy(); } }

        public void Commit() {}
    }
}
#endif