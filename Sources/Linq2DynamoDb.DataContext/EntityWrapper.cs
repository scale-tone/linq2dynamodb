﻿using System;
using System.Linq;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Utils;
using System.Reflection;
using Amazon.DynamoDBv2.DataModel;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// A wrapper for unobtrusive and newly added entities. Implements change tracking and also prohibits key modification
    /// </summary>
    internal class EntityWrapper : IEntityWrapper
    {
        private readonly Func<object, Document> _conversionFunctor; 
        private readonly IEntityKeyGetter _keyGetter;
        private Document _doc, _newDoc;

        private PropertyInfo _entityVersionNumberProperty;
        private bool _hasResolvedEntityVersionNumberProperty;

        /// <summary>
        /// Gets PropertyInfo for the entity's property that has [DynamoDBVersion] 
        /// attribute or returns null if there is none.
        /// </summary>
        private PropertyInfo EntityVersionNumberProperty {
            get {
                if (!_hasResolvedEntityVersionNumberProperty) {
                    _entityVersionNumberProperty = Entity
                        .GetType()
                        .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                        .Where(property =>
                            property
                                .GetCustomAttributes(typeof(DynamoDBVersionAttribute), true)
                                .SingleOrDefault() != null
                        ).SingleOrDefault();

                    _hasResolvedEntityVersionNumberProperty = true;
                }
                return _entityVersionNumberProperty;
            }
        }

        internal EntityWrapper(object entity, Func<object, Document> conversionFunctor, IEntityKeyGetter keyGetter)
        {
            this.Entity = entity;
            this._conversionFunctor = conversionFunctor;
            this._keyGetter = keyGetter;
        }

        internal EntityWrapper(Document doc, Type entityType, Func<object, Document> conversionFunctor, IEntityKeyGetter keyGetter)
        {
            this._doc = doc;
            this.Entity = doc.ToObject(entityType);
            this._conversionFunctor = conversionFunctor;
            this._keyGetter = keyGetter;
        }

        public object Entity { get; private set; }

        internal EntityKey EntityKey { get { return this._keyGetter.GetKey(this.Entity); } }

        /// <summary>
        /// true, if the entity was submitted to server at least once.
        /// </summary>
        internal bool IsCommited { get; private set; }

        public Document AsDocument() {  return this._conversionFunctor(this.Entity); }

        /// <summary>
        /// Returns a new document, if the entity was modified since the last call to this method.
        /// Otherwise returns null.
        /// </summary>
        /// <returns></returns>
        public Document GetDocumentIfDirty()
        {
            this._newDoc = AsDocument();

            if (this._doc == null)
            {
                return this._newDoc;
            }

            bool isDirty = false;
            foreach (var field in this._newDoc)
            {
                // A field might be absent in old document and be set to null in new document.
                // This doesn't mean the value was changed.
                if 
                (
                    (!this._doc.ContainsKey(field.Key)) 
                    &&
                    (!(field.Value is DynamoDBList))
                    &&
                    (!(field.Value is PrimitiveList))
                    &&
                    (field.Value.AsString() == null)
                )
                {
                    continue;
                }

                // if field values are equal
                if 
                (
                    (this._doc.ContainsKey(field.Key)) 
                    && 
                    (field.Value.Equals(this._doc[field.Key]))
                )
                {
                    continue;
                }

                // checking that key fields are not modified after initialization
                if (this._keyGetter.KeyNames.Contains(field.Key))
                {
                    throw new InvalidOperationException("Key properties cannot be edited");
                }

                isDirty = true;
            }

            return isDirty ? this._newDoc : null;
        }

        /// <summary>
        /// Unsets the IsDirty flag. Should never be called without GetDocumentIfDirty()
        /// </summary>
        public void Commit()
        {
            this._doc = this._newDoc;
            this._newDoc = null;
            this.UpdateEntityVersionNumber();
            this.IsCommited = true;
        }

        /// <summary>
        /// Sets the value of the Entity's propety that has the DynamoDBVersionAttribute to 
        /// the value in _doc. When adding/updating the document the version number will
        /// change in the _doc on the way to DynamoDB 
        /// </summary>
        private void UpdateEntityVersionNumber() 
        {
            if (EntityVersionNumberProperty == default(PropertyInfo))
            {
                return;
            }

            EntityVersionNumberProperty.SetValue(
                Entity,
                this._doc[EntityVersionNumberProperty.Name].ToObject(EntityVersionNumberProperty.PropertyType)
            );
        }

        #region Redirecting Equals() and GetHashCode() to the underlying entity

        public override bool Equals(object obj)
        {
            return this.Entity.Equals(obj);
        }

        public override int GetHashCode()
        {
            return this.Entity.GetHashCode();
        }

        #endregion
    }
}
