using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Services;
using System.Linq;
using System.Reflection;
using Amazon.DynamoDBv2;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// IUpdatable implementation for Linq2DynamoDb.DataContext.
    /// Needed to support WCF Data Services (OData protocol).
    /// </summary>
    public class UpdatableDataContext : DataContext, IUpdatable
    {
        public UpdatableDataContext(IAmazonDynamoDB client, string tableNamePrefix) : base(client, tableNamePrefix)
        {
        }

        public UpdatableDataContext(string tableNamePrefix) : base(tableNamePrefix)
        {
        }

        #region IUpdatable implementation

        object IUpdatable.CreateResource(string containerName, string fullTypeName)
        {
            Type entityType = this.ResolveTypeByName(fullTypeName);
            if (entityType == null)
            {
                return null;
            }

            object resource = Activator.CreateInstance(entityType);

            // if it is an entity - adding it to the table
            var tableWrapper = this.GetTableWrapperForType(containerName, entityType);
            if (tableWrapper != null)
            {
                tableWrapper.AddNewEntity(resource);
            }
            
            return resource;
        }

        object IUpdatable.GetResource(IQueryable query, string fullTypeName)
        {
            // here the GET query is executed
            object resource = query.Cast<object>().Single();

            if ((fullTypeName != null) && (resource.GetType().FullName != fullTypeName))
            {
                throw new Exception(string.Format("Unexpected type {0} for resource", fullTypeName));
            }

            return resource;
        }

        object IUpdatable.ResetResource(object resource)
        {
            // setting all entity's properties to their default values
            foreach (var propInfo in resource.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                object defaultValue = ReflectionUtils.DefaultValue(propInfo.PropertyType)();

                propInfo.SetValue(resource, defaultValue);
            }

            return resource;
        }

        void IUpdatable.SetValue(object targetResource, string propertyName, object propertyValue)
        {
            // this is even faster, than any sophisticated caching 
            var propInfo = targetResource.GetType().GetProperty(propertyName);
            var propType = propInfo.PropertyType;

            // workaround for WCF Data Services trouble - it always passes untyped IEnumerable to propertyValue
            var enumerablePropertyValue = propertyValue as IEnumerable;
            if (enumerablePropertyValue != null)
            {
                Type entityType = null;
                if (propType.IsGenericType)
                {
                    entityType = propType.GetGenericArguments().First();
                }
                else if (propType.IsArray)
                {
                    entityType = propType.GetElementType();
                }

                if
                (
                    (entityType != null)
                    &&
                    (entityType != typeof(string)) // surprise: string also implements IEnumerable<char>
                    &&
                    (propType.ImplementsInterface(typeof(IEnumerable<>)))
                )
                {
                    // creating a List<T> instance
                    var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(entityType));

                    foreach (var value in enumerablePropertyValue)
                    {
                        list.Add(value);
                    }

                    propInfo.SetValue(targetResource, propType.IsArray ? list.ToArray(entityType) : list);

                    return;
                }
            }

            propInfo.SetValue(targetResource, propertyValue);
        }

        object IUpdatable.GetValue(object targetResource, string propertyName)
        {
            // this is even faster, than any sophisticated caching 
            var propInfo = targetResource.GetType().GetProperty(propertyName);
            return propInfo.GetValue(targetResource, null);
        }

        void IUpdatable.SetReference(object targetResource, string propertyName, object propertyValue)
        {
            throw new NotSupportedException();
        }

        void IUpdatable.AddReferenceToCollection(object targetResource, string propertyName, object resourceToBeAdded)
        {
            throw new NotSupportedException();
        }

        void IUpdatable.RemoveReferenceFromCollection(object targetResource, string propertyName, object resourceToBeRemoved)
        {
            throw new NotSupportedException();
        }

        void IUpdatable.DeleteResource(object targetResource)
        {
            Type entityType = targetResource.GetType();

            // There might be multiple TableWrappers created for this entity type. 
            // The only thing we can do is to try to remove the entity from all of them.
            foreach (var pair in this.TableWrappers.Where(pair => pair.Key.Item1 == entityType))
            {
                pair.Value.RemoveEntity(targetResource);
            }
        }

        void IUpdatable.SaveChanges()
        {
            this.SubmitChanges();
        }

        object IUpdatable.ResolveResource(object resource)
        {
            return resource;
        }

        void IUpdatable.ClearChanges()
        {
            foreach (var pair in this.TableWrappers)
            {
                pair.Value.ClearModifications();
            }
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Returns a Type by it's full name
        /// </summary>
        private Type ResolveTypeByName(string fullTypeName)
        {
            // first trying our memoized resolver
            var type = FindTypeInPropertiesFunctor(this.GetType(), fullTypeName);
            if (type != null)
            {
                return type;
            }

            // then trying .Net
            return Type.GetType(fullTypeName);
        }

        protected static readonly Func<Type, string, Type> FindTypeInPropertiesFunctor = ((Func<Type, string, Type>)RecursivelyFindTypeInProperties).Memoize();

        /// <summary>
        /// Recursively iterates through the object tree to resolve a type by it's full name.
        /// This is needed, because the type might be in an assembly, which we know nothing about.
        /// </summary>
        private static Type RecursivelyFindTypeInProperties(Type aggregatingType, string fullTypeName)
        {
            foreach (var prop in aggregatingType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                var propType = prop.PropertyType;

                if (propType.FullName == fullTypeName)
                {
                    return prop.PropertyType;
                }

                if (propType.IsArray)
                {
                    var elementType = propType.GetElementType();
                    if (elementType.FullName == fullTypeName)
                    {
                        return elementType;
                    }

                    var internalType = RecursivelyFindTypeInProperties(elementType, fullTypeName);
                    if (internalType != null)
                    {
                        return internalType;
                    }
                }

                if (propType.IsGenericType)
                {
                    foreach (var genericArgType in propType.GetGenericArguments())
                    {
                        if (genericArgType.FullName == fullTypeName)
                        {
                            return genericArgType;
                        }

                        var internalType = RecursivelyFindTypeInProperties(genericArgType, fullTypeName);
                        if (internalType != null)
                        {
                            return internalType;
                        }
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// Tries to find a pre-created TableDefinitionWrapper for specified entity set and entity type
        /// </summary>
        private TableDefinitionWrapper GetTableWrapperForType(string entitySetName, Type entityType)
        {
            if (string.IsNullOrEmpty(entitySetName))
            {
                return null;
            }

            // Because there might be multiple entity sets defined with the same entity type, 
            // we have to use reflection to get the right one
            Type thisType = this.GetType();

            var entitySetPropInfo = thisType.GetProperty(entitySetName);
            if
            (
                (entitySetPropInfo == null)
                ||
                (entitySetPropInfo.PropertyType.GetGenericTypeDefinition() != typeof(DataTable<>))
            )
            {
                throw new InvalidOperationException(string.Format("{0} does not contain an entity set {1}", thisType.Name, entitySetName));
            }

            if (!entitySetPropInfo.PropertyType.GetGenericArguments().First().IsAssignableFrom(entityType))
            {
                throw new InvalidOperationException(string.Format("{0} contains an entity set {1}, but it's not of type {2}", thisType.Name, entitySetName, entityType.Name));
            }

            var table = (ITableCudOperations)entitySetPropInfo.GetValue(this);
            return table.TableWrapper;
        }

        #endregion
    }
}
