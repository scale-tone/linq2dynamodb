using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Linq2DynamoDb.DataContext.Caching;
using Linq2DynamoDb.DataContext.ExpressionUtils;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Implements entity creating, updating and deleting
    /// </summary>
    public class TableDefinitionWrapper : TableDefinitionWrapperBase
    {
        #region ctor

        internal TableDefinitionWrapper(Table tableDefinition, Type tableEntityType, object hashKeyValue, ITableCache cacheImplementation, bool consistentRead) 
            : 
            base(tableDefinition, tableEntityType, hashKeyValue, cacheImplementation, consistentRead)
        {
            if (this.HashKeyValue == null)
            {
                this.ToDocumentConversionFunctor = DynamoDbConversionUtils.ToDocumentConverter(this.TableEntityType);
            }
            else
            {
                var converter = DynamoDbConversionUtils.ToDocumentConverter(this.TableEntityType);
                // adding a step for filling in the predefined HashKey value
                this.ToDocumentConversionFunctor = entity =>
                    {
                        var doc = converter(entity);
                        doc[this.TableDefinition.HashKeys[0]] = this.HashKeyValue;
                        return doc;
                    };
            }
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Registers an added entity
        /// </summary>
        public void AddNewEntity(object newEntity)
        {
            this._addedEntities.Add(new EntityWrapper(newEntity, this.ToDocumentConversionFunctor, this.EntityKeyGetter));
        }

        /// <summary>
        /// Registers a removed entity
        /// </summary>
        public void RemoveEntity(object removedEntity)
        {
            EntityKey removedKey = null;
            if (removedEntity != null)
            {
                try
                {
                    removedKey = this.EntityKeyGetter.GetKey(removedEntity);
                }
                catch (Exception)
                {
                    removedKey = null;
                }
            }

            if (removedKey == null)
            {
                // just skipping entities with no key specified
                return;
            }

            // the process of filling an index in cache should be cancelled
            this.CurrentIndexCreator = null;

            // checking if this is a newly added entity
            int i = 0;
            while (i < this._addedEntities.Count)
            {
                var addedEntityWrapper = this._addedEntities[i];
                if (ReferenceEquals(addedEntityWrapper.Entity, removedEntity))
                {
                    this._addedEntities.RemoveAt(i);

                    // if this entity was not even sent to the server
                    if (!addedEntityWrapper.IsCommited)
                    {
                        // then there's no need to remove it
                        return;
                    }
                }
                else
                {
                    i++;
                }
            }

            var entityWrapperToRemove = removedEntity is IEntityWrapper
                    ? (IEntityWrapper)removedEntity
                    : new EntityWrapper(removedEntity, this.ToDocumentConversionFunctor, this.EntityKeyGetter);

            this._removedEntities.Add(
                removedKey, 
                entityWrapperToRemove
            );

            // removing the entity from the list of loaded entities as well
            this._loadedEntities.Remove(removedKey);
        }

        /// <summary>
        /// Called by base class, before a new get/query/scan request is sent to DynamoDb
        /// </summary>
        public override void ClearModifications()
        {
            // Skipping added and removed entities.
            // Loaded (and probably then modified) entities should not be skipped.
            this._addedEntities.Clear();
            this._removedEntities.Clear();
        }

        #endregion

        #region Private Properties

        /// <summary>
        /// An action, that must be executed after next submit
        /// </summary>
        internal Action<Exception> ThingsToDoUponSubmit;

        /// <summary>
        /// Entities that were loaded from DynamoDb or 
        /// </summary>
        private readonly Dictionary<EntityKey, IEntityWrapper> _loadedEntities = new Dictionary<EntityKey, IEntityWrapper>();

        /// <summary>
        /// Entities to add
        /// </summary>
        private readonly List<EntityWrapper> _addedEntities = new List<EntityWrapper>();

        /// <summary>
        /// Keys of removed entities
        /// </summary>
        private readonly Dictionary<EntityKey, IEntityWrapper> _removedEntities = new Dictionary<EntityKey, IEntityWrapper>();

        /// <summary>
        /// Implements converting objects into documents (caches everything it needs for that)
        /// </summary>
        internal readonly Func<object, Document> ToDocumentConversionFunctor;  

        #endregion

        #region Private Methods

        /// <summary>
        /// Sends all modifications to DynamoDb
        /// </summary>
        protected internal Task SubmitChangesAsync()
        {
            var entitiesToAdd = new Dictionary<EntityKey, Document>();
            var entitiesToUpdate = new Dictionary<EntityKey, Document>();
            var entitiesToRemove = new Dictionary<EntityKey, Document>();

            // all newly added (even already saved and not modified any more) entities
            var addedEntitiesDictionary = new Dictionary<EntityKey, IEntityWrapper>();

            try
            {
                // saving modified entities
                foreach (var wrapper in this._loadedEntities.Values)
                {
                    var modifiedDoc = wrapper.GetDocumentIfDirty();
                    if (modifiedDoc == null)
                    {
                        continue;
                    }

                    var modifiedKey = this.EntityKeyGetter.GetKey(modifiedDoc);

                    // no need to modify the entity, if it was removed
                    if (this._removedEntities.ContainsKey(modifiedKey))
                    {
                        continue;
                    }

                    this.Log("Putting modified entity with key {0}", modifiedKey);
                    entitiesToUpdate.Add(modifiedKey, modifiedDoc);
                }

                foreach (var addedEntity in this._addedEntities)
                {
                    var addedKey = addedEntity.EntityKey;
                    // there should be no way to add an existing entity
                    //TODO: add support for entities, that were removed and then added anew
                    if
                    (
                        (this._loadedEntities.ContainsKey(addedKey))
                        ||
                        (addedEntitiesDictionary.ContainsKey(addedKey))
                    )
                    {
                        throw new InvalidOperationException(string.Format("An entity with key {0} cannot be added, because entity with that key already exists", addedKey));
                    }

                    addedEntitiesDictionary.Add(addedKey, addedEntity);

                    var addedDoc = addedEntity.GetDocumentIfDirty();
                    // if this entity was already submitted and wasn't modified after that
                    if (addedDoc != null)
                    {
                        this.Log("Putting added entity with key {0}", addedKey);
                        entitiesToAdd.Add(addedKey, addedDoc);
                    }
                }

                // removing removed entities
                foreach (var removedKvp in this._removedEntities)
                {
                    // if the entity was removed and then added anew - then it shouldn't be removed from the table
                    if (addedEntitiesDictionary.ContainsKey(removedKvp.Key))
                    {
                        continue;
                    }

                    this.Log("Removing entity with key {0}", removedKvp);
                    entitiesToRemove.Add(removedKvp.Key, removedKvp.Value.AsDocument());
                }
            }
            catch (Exception ex)
            {
                // executing a registered action, if any
                this.ThingsToDoUponSubmit.FireSafely(ex);
                this.ThingsToDoUponSubmit = null;
                throw;
            }

            // sending updates to DynamoDb and to cache
#if DEBUG
            var sw = new Stopwatch();
            sw.Start();
#endif

            // this stuff is here only because we want to keep this file free of async keyword
            return
                this.ExecuteUpdateBatchAsync(entitiesToAdd, entitiesToUpdate, entitiesToRemove)
                .ContinueWith
                (
                    updateTask => // and we can't use TaskContinuationOptions.OnlyOnRanToCompletion, because we don't want to return a cancelled task from this method 
                    {
                        // executing a registered action, if any
                        this.ThingsToDoUponSubmit.FireSafely(updateTask.Exception);
                        this.ThingsToDoUponSubmit = null;
                        if (updateTask.Exception != null)
                        {
                            throw updateTask.Exception;
                        }
#if DEBUG
                        sw.Stop();
                        this.Log("Batch update operation took {0} ms", sw.ElapsedMilliseconds);
#endif

                        // clearing the list of removed entities, as they should not be removed twice
                        this._removedEntities.Clear();

                        // clearing IsDirty-flag on all newly added entities (even those, that are not dirty) 
                        // and update the version attribute (if applicable)
                        foreach (var addedEntity in addedEntitiesDictionary.Values)
                        {
                            addedEntity.Commit();
                        }

                        // clearing IsDirty-flag on updated entities (even those, that are not dirty)
                        // and update the version attribute (if applicable)
                        foreach (var updatedEntityWrapper in this._loadedEntities.Values)
                        {
                            updatedEntityWrapper.Commit();
                        }
                    }
                )
            ;
        }

        /// <summary>
        /// Registers an entity to be updated (for CUD operations)
        /// </summary>
        protected internal void AddUpdatedEntity(object newEntity, object oldEntity)
        {
            var key = this.EntityKeyGetter.GetKey(newEntity);

            EntityKey oldKey = null;
            if (oldEntity != null)
            {
                try
                {
                    oldKey = this.EntityKeyGetter.GetKey(oldEntity);
                }
                catch (Exception)
                {
                    oldKey = null;
                }
            }

            // if there's no previous key value - then a new entity is being added
            if (oldKey == null)
            {
                this._addedEntities.Add(new EntityWrapper(newEntity, this.ToDocumentConversionFunctor, this.EntityKeyGetter));
                return;
            }

            // checking that the key wasn't modified (which is not allowed)
            if (!key.Equals(oldKey))
            {
                throw new InvalidOperationException("Entity key cannot be edited");
            }

            this._loadedEntities[key] = new EntityWrapper(newEntity, this.ToDocumentConversionFunctor, this.EntityKeyGetter);
        }

        /// <summary>
        /// Returns a single entity by it's keys. Very useful in ASP.Net MVC
        /// </summary>
        protected internal object Find(params object[] keyValues)
        {
            return this.LoadEntities(this.GetTranslationResultForFind(keyValues), this.TableEntityType);
        }

        /// <summary>
        /// Asyncronously returns a single entity by it's keys. Very useful in ASP.Net MVC
        /// </summary>
        protected internal Task<object> FindAsync(params object[] keyValues)
        {
            return this.LoadEntitiesAsync(this.GetTranslationResultForFind(keyValues), this.TableEntityType);
        }

        /// <summary>
        /// Called by base class after creating a get/query/scan results reader
        /// </summary>
        /// <param name="reader"></param>
        protected override void InitReader(ISupervisableEnumerable reader)
        {
            base.InitReader(reader);

            // When an item is fetched from DynamoDb and enumerated - we need to keep a reference to it.
            // But only table entities should be registered. Projection types shouldn't.
            reader.EntityDocumentEnumerated += (entityDocument, entityWrapper) =>
            {
                EntityKey entityKey = null;

                // if this is the whole entity - then attaching it
                if (entityWrapper != null)
                {
                    entityKey = this.EntityKeyGetter.GetKey(entityDocument);
                    this._loadedEntities[entityKey] = entityWrapper;
                }

                // also putting the entity to cache
                var curIndexCreator = this.CurrentIndexCreator;
                if (curIndexCreator != null)
                {
                    curIndexCreator.AddEntityToIndex(entityKey, entityDocument);
                }
            };
        }

        /// <summary>
        /// Fills in and executes an update batch
        /// </summary>
        private Task ExecuteUpdateBatchAsync(IDictionary<EntityKey, Document> addedEntities, IDictionary<EntityKey, Document> modifiedEntities, IDictionary<EntityKey, Document> removedEntities)
        {
            var dynamoDbUpdateTasks = AddEntriesAsync(addedEntities)
                .Concat(AddEntriesAsync(modifiedEntities))
                .Concat(DeleteEntitiesAsync(removedEntities))
                .ToArray();

            if (string.IsNullOrEmpty(VersionPropertyName)) 
            {
                // Only update the cache in a separate thread if there is no Version Property
                this.Cache.UpdateCacheAndIndexes(addedEntities, modifiedEntities, removedEntities.Keys);
            }


            return Task.WhenAll(dynamoDbUpdateTasks).ContinueWith
            (
                updateTask =>
                {
                    if (updateTask.Exception != null)
                    {
                        // If the update failed - then removing all traces of modified entities from cache,
                        // as the update might partially succeed (in that case some of entities in cache might become stale)
                        this.Cache.RemoveEntities(addedEntities.Keys.Union(modifiedEntities.Keys).Union(removedEntities.Keys));

                        throw updateTask.Exception;
                    }

                    if (!string.IsNullOrEmpty(VersionPropertyName))
                    {
                        // Update the cache if there is a Version Property. Version properties change 
                        // during insertion/updating so we want to have the latest values for versions 
                        // of added and modified entities. 
                        this.Cache.UpdateCacheAndIndexes(addedEntities, modifiedEntities, removedEntities.Keys);
                    }
                }
            );
        }

        /// <summary>
        /// Performs create/update operations with DynamoDB, respecting the [DynamoDBVersion] attribute if present
        /// </summary>
        /// <remarks>
        /// If the entity is new (Version property has not been set) set the version property 0. If 
        /// updating an existing entity increment the Version property by 1.
        /// </remarks>
        /// <param name="entitiesToAdd">Dictionary of entity keys and documents to add/update</param>
        /// <returns>An IEnumerable containing one task for each entity to add or update.</returns>
        private IEnumerable<Task> AddEntriesAsync(IDictionary<EntityKey, Document> entitiesToAdd) 
        {
            return entitiesToAdd
                .Values
                .Select(entry => {
                    var putConfiguration = new PutItemOperationConfig();
                    if (!string.IsNullOrEmpty(VersionPropertyName)) 
                    {
                        if (entry[VersionPropertyName] != null && 
                            entry[VersionPropertyName].ToPrimitive(typeof(object)).Value == null)
                        {
                            entry[VersionPropertyName] = 0;

                            putConfiguration.ConditionalExpression = new Expression() 
                            {
                                ExpressionStatement = $"attribute_not_exists(#key)",
                                ExpressionAttributeNames = new Dictionary<string, string>
                                {
                                    { "#key", HashKeyPropetyName }
                                }
                            };
                        }
                        else
                        {
                            putConfiguration.Expected = new Document(new Dictionary<string, DynamoDBEntry>()
                            {
                                { VersionPropertyName, entry[VersionPropertyName] }
                            });

                            // TODO: We assume that the Version is an int here but it could be 
                            // any numeric (signed or unsigned) so we may overflow or lose precision when 
                            // converting or incrementing. This isn't strcitly a problem for optimstic locking 
                            // as long as the verison number is different from the fetched version number.
                            entry[VersionPropertyName] = (entry[VersionPropertyName].AsInt() + 1);
                        }
                    }

                    return TableDefinition.PutItemAsync(entry, putConfiguration);
                });    
        }


        /// <summary>
        /// Performs Delete for entitites on DynamoDB, respecting the [DynamoDBVersion] attribute if present
        /// </summary>
        /// <param name="removedEntities">Dictionary of entity keys and documents to remove</param>
        /// <returns>An IEnumerable containing one task for each entity to add or update.</returns>
        private IEnumerable<Task> DeleteEntitiesAsync(IDictionary<EntityKey, Document> removedEntities) 
        {
            return removedEntities
                .Select(entry => {
                    var deleteConfiguration = new DeleteItemOperationConfig();
                    if (!string.IsNullOrEmpty(VersionPropertyName)) 
                    {
                        deleteConfiguration.Expected = new Document(new Dictionary<string, DynamoDBEntry>() 
                        {
                            { VersionPropertyName, entry.Value[VersionPropertyName] }
                        });
                    }

                    return TableDefinition.DeleteItemAsync(
                        this.EntityKeyGetter.GetKeyDictionary(entry.Key),
                        deleteConfiguration
                    );
                });
        }


        private TranslationResult GetTranslationResultForFind(params object[] keyValues)
        {
            var keyValuesList = new List<DynamoDBEntry>();

            // if a HashKey value was explicitly specified
            if (this.HashKeyValue != null)
            {
                // then adding it's value to the list of conditions
                keyValuesList.Add(this.HashKeyValue);
            }

            foreach (var keyValue in keyValues)
            {
                keyValuesList.Add(keyValue.ToDynamoDbEntry(keyValue.GetType()));
            }

            if (this.KeyNames.Length != keyValuesList.Count)
            {
                throw new InvalidOperationException
                (
                    string.Format
                    (
                        "Table {0} has {1} key fields, but {2} key values was provided",
                        this.TableDefinition.TableName,
                        this.KeyNames.Length,
                        keyValues.Length
                    )
                );
            }

            // constructing a GET query
            var result = new TranslationResult(this.TableEntityType.Name);
            for (int i = 0; i < keyValuesList.Count; i++)
            {
                var condition = new SearchCondition(ScanOperator.Equal, keyValuesList[i]);
                result.Conditions[this.KeyNames[i]] = new List<SearchCondition> { condition };
            }

            return result;
        }

        #endregion
    }
}
