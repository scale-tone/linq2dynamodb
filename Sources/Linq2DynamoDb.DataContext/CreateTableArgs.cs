using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Amazon.DynamoDBv2.Model;
using Linq2DynamoDb.DataContext.Utils;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// Represents arguments for DataContext.CreateTableIfNotExists() method
    /// </summary>
    public class CreateTableArgs<TEntity>
    {
        public CreateTableArgs(Expression<Func<TEntity, object>> hashKeyFieldExp)
        {
            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            params Expression<Func<TEntity, object>>[] localSecondaryIndexFieldExps
        )
        {
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            string hashKeyFieldName,
            Type hashKeyFieldType,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            params Expression<Func<TEntity, object>>[] localSecondaryIndexFieldExps
        )
        {
            this._hashKeyFieldName = hashKeyFieldName;
            this._hashKeyFieldType = hashKeyFieldType;
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
        }

        public CreateTableArgs
        (
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            params Expression<Func<TEntity, object>>[] localSecondaryIndexFieldExps
        )
        {
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

		public CreateTableArgs
		(
			Expression<Func<TEntity, object>> hashKeyFieldExp,
			Expression<Func<TEntity, object>> rangeKeyFieldExp,
			Func<IEnumerable<TEntity>> getInitialEntities
		)
		{
			this._rangeKeyFieldExp = rangeKeyFieldExp;
			this._getInitialEntities = getInitialEntities;

			ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
		}

        public CreateTableArgs
        (
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            IEnumerable<Expression<Func<TEntity, object>>> localSecondaryIndexFieldExps,
            Func<IEnumerable<TEntity>> getInitialEntities
        )
        {
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
            this._getInitialEntities = getInitialEntities;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            long readCapacityUnits,
            long writeCapacityUnits,
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            params Expression<Func<TEntity, object>>[] localSecondaryIndexFieldExps
        )
        {
            this._readCapacityUnits = readCapacityUnits;
            this._writeCapacityUnits = writeCapacityUnits;
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            long readCapacityUnits,
            long writeCapacityUnits,
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            LocalSecondaryIndexDefinitions<TEntity> localSecondaryIndexFieldExps,
            Func<IEnumerable<TEntity>> getInitialEntities
        )
        {
            this._readCapacityUnits = readCapacityUnits;
            this._writeCapacityUnits = writeCapacityUnits;
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
            this._getInitialEntities = getInitialEntities;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            long readCapacityUnits,
            long writeCapacityUnits,
            string hashKeyFieldName,
            Type hashKeyFieldType,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            IEnumerable<Expression<Func<TEntity, object>>> localSecondaryIndexFieldExps,
            Func<IEnumerable<TEntity>> getInitialEntities
        )
        {
            this._readCapacityUnits = readCapacityUnits;
            this._writeCapacityUnits = writeCapacityUnits;
            this._hashKeyFieldName = hashKeyFieldName;
            this._hashKeyFieldType = hashKeyFieldType;
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
            this._getInitialEntities = getInitialEntities;
        }

        public CreateTableArgs
        (
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            LocalSecondaryIndexDefinitions<TEntity> localSecondaryIndexFieldExps,
            GlobalSecondaryIndexDefinitions<TEntity> globalSecondaryIndexFieldExps
        )
        {
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
            this._globalSecondaryIndexFieldExps = globalSecondaryIndexFieldExps;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            long readCapacityUnits,
            long writeCapacityUnits,
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            LocalSecondaryIndexDefinitions<TEntity> localSecondaryIndexFieldExps,
            GlobalSecondaryIndexDefinitions<TEntity> globalSecondaryIndexFieldExps
        )
        {
            this._readCapacityUnits = readCapacityUnits;
            this._writeCapacityUnits = writeCapacityUnits;
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
            this._globalSecondaryIndexFieldExps = globalSecondaryIndexFieldExps;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            long readCapacityUnits,
            long writeCapacityUnits,
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            LocalSecondaryIndexDefinitions<TEntity> localSecondaryIndexFieldExps,
            GlobalSecondaryIndexDefinitions<TEntity> globalSecondaryIndexFieldExps,
            Func<IEnumerable<TEntity>> getInitialEntities
        )
        {
            this._readCapacityUnits = readCapacityUnits;
            this._writeCapacityUnits = writeCapacityUnits;
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
            this._globalSecondaryIndexFieldExps = globalSecondaryIndexFieldExps;
            this._getInitialEntities = getInitialEntities;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        public CreateTableArgs
        (
            Expression<Func<TEntity, object>> hashKeyFieldExp,
            Expression<Func<TEntity, object>> rangeKeyFieldExp,
            LocalSecondaryIndexDefinitions<TEntity> localSecondaryIndexFieldExps,
            GlobalSecondaryIndexDefinitions<TEntity> globalSecondaryIndexFieldExps,
            Func<IEnumerable<TEntity>> getInitialEntities
        )
        {
            this._rangeKeyFieldExp = rangeKeyFieldExp;
            this._localSecondaryIndexFieldExps = localSecondaryIndexFieldExps;
            this._globalSecondaryIndexFieldExps = globalSecondaryIndexFieldExps;
            this._getInitialEntities = getInitialEntities;

            ExtractFieldNameAndTypeFromExpression(hashKeyFieldExp, out this._hashKeyFieldName, out this._hashKeyFieldType);
        }

        private readonly long _readCapacityUnits = 5;
        private readonly long _writeCapacityUnits = 5;
        private readonly string _hashKeyFieldName;
        private readonly Type _hashKeyFieldType;
        private readonly Expression<Func<TEntity, object>> _rangeKeyFieldExp;
        private readonly IEnumerable<Expression<Func<TEntity, object>>> _localSecondaryIndexFieldExps;
        private readonly IEnumerable<Expression<Func<TEntity, GlobalSecondaryIndexDefinition>>> _globalSecondaryIndexFieldExps;
        private readonly Func<IEnumerable<TEntity>> _getInitialEntities;

        /// <summary>
        /// Converts table creation parameters into a request
        /// </summary>
        internal CreateTableRequest GetCreateTableRequest(string tableName)
        {
            var attributeDefinitions = new List<AttributeDefinition>
            {
                new AttributeDefinition
                {
                    AttributeName = this._hashKeyFieldName,
                    AttributeType = this._hashKeyFieldType.ToAttributeType()
                }
            };

            var keySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement { AttributeName = this._hashKeyFieldName, KeyType = "HASH" }
            };

            if (this._rangeKeyFieldExp != null)
            {
                string rangeKeyFieldName;
                Type rangeKeyFieldType;

                ExtractFieldNameAndTypeFromExpression(this._rangeKeyFieldExp, out rangeKeyFieldName, out rangeKeyFieldType);

                attributeDefinitions.Add
                (
                    new AttributeDefinition
                    {
                        AttributeName = rangeKeyFieldName,
                        AttributeType = rangeKeyFieldType.ToAttributeType()
                    }
                );

                keySchema.Add(new KeySchemaElement { AttributeName = rangeKeyFieldName, KeyType = "RANGE" });
            }

            // specifying local secondary indexes (all columns are always projected!)
            var localIndexes = new List<LocalSecondaryIndex>();
            if (this._localSecondaryIndexFieldExps != null)
            {
                foreach (var secondaryIndexFieldExp in this._localSecondaryIndexFieldExps)
                {
                    string indexFieldName;
                    Type indexFieldType;
                    ExtractFieldNameAndTypeFromExpression(secondaryIndexFieldExp, out indexFieldName, out indexFieldType);

                    if (!attributeDefinitions.Any(ad => ad.AttributeName == indexFieldName))
                    {
                        attributeDefinitions.Add
                        (
                            new AttributeDefinition
                            {
                                AttributeName = indexFieldName,
                                AttributeType = indexFieldType.ToAttributeType()
                            }
                        );
                    }

                    localIndexes.Add
                    (
                        new LocalSecondaryIndex
                        {
                            IndexName = indexFieldName + "Index",
                            KeySchema = new List<KeySchemaElement>
                            {
                                new KeySchemaElement
                                {
                                    AttributeName = this._hashKeyFieldName,
                                    KeyType = "HASH"
                                },
                                new KeySchemaElement
                                {
                                    AttributeName = indexFieldName,
                                    KeyType = "RANGE"
                                },
                            },
                            Projection = new Projection { ProjectionType = "ALL" }
                        }
                    );
                }
            }

            // specifying global secondary indexes (all columns are always projected!)
            var globalIndexes = new List<GlobalSecondaryIndex>();
            if (this._globalSecondaryIndexFieldExps != null)
            {
                foreach (var secondaryIndexFieldExp in this._globalSecondaryIndexFieldExps)
                {
                    GlobalSecondaryIndex indexDefinition;
                    IDictionary<string, Type> keyFields;

                    ExtractGlobalSecondaryIndexDefinitionFromExpression(secondaryIndexFieldExp, out indexDefinition, out keyFields);

                    attributeDefinitions.AddRange
                    (
                        keyFields
                            .Where(kv => !attributeDefinitions.Any(ad => ad.AttributeName == kv.Key))
                            .Select(kv => new AttributeDefinition {AttributeName = kv.Key, AttributeType = kv.Value.ToAttributeType()})
                    );

                    globalIndexes.Add(indexDefinition);
                }
            }

            return new CreateTableRequest
            {
                TableName = tableName,
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = this._readCapacityUnits,
                    WriteCapacityUnits = this._writeCapacityUnits
                },
                AttributeDefinitions = attributeDefinitions,
                KeySchema = keySchema,
                LocalSecondaryIndexes = localIndexes,
                GlobalSecondaryIndexes = globalIndexes,
            };
        }

        internal Func<IEnumerable<TEntity>> GetInitialEntitiesFunc
        {
            get { return this._getInitialEntities; }
        }

        private static void ExtractFieldNameAndTypeFromExpression(Expression<Func<TEntity, object>> fieldExp, out string fieldName, out Type fieldType)
        {
            var memberExp = fieldExp.Body as MemberExpression;
            if (memberExp == null)
            {
                var conversionExp = fieldExp.Body as UnaryExpression;
                if
                (
                    (conversionExp == null)
                    ||
                    ((memberExp = conversionExp.Operand as MemberExpression) == null)
                )
                {
                    throw new InvalidOperationException(string.Format("Failed to extract field name from provided expression {0}", fieldExp));
                }
            }
            fieldName = memberExp.Member.Name;
            fieldType = memberExp.Type;
        }

        private static void ExtractGlobalSecondaryIndexDefinitionFromExpression
        (
            Expression<Func<TEntity, GlobalSecondaryIndexDefinition>> indexDefExp,
            out GlobalSecondaryIndex indexDefinition,
            out IDictionary<string, Type> keyFields 
        )
        {
            var memberInitExp = indexDefExp.Body as MemberInitExpression;

            if 
            (
                (memberInitExp == null)
                ||
                (memberInitExp.Type != typeof (GlobalSecondaryIndexDefinition))
            )
            {
                throw new InvalidOperationException(string.Format("The provided expression {0} is invalid", indexDefExp));
            }

            indexDefinition = new GlobalSecondaryIndex
            {
                // all fields are always projected!
                Projection = new Projection { ProjectionType = "ALL" },
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 5,
                    WriteCapacityUnits = 5
                }
            };
            var keyFieldList = new Dictionary<string, Type>();
         
            // detaching global secondary index parameters from the expression
            foreach (var propAssignmentExp in memberInitExp.Bindings.Cast<MemberAssignment>().Where(exp => exp.BindingType == MemberBindingType.Assignment))
            {
                var keyPropertyExp = propAssignmentExp.Expression as MemberExpression;
                if (keyPropertyExp == null)
                {
                    // there might be a boxing operation
                    var convertExp = propAssignmentExp.Expression as UnaryExpression;
                    if (convertExp != null)
                    {
                        keyPropertyExp = (MemberExpression)convertExp.Operand;
                    }
                }
               
                switch (propAssignmentExp.Member.Name)
                {
                    case "HashKeyField":
                    {
                        if (keyPropertyExp == null)
                        {
                            throw new InvalidOperationException("HashKeyField expression is of wrong type");
                        }

                        indexDefinition.KeySchema.Add(new KeySchemaElement { KeyType = "HASH", AttributeName = keyPropertyExp.Member.Name });

                        keyFieldList[keyPropertyExp.Member.Name] = ((PropertyInfo)keyPropertyExp.Member).PropertyType;
                    }
                    break;
                    case "RangeKeyField":
                    {
                        if (keyPropertyExp == null)
                        {
                            throw new InvalidOperationException("RangeKeyField expression is of wrong type");
                        }

                        indexDefinition.KeySchema.Add(new KeySchemaElement { KeyType = "RANGE", AttributeName = keyPropertyExp.Member.Name });

                        keyFieldList[keyPropertyExp.Member.Name] = ((PropertyInfo)keyPropertyExp.Member).PropertyType;
                    }
                    break;
                    case "ReadCapacityUnits":
                    {
                        var capacityUnits = Expression.Lambda(propAssignmentExp.Expression).Compile().DynamicInvoke();
                        indexDefinition.ProvisionedThroughput.ReadCapacityUnits = (long)capacityUnits;
                    }
                    break;
                    case "WriteCapacityUnits":
                    {
                        var capacityUnits = Expression.Lambda(propAssignmentExp.Expression).Compile().DynamicInvoke();
                        indexDefinition.ProvisionedThroughput.WriteCapacityUnits = (long)capacityUnits;
                    }
                    break;
                }
            }

            indexDefinition.IndexName = 
                indexDefinition.KeySchema.Aggregate(string.Empty, (current, keySchemaElement) => current + keySchemaElement.AttributeName)
                +
                "Index";

            keyFields = keyFieldList;
        }
    }
}