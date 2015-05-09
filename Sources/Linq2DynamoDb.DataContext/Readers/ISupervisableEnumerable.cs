using System;
using System.Collections;
using Amazon.DynamoDBv2.DocumentModel;

namespace Linq2DynamoDb.DataContext
{
    /// <summary>
    /// An IEnumerable, which enumeration process can be controlled
    /// </summary>
    public interface ISupervisableEnumerable : IEnumerable
    {
        event Action<Document, IEntityWrapper> EntityDocumentEnumerated;
        event Action EnumerationFinished;
    }
}
