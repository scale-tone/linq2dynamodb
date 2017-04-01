using System;
using Amazon.DynamoDBv2.DataModel;
using Linq2DynamoDb.DataContext;

namespace MobileNotes.WebApi.Models
{
    [DynamoDBTable("MobileNotes")]
    public class Note : EntityBase
    {
        public string ID { get; set; }

        public string Text { get; set; }

        public DateTime TimeCreated { get; set; }
    }
}