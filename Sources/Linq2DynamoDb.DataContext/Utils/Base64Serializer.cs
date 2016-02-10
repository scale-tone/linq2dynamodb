using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Linq2DynamoDb.DataContext.Utils
{
	public class Base64Serializer<T>
	{
		public string Serialize(T obj)
		{
			byte[] bytes = SerializeToBytes(obj);
			return Convert.ToBase64String(bytes);
		}

		public T Deserialize(string text)
		{
			byte[] bytes = Convert.FromBase64String(text);
			return DeserializeBytes(bytes);
		}

		public static byte[] SerializeToBytes(T obj)
		{
			using (MemoryStream memoryStream = new MemoryStream())
			{
				BinaryFormatter serializer = new BinaryFormatter();
				serializer.Serialize(memoryStream, obj);
				return memoryStream.ToArray();
			}
		}

		public static T DeserializeBytes(byte[] bytes)
		{
			using (MemoryStream memoryStream = new MemoryStream(bytes))
			{
				BinaryFormatter serializer = new BinaryFormatter();
				memoryStream.Seek(0, SeekOrigin.Begin);
				return (T) serializer.Deserialize(memoryStream);
			}
		}
	}
}
