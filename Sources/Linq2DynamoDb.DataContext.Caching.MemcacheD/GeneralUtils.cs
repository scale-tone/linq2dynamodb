using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace Linq2DynamoDb.DataContext.Caching.MemcacheD
{
    public static class GeneralUtils
    {
        private static readonly ThreadLocal<HashAlgorithm> Md5ThreadLocal = new ThreadLocal<HashAlgorithm>(MD5.Create);

        public static string ToMd5String(this string s)
        {
            var data = Md5ThreadLocal.Value.ComputeHash(Encoding.UTF8.GetBytes(s));

            var builder = new StringBuilder();
            foreach (byte b in data)
            {
                builder.Append(b.ToString("x2"));
            }

            return builder.ToString();
        }
    }
}
