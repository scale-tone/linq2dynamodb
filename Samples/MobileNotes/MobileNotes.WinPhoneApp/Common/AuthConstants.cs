using System.IO;
using System.Runtime.Serialization.Json;

namespace MobileNotes.WinPhoneApp.Common
{
    /// <summary>
    /// Represents the contents of AuthConstants.json file with OpenID Connect app credentials and other authentication constants
    /// </summary>
    public class AuthConstants
    {
        public string JwtAuthSchema { get; set; }
        public string LiveClientId { get; set; }

        public static AuthConstants FromJsonStream(Stream stream)
        {
            return (AuthConstants)new DataContractJsonSerializer(typeof(AuthConstants)).ReadObject(stream);
        }
    }
}
