using System.IdentityModel.Tokens;
using System.IO;
using System.Web;

namespace MobileNotes.OAuth
{
    /// <summary>
    /// Represents the contents of AuthConstants.json file with OpenID Connect app credentials and other authentication constants
    /// </summary>
    public class AuthConstants
    {
        public string JwtAuthSchema { get; set; }
        public string LiveClientSecret { get; set; }

        public static AuthConstants FromLocalJsonFile()
        {
            string authConstantsFileName = HttpContext.Current.Server.MapPath("~/AuthConstants.json");
            return JsonExtensions.DeserializeFromJson<AuthConstants>(File.ReadAllText(authConstantsFileName));
        }
    }
}
