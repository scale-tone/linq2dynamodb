using System.IdentityModel.Tokens;
using System.Linq;
using System.Security.Claims;
using System.Security.Cryptography;
using System.ServiceModel.Security.Tokens;
using System.Text;

namespace MobileNotes.Web.Common
{
    public class LiveIdJwtParser : JwtParser
    {
        private static readonly byte[] MsLiveKeyInBytes;

        static LiveIdJwtParser()
        {
            // MS derives a signing key from your app's client secret. Don't know, what was the idea...
            using (var sha256 = new SHA256Managed())
            {
                var liveClientSecret = AuthConstants.FromLocalJsonFile().LiveClientSecret;
                var secretBytes = new UTF8Encoding(true, true).GetBytes(liveClientSecret + "JWTSig");
                MsLiveKeyInBytes = sha256.ComputeHash(secretBytes);
            }
        }

        protected override SecurityKey GetSecurityKey(string notUsed)
        {
            // Because BinarySecretSecurityToken class is not marked as thread-safe, we'd better not cache it, but cache the raw cert bytes
            var binarySecurityToken = new BinarySecretSecurityToken(MsLiveKeyInBytes);
            return binarySecurityToken.SecurityKeys == null ? null : binarySecurityToken.SecurityKeys.First();
        }

        protected override string GetUserId(ClaimsPrincipal principal)
        {
            return "ms:" + principal.Claims.First(c => c.Type == "uid").Value;
        }
    }
}