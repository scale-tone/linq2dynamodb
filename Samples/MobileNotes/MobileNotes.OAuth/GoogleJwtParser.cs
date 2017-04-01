using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens;
using System.Linq;
using System.Net.Http;
using System.Security.Claims;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Web.Script.Serialization;

namespace MobileNotes.OAuth
{
    public class GoogleJwtParser : JwtParser
    {
        protected override SecurityKey GetSecurityKey(string securityKeyIdentifier)
        {
            // Because X509Certificate2 class is not marked as thread-safe, we'd better not cache it, but cache the raw cert bytes
            return new X509SecurityKey(new X509Certificate2(CertsInBytes.Value[securityKeyIdentifier]));
        }

        protected override string GetUserId(ClaimsPrincipal principal)
        {
            return "google:" + principal.Claims.First(c => c.Type == "http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier").Value;
        }

        private static readonly Lazy<IDictionary<string, byte[]>> CertsInBytes = new Lazy<IDictionary<string, byte[]>>(() =>
        {
            // loading Google's public certificate bytes only once
            using (var client = new HttpClient())
            {
                var certsString = client.GetStringAsync("https://www.googleapis.com/oauth2/v1/certs").Result;
                var certsInBase64 = new JavaScriptSerializer().Deserialize<IDictionary<string, string>>(certsString);

                return certsInBase64.ToDictionary(p => p.Key, p =>
                {
                    var certInBase64 = p.Value
                        .Replace("-----BEGIN CERTIFICATE-----", "")
                        .Replace("-----END CERTIFICATE-----", "")
                        .Trim();

                    return new UTF8Encoding().GetBytes(certInBase64);
                });
            }
        });
    }
}
