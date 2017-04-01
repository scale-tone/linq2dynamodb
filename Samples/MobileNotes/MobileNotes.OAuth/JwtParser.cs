using System.Collections.Generic;
using System.IdentityModel.Tokens;
using System.Security.Claims;

namespace MobileNotes.OAuth
{
    public abstract class JwtParser
    {
        /// <summary>
        /// Validates JWT tokens from different providers and returns a userId extracted from token
        /// </summary>
        public static string ValidateAndGetUserId(string tokenString)
        {
            var jwtHandler = new JwtSecurityTokenHandler();

            var validationParams = new TokenValidationParameters
            {
                ValidateAudience = false,
                ValidateIssuer = true,
                ValidIssuers = JwtParsers.Keys,
                IssuerSigningKeyResolver = (t, securityToken, identifier, p) =>
                {
                    var securityKeyIdentifier = identifier.Count > 0 ? identifier[0].Id : string.Empty;

                    return JwtParsers[((JwtSecurityToken)securityToken).Issuer].GetSecurityKey(securityKeyIdentifier);
                }
            };

            SecurityToken validatedToken;
            var principal = jwtHandler.ValidateToken(tokenString, validationParams, out validatedToken);

            var jwtSecurityToken = (JwtSecurityToken)validatedToken;
            return JwtParsers[jwtSecurityToken.Issuer].GetUserId(principal);
        }

        private static readonly Dictionary<string, JwtParser> JwtParsers = new Dictionary<string, JwtParser>
        {
            // Google (can use two different issuer values, according to documentation)
            {"accounts.google.com", new GoogleJwtParser()},
            {"https://accounts.google.com", new GoogleJwtParser()},

            // MS LiveID
            {"urn:windows:liveid", new LiveIdJwtParser()},
        };

        protected abstract SecurityKey GetSecurityKey(string securityKeyIdentifier);
        protected abstract string GetUserId(ClaimsPrincipal principal);
    }
}