using System;
using System.Web;

namespace MobileNotes.OAuth
{
    /// <summary>
    /// Implements authentication via Authorization header and JWT token
    /// </summary>
    public static class AuthRoutine
    {
        private static readonly string AuthSchema = AuthConstants.FromLocalJsonFile().JwtAuthSchema;

        public static string GetUserIdFromAuthorizationHeader()
        {
            var authHeader = HttpContext.Current.Request.Headers["Authorization"];
            if (string.IsNullOrEmpty(authHeader))
            {
                throw new UnauthorizedAccessException();
            }

            var authHeaderParts = authHeader.Split(' ');
            if
            (
                (authHeaderParts.Length < 2)
                ||
                (authHeaderParts[0] != AuthSchema)
            )
            {
                throw new UnauthorizedAccessException();
            }

            try
            {
                return JwtParser.ValidateAndGetUserId(authHeaderParts[1]);
            }
            catch (Exception)
            {
                throw new UnauthorizedAccessException();
            }
        }
    }
}
