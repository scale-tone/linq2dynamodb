//------------------------------------------------------------------------------
// <copyright file="WebDataService.svc.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System.Data.Services;
using System.Data.Services.Common;
using System.Diagnostics;
using MobileNotes.Web.Model;

namespace MobileNotes.Web.Services
{
    public class NotesDataService : DataService<NotesDataContext>
    {
        // This method is called only once to initialize service-wide policies.
        public static void InitializeService(DataServiceConfiguration config)
        {
            config.SetEntitySetAccessRule("*", EntitySetRights.All);

            config.DataServiceBehavior.MaxProtocolVersion = DataServiceProtocolVersion.V3;
        }

        protected override void HandleException(HandleExceptionArgs args)
        {
            Debug.WriteLine(args.Exception);
            base.HandleException(args);
        }
    }
}
