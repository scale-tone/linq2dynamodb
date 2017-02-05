# MobileNotes.WinPhoneApp

An sample Windows Phone app, that demonstrates, how to communicate with [NotesDataService](https://github.com/scale-tone/linq2dynamodb/blob/master/Samples/MobileNotes.Web/Services/NotesDataService.svc) OData service with [DataServiceContext](https://msdn.microsoft.com/en-us/library/system.data.services.client.dataservicecontext(v=vs.95).aspx).

Implements [user authentication](https://github.com/scale-tone/linq2dynamodb/blob/master/Samples/MobileNotes.IonicApp/www/js/services.js) via [Microsoft.Live.Controls.SignInButton](https://msdn.microsoft.com/en-us/library/microsoft.live.controls.signinbutton.aspx).

Please, specify your service deployment address at the beginning of [MainViewModel.cs](https://github.com/scale-tone/linq2dynamodb/blob/master/Samples/MobileNotes.WinPhoneApp/ViewModels/MainViewModel.cs).
