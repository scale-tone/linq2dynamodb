
angular.module('mobilenotesapp.services', []).factory('GoogleOauth', ['$q', '$http', function ($q, $http) {

    return {

        // shows Google's OAuth form inside inappbrowser and captures accessToken
        getAccessCode: function (clientId, redirectUri) {
            var deferred = $q.defer();

            var uri = 'https://accounts.google.com/o/oauth2/v2/auth?client_id=' + clientId + '&redirect_uri=' + redirectUri + '&scope=email%20profile&response_type=code';

            var inAppBrowser = window.cordova.InAppBrowser.open(uri, '_blank', 'location=no,clearsessioncache=no,clearcache=no');

            inAppBrowser.addEventListener('loadstart', function (event) {

                // checking if google has redirected us to our callback uri
                if ((event.url).indexOf(redirectUri) !== 0) {
                    return;
                }

                inAppBrowser.close();

                var accessCode = event.url.split('code=');
                if (accessCode.length <= 1) {
                    deferred.reject('Authentication failed: Google didn\'t return access code');
                    return;
                }

                deferred.resolve(accessCode[1]);
            });

            return deferred.promise;
        },

        //NOTE: Trading accessCode for id_token is supposed to be done on the server side, because it requires your app's clientSecret (which is actually a secret!).
        // Here we do this on the client for demo purposes only!
        getIdToken: function (clientId, clientSecret, redirectUri, accessCode) {
            var deferred = $q.defer();

            $http({
                method: 'POST',
                
                // skipping the OpenID Connect URI discovery, just using the URI from documentation - might stop working at some time
                url: 'https://www.googleapis.com/oauth2/v4/token',
                headers: {'Content-Type': 'application/x-www-form-urlencoded'},

                data: 'code=' + accessCode + '&client_id=' + clientId + '&client_secret=' + clientSecret + '&redirect_uri=' + redirectUri + '&grant_type=authorization_code'

            }).then(function(response) {

                if (response.data && response.data.id_token) {
                    deferred.resolve(response.data.id_token);
                } else {
                    deferred.reject('Authentication failed: Google didn\'t return id_token');
                }

            }, function (response) {
                deferred.reject('Authentication failed: Google didn\'t return id_token');
            });

            return deferred.promise;
        }
    };
}]);
