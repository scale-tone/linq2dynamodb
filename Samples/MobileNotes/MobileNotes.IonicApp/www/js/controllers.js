
// Specify your own MobileNotes.Web service URI here
var serviceUri = 'http://<your-service-address>.elasticbeanstalk.com';

angular.module('mobilenotesapp.controllers', []).controller('maincontroller', ['$scope', '$resource', '$http', 'GoogleOauth', function ($scope, $resource, $http, googleOauth) {

    function todayMinusOneWeek() {
        var today = new Date();
        var weekAgo = new Date(today);
        weekAgo.setDate(today.getDate() - 7);
        return weekAgo;
    }

    // creating a resource
    var noteResource = $resource
    (
        serviceUri + '/Services/NotesDataService.svc/Notes', {},
        {
            query: {

                method: 'GET',
                // need to fix isArray value, because WCF Data Services return a scalar 'd' object instead of a collection
                isArray: false,
                params: {
                    // sorting by TimeCreated
                    '$orderby': 'TimeCreated desc',
                    // demonstrating, how to make LINQ queries
                    '$filter': 'TimeCreated gt DateTime\'' + todayMinusOneWeek().toISOString() + '\''
                }
            }
        }
    );

    $scope.newNoteText = 'New Note';

    $scope.login = function () {

        $scope.errorMsg = '';

        // requesting OpenID Connect app creds from server. 
        $http.get(serviceUri + '/AuthConstants.json').then(function (response) {

            googleOauth.getAccessCode(response.data.GoogleClientId, response.data.GoogleRedirectUri).then(function (accessCode) {

                googleOauth.getIdToken(response.data.GoogleClientId, response.data.GoogleClientSecret, response.data.GoogleRedirectUri, accessCode).then(function (idToken) {

                    $scope.isLoggedIn = true;

                    // now the easiest way to send auth header with each request is to add it to $http's default headers
                    $http.defaults.headers.common['Authorization'] = response.data.JwtAuthSchema + ' ' + idToken;
                    $http.defaults.headers.common['Accept'] = 'application/json';

                    // loading notes
                    $scope.notes = noteResource.query(function () { }, function (err) {
                        $scope.errorMsg = err;
                    });

                }, function (err) {
                    $scope.isLoggedIn = false;
                    $scope.errorMsg = err;
                });

            }, function (err) {
                $scope.isLoggedIn = false;
                $scope.errorMsg = err;
            });

        }, function() {
            $scope.errorMsg = 'Failed to get OpenID Connect app creds from server. Please, create OauthAppCredentials.json file in server\'s root and fill it with your own values!';
        });
    }

    $scope.addNote = function () {

        $scope.errorMsg = '';

        var newNote = new noteResource();

        newNote.ID = (new Date()).getTime().toString();
        newNote.TimeCreated = new Date();
        newNote.Text = this.newNoteText;

        newNote.$save(function (result) {

            // adding the newly created note to the top
            $scope.notes.d.unshift(result.d);

        }, function(err) {
            $scope.errorMsg = err;
        });
    }

}]);
