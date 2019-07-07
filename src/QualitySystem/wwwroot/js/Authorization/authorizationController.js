var appModule = angular.module('appModule');

//define Auth
appModule.factory('Auth', function () {
    var user;

    return {
        setUser: function (aUser) {
            user = aUser;
        },
        isLoggedIn: function () {
            return (user) ? user : false;
        }
    }
});

//Authorization Controller
appModule.controller('AuthController', function ($scope, Auth) {
    $scope.$watch(Auth.isLoggedIn, function (isLoggedIn) {
        $scope.isLoggedIn = isLoggedIn;
    });
});