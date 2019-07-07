(function () {
    "use strict";

    angular.module("appModule").controller("loginController", loginController);

    function loginController($http, $location, Global, Auth) {
        var vm = this;
        vm.login = {
            username: "aliasmael",
            password: "Ali@fci.7"
        };
        vm.errorMessage = "";
        vm.successMessage = "";
        vm.isBusy = false;


        //login
        vm.Login = function () {
            vm.isBusy = true;

            $http.post("/api/auth/login", JSON.stringify(vm.login))
                .then(function (response) {
                    //success
                    if (response.data == true){
                        vm.successMessage = "Login successfully";
                        Auth.setUser(vm.login.username); //Update the state of the user in the app
                        $location.path('/');
                    }
                    else
                        vm.errorMessage = "Failed to login. ";

                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to connect to server. ";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }

        //login
        vm.Logout = function () {
            vm.isBusy = true;

            $http.post("/api/auth/logout")
                .then(function (response) {
                    //success
                    vm.successMessage = "Logout successfully";
                    Auth.setUser(null); //Update the state of the user in the app
                    $location.path('/login');
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to connect to server. ";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }
    }
})();