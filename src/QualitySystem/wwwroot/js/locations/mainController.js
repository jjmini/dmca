(function () {
    "use strict";

    angular.module("appModule").controller("locationsMainController", locationsMainController);

    function locationsMainController($http, Global) {
        var vm = this;
        vm.locations = [];
        vm.errorMessage = "";
        vm.successMessage = "";
        vm.isBusy = true;

        vm.newLocation = {};
        vm.locationToEdit = {};

        //get data of all locations
        $http.get("/api/location/getAllLocations")
                .then(function (response) {
                    //success
                    angular.copy(response.data, vm.locations)
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to load data. ";
                })
                .finally(function () {
                    vm.isBusy = false
                });

        //Add new location
        vm.AddNewLocation = function(location){
            //get data of all locations
            $http.post("/api/location/AddNewLocation", location)
                    .then(function (response) {
                        //success
                        vm.successMessage = "Location added successfully.";
                        vm.locations.push(response.data);
                    }, function (error) {
                        //failure
                        vm.errorMessage = "Failed to add location. ";
                    })
                    .finally(function () {
                        vm.isBusy = false
                    });
        }

        //Delete location
        vm.DeleteLocation = function (locationId) {
            //get data of all locations
            $http.post("/api/location/DeleteLocation", locationId)
                    .then(function (response) {
                        //success
                        vm.successMessage = "Location deleted successfully.";
                        vm.locations = vm.locations.filter(function (item) {
                            return item.id !== locationId;
                        });
                    }, function (error) {
                        //failure
                        vm.errorMessage = "Failed to delete location. ";
                    })
                    .finally(function () {
                        vm.isBusy = false
                    });
        }

        vm.BeginLocationEditing = function (id) {
            var location = vm.locations.filter(function (item) {
                return item.id == id;
            })[0];

            angular.copy(location, vm.locationToEdit);
        }

        //Update location
        vm.UpdateLocation = function (location) {
            //get data of all locations
            $http.post("/api/location/UpdateLocation", location)
                    .then(function (response) {
                        //success
                        vm.successMessage = "Location updated successfully.";
                        var tempLocation = vm.locations.filter(function (item) {
                            return item.id == location.id;
                        })[0];
                        angular.copy(location, tempLocation);
                        location = {};
                    }, function (error) {
                        //failure
                        vm.errorMessage = "Failed to upadate location. ";
                    })
                    .finally(function () {
                        vm.isBusy = false
                    });
        }
        
    }
})();