(function () {
    "use strict";

    //add new controller to appModule
    angular.module("appModule").controller("addNewDeviceController", addNewDeviceController);

    function addNewDeviceController($http, Global) {
        //vars decleration
        var vm = this;
        vm.newDevice = {
            "type": "0",
            "serialNumber": "",
            "broken": false,
            "spare": false,
            "locationName": "",
            "modelName": "",
            "components": []
        };
        vm.errorMessage = "";
        vm.successMessage = "";
        vm.isBusy = false;
        vm.DeviceTypes = Global.DeviceTypes;

        vm.locations = [];
        vm.models = [];

        //load all locations
        $http.get("/api/location/getAllLocations")
                .then(function (response) {
                    //success
                    angular.copy(response.data, vm.locations);
                    vm.newDevice.locationName = vm.locations[0].name;
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to load locations. ";
                })
                .finally(function () {
                    vm.isBusy = false
                });

        //load all models
        $http.get("/api/model/getAllModels")
                .then(function (response) {
                    //success
                    angular.copy(response.data, vm.models);
                    vm.newDevice.modelName = vm.models[0].name;
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to load models. ";
                })
                .finally(function () {
                    vm.isBusy = false
                });

        //add new device adds the binding object newDevice
        vm.addNewDevice = function () {
            vm.isBusy = true;

            $http.post("/api/device/addNewDevice", vm.newDevice)
                .then(function (response) {
                    //success
                    vm.newDevice = {};
                    vm.newDevice.type = "0";
                    vm.successMessage = "Device added successfully.";
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to add device.";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }
    }
})();