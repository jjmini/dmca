(function () {
    "use strict";

    angular.module("appModule").controller("devicesMainController", devicesMainController);

    function devicesMainController($http, Global) {
        var vm = this;
        vm.devices = [];
        vm.errorMessage = "";
        vm.isBusy = true;
        vm.ComponentTypes = Global.ComponentTypes;
        vm.DeviceTypes = Global.DeviceTypes;

        vm.newComponent = {
            type: "0",
            description: "",
            deviceId: ""
        };

        vm.componentToEdit = {};
        vm.deviceToEdit = {};

        //get data of all devices
        $http.get("/api/device/getAllDevices")
                .then(function (response) {
                    //success
                    angular.copy(response.data, vm.devices)
                    angular.forEach(vm.devices, function (device, key) {
                        device.type = vm.DeviceTypes[device.type],
                        angular.forEach(device.components, function (component, key) {
                            component.type = vm.ComponentTypes[component.type]
                        });
                    });
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to load data. ";
                })
                .finally(function () {
                    vm.isBusy = false
                });

        //delete device
        vm.deleteDevice = function (id) {
            vm.isBusy = true;
            $http.post("/api/device/deleteDevice", id)
                .then(function (response) {
                    //success
                    vm.devices = vm.devices.filter(function (item) {
                        return item.id !== id;
                    });
                    vm.successMessage = "Device deleted successfully.";
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to delete device.";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }

        //keep track of the serial number of the device that new component added to
        vm.BeginAddNewComponent = function (deviceId) {
            vm.newComponent.deviceId = deviceId;
        }

        //add new component
        vm.addNewComponent = function () {
            vm.isBusy = true;
            vm.successMessage = vm.newComponent;
            $http.post("/api/component/addNewComponent", JSON.stringify(vm.newComponent))
                .then(function (response) {
                    //success
                    var component = response.data;
                    component.type = vm.ComponentTypes[component.type];
                    vm.successMessage = "component added successfully.";
                    vm.devices.filter(function (item) {
                        return item.id === vm.newComponent.deviceId;
                    })[0].components.push(component);
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to add new component.";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }

        //Delete component
        vm.deleteComponent = function (deviceId, componentId) {
            vm.isBusy = true;
            $http.post("/api/component/deleteComponent", componentId)
                .then(function (response) {
                    //success
                    var device = vm.devices.filter(function (item) {
                        return item.id == deviceId;
                    })[0];
                    device.components = device.components.filter(function (item) {
                        return item.id != componentId;
                    });
                    vm.successMessage = "Component deleted successfully.";
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to delete component.";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }

        //Edit component
        vm.BeginComponentEditing = function (id) {
            angular.forEach(vm.devices, function (device, key) {
                angular.forEach(device.components, function (component, key) {
                    if (component.id == id) {
                        angular.copy(component, vm.componentToEdit);
                        angular.forEach(vm.ComponentTypes, function (type, key) {
                            if (type == vm.componentToEdit.type) {
                                vm.componentToEdit.type = key;
                            }
                        });
                    }
                });
            });
        }

        //Done Edit component
        vm.DoneComponentEditing = function () {
            $http.post("api/component/updateComponent", vm.componentToEdit)  
                .then(function (response) {
                    //success
                    var device = vm.devices.filter(function (item) {
                        return item.id == vm.componentToEdit.deviceId;
                    })[0];
                    angular.forEach(device.components, function (component, key) {
                        if (component.id == vm.componentToEdit.id) {
                            angular.copy(vm.componentToEdit, component);
                            component.type = vm.ComponentTypes[component.type];
                        }
                    });
                    vm.componentToEdit = {};
                    vm.successMessage = "Component updated successfully.";
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to update component.";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }

        //Edit component
        vm.BeginDeviceEditing = function (id) {
            var device = vm.devices.filter(function (item) {
                return item.id == id;
            })[0];

            angular.copy(device, vm.deviceToEdit);
            angular.forEach(vm.DeviceTypes, function (type, key) {
                if (type == vm.deviceToEdit.type) {
                    vm.deviceToEdit.type =  key;
                }
            });
        }

        //Done Edit component
        vm.DoneDeviceEditing = function () {
            var deviceToSend = {};
            angular.copy(vm.deviceToEdit, deviceToSend);
            deviceToSend.components = [];
            $http.post("api/device/updateDevice", JSON.stringify(deviceToSend))
                .then(function (response) {
                    //success
                    var device = vm.devices.filter(function (item) {
                        return item.id == vm.deviceToEdit.id;
                    })[0];
                    vm.deviceToEdit.type = vm.DeviceTypes[vm.deviceToEdit.type];
                    angular.copy(vm.deviceToEdit, device);
                    vm.deviceToEdit = {};
                    vm.successMessage = "Device updated successfully.";
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to update device.";
                })
                .finally(function () {
                    vm.isBusy = false
                });
        }
    }
})();