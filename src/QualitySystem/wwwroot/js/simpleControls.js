(function () {
    "use strict";

    var simpleControls = angular.module("simpleControls", [])

    //wait cursor
    simpleControls.directive("waitCursor", waitCursor);
    function waitCursor() {
        return {
            templateUrl: '/directives/waitCursor.html'
        };
    }


    //add new component
    simpleControls.directive("addNewComponent", addNewComponent);
    function addNewComponent() {
        return {
            templateUrl: '/directives/devices/addNewComponent.html'
        };
    }

    //edit component
    simpleControls.directive("editComponent", editComponent);
    function editComponent() {
        return {
            templateUrl: '/directives/devices/editComponent.html'
        };
    }

    //edit device
    simpleControls.directive("editDevice", editDevice);
    function editDevice() {
        return {
            templateUrl: '/directives/devices/editDevice.html'
        };
    }

    //Add location
    simpleControls.directive("addNewLocation", addNewLocation);
    function addNewLocation() {
        return {
            templateUrl: '/directives/locations/addNewLocation.html'
        };
    }

    //Update location
    simpleControls.directive("updateLocation", updateLocation);
    function updateLocation() {
        return {
            templateUrl: '/directives/locations/updateLocation.html'
        };
    }

    //Add model
    simpleControls.directive("addNewModel", addNewModel);
    function addNewModel() {
        return {
            templateUrl: '/directives/models/addNewModel.html'
        };
    }

    //Update model
    simpleControls.directive("updateModel", updateModel);
    function updateModel() {
        return {
            templateUrl: '/directives/models/updateModel.html'
        };
    }
})();