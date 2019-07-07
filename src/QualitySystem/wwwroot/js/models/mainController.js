(function () {
    "use strict";

    angular.module("appModule").controller("modelsMainController", modelsMainController);

    function modelsMainController($http, Global) {
        var vm = this;
        vm.models = [];
        vm.errorMessage = "";
        vm.successMessage = "";
        vm.isBusy = true;

        vm.newModel = {};
        vm.modelToEdit = {};

        //get data of all models
        $http.get("/api/model/getAllModels")
                .then(function (response) {
                    //success
                    angular.copy(response.data, vm.models)
                }, function (error) {
                    //failure
                    vm.errorMessage = "Failed to load data. ";
                })
                .finally(function () {
                    vm.isBusy = false
                });

        //Add new model
        vm.AddNewModel = function (model) {
            //get data of all models
            $http.post("/api/model/AddNewModel", model)
                    .then(function (response) {
                        //success
                        vm.successMessage = "Model added successfully.";
                        vm.models.push(response.data);
                    }, function (error) {
                        //failure
                        vm.errorMessage = "Failed to add model. ";
                    })
                    .finally(function () {
                        vm.isBusy = false
                    });
        }

        //Delete model
        vm.DeleteModel = function (modelId) {
            //get data of all models
            $http.post("/api/model/DeleteModel", modelId)
                    .then(function (response) {
                        //success
                        vm.successMessage = "Model deleted successfully.";
                        vm.models = vm.models.filter(function (item) {
                            return item.id !== modelId;
                        });
                    }, function (error) {
                        //failure
                        vm.errorMessage = "Failed to delete model. ";
                    })
                    .finally(function () {
                        vm.isBusy = false
                    });
        }

        vm.BeginModelEditing = function (id) {
            var model = vm.models.filter(function (item) {
                return item.id == id;
            })[0];

            angular.copy(model, vm.modelToEdit);

            vm.modelToEdit.dateEntered = new Date(vm.modelToEdit.dateEntered);
        }

        //Update model
        vm.UpdateModel = function (model) {
            //get data of all models
            $http.post("/api/model/UpdateModel", model)
                    .then(function (response) {
                        //success
                        vm.successMessage = "Model updated successfully.";
                        var tempModel = vm.models.filter(function (item) {
                            return item.id == model.id;
                        })[0];
                        angular.copy(model, tempModel);
                        model = {};
                    }, function (error) {
                        //failure
                        vm.errorMessage = "Failed to upadate model. ";
                    })
                    .finally(function () {
                        vm.isBusy = false
                    });
        }
    }
})();