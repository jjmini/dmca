(function () {
    "use strict";

    var appModule = angular.module('appModule', ["simpleControls", "ngRoute"]);

    appModule.config(function ($routeProvider, $locationProvider) {
        $routeProvider.when('/', {
            title: 'Quality System',
            templateUrl: '/pages/index.html'
        });

        //main routes
        $routeProvider.when('/devices', {
            title: 'Devices',
            templateUrl: '/pages/devices/index.html',
            controller: 'devicesMainController',
            controllerAs: 'vm'
        });

        $routeProvider.when('/locations', {
            title: 'Locations',
            templateUrl: '/pages/locations/index.html',
            controller: 'locationsMainController',
            controllerAs: 'vm'
        });

        $routeProvider.when('/models', {
            title: 'Models',
            templateUrl: '/pages/models/index.html',
            controller: 'modelsMainController',
            controllerAs: 'vm'
        });

        $routeProvider.when('/reports', {
            title: 'Reports',
            templateUrl: '/pages/reports/index.html'
        });

        $routeProvider.when('/login', {
            title: 'Login',
            templateUrl: '/pages/login/index.html',
            controller: 'loginController',
            controllerAs: 'vm'
        });

        //routes for devices
        $routeProvider.when('/devices/add', {
            title: 'Add New Device',
            templateUrl: '/pages/devices/addNewDevice.html',
            controller: 'addNewDeviceController',
            controllerAs: 'vm'
        });

        $routeProvider.when('/devices/check', {
            title: 'Devices Check',
            templateUrl: '/pages/devices/check.html'
        });

        //reports routes
        $routeProvider.when('/reports/deviceCard', {
            title: 'Device Card Report',
            templateUrl: '/reports/devicecard.html'
        });

        $routeProvider.otherwise({ redirectTo: "/" });

        // use the HTML5 History API
        $locationProvider.html5Mode(true);
    });

    //change site title
    appModule.run(['$rootScope', '$route', function ($rootScope, $route) {
        $rootScope.$on('$routeChangeSuccess', function () {
            document.title = $route.current.title;
        });
    }]);

    //redirect to login if user is not authorised
    appModule.run(['$rootScope', '$location', 'Auth', function ($rootScope, $location, Auth) {
        $rootScope.$on('$routeChangeStart', function (event) {
            if (!Auth.isLoggedIn()) {
                $location.path('/login');
            }
        });
    }]);
})();