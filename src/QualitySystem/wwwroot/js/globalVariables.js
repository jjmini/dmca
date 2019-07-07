var appModule = angular.module('appModule');

//decleare global variables
appModule.factory('Global', function () {
    return {
        //types of devices and components
        DeviceTypes: {
            0: "PC",
            1: "Laptop"
        },
        ComponentTypes: {
            0: "Power Supply",
            1: "Motherboared",
            2: "Hard Disk Drive",
            3: "RAM",
            4: "CD-ROM Drive",
            5: "Floppy Drive",
            6: "Monitor",
            7: "Keyboared",
            8: "Mouse"
        }
    };
});