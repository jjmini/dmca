using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using QualitySystem.ViewModels;

namespace QualitySystem.Models
{
    public interface IQualitySystemRepository
    {
        IEnumerable<Device> GetAllDevices();
        void AddNewDevice(Device device);
        Task<bool> SaveChangesAsync();
        Device GetDeviceBySerialNumber(string serialNumber);
        Location GetLocationByName(string locationName);
        Model GetModelByName(string modelName);
        void DeleteDevice(int id);
        void AddNewComponent(Component component);
        void DeleteComponent(int componentId);
        void UpdateComponent(Component component);
        void UpdateDevice(Device device);
        IEnumerable<Location> GetAllLocations();
        IEnumerable<Model> GetAllModels();
        void UpdateLocation(Location locationToUpdate);
        void DeleteLocation(int id);
        void AddNewLocation(Location newLocation);
        void AddNewModel(Model newModel);
        void DeleteModel(int id);
        void UpdateModel(Model modelToUpdate);
    }
}
