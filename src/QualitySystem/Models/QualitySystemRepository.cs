using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using QualitySystem.ViewModels;

namespace QualitySystem.Models
{
    public class QualitySystemRepository : IQualitySystemRepository
    {
        private QualitySystemContext _context;
        private ILogger<QualitySystemRepository> _logger;

        public QualitySystemRepository(QualitySystemContext context, ILogger<QualitySystemRepository> logger)
        {
            _context = context;
            _logger = logger;
        }

        public void AddNewDevice(Device device)
        {
            _context.Devices.Add(device);
        }

        public void DeleteDevice(int id)
        {
            var device = _context.Devices.Where(t => t.ID == id).FirstOrDefault();
            _context.Devices.Remove(device);
        }
        
        public Device GetDeviceBySerialNumber(string serialNumber)
        {
            return _context.Devices
                                .Include(t => t.Components)
                                .Include(t => t.Model)
                                .Include(t => t.Location)
                                .Where(t => t.SerialNumber == serialNumber)
                                .FirstOrDefault();
        }

        public IEnumerable<Device> GetAllDevices()
        {
            return _context.Devices
                                .Include(t => t.Components)
                                .Include(t => t.Model)
                                .Include(t => t.Location)
                                .ToList();
        }

        public Location GetLocationByName(string locationName)
        {
            return _context.Locations
                                    .Where(t => t.Name == locationName)
                                    .FirstOrDefault();
        }

        public Model GetModelByName(string modelName)
        {
            return _context.Models
                                .Where(t => t.Name == modelName)
                                .FirstOrDefault();
        }

        public async Task<bool> SaveChangesAsync()
        {
            return await (_context.SaveChangesAsync()) > 0;
        }

        public void AddNewComponent(Component component)
        {
            _context.Components.Add(component);
        }

        public void DeleteComponent(int componentId)
        {
            Component component = _context.Components.Where(t => t.ID == componentId).FirstOrDefault();
            _context.Components.Remove(component);
        }

        public void UpdateComponent(Component component)
        {
            _context.Components.Update(component);
        }

        public void UpdateDevice(Device device)
        {
            _context.Devices.Update(device);
        }

        public IEnumerable<Location> GetAllLocations()
        {
            return _context.Locations.ToList();
        }

        public IEnumerable<Model> GetAllModels()
        {
            return _context.Models.ToList();
        }

        public void UpdateLocation(Location locationToUpdate)
        {
            _context.Locations.Update(locationToUpdate);
        }

        public void DeleteLocation(int id)
        {
            var location = _context.Locations.Where(t => t.ID == id).FirstOrDefault();
            _context.Locations.Remove(location);
        }

        public void AddNewLocation(Location newLocation)
        {
            _context.Locations.Add(newLocation);
        }

        public void AddNewModel(Model newModel)
        {
            _context.Models.Add(newModel);
        }

        public void DeleteModel(int id)
        {
            var model = _context.Models.Where(t => t.ID == id).FirstOrDefault();
            _context.Models.Remove(model);
        }

        public void UpdateModel(Model modelToUpdate)
        {
            _context.Models.Update(modelToUpdate);
        }
    }
}
