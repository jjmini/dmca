using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using QualitySystem.Models;
using Microsoft.Extensions.Logging;
using AutoMapper;
using QualitySystem.ViewModels;
using Microsoft.AspNetCore.Authorization;

// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace QualitySystem.Controllers.API
{
    [Authorize]
    [Route("api/device")]
    public class DevicesController : Controller
    {
        private ILogger<DevicesController> _logger;
        private IQualitySystemRepository _repository;

        public DevicesController(IQualitySystemRepository repository, ILogger<DevicesController> logger)
        {
            _repository = repository;
            _logger = logger;
        }

        // GET: /<controller>/
        [HttpGet]
        [Route("getAllDevices")]
        public JsonResult GetAllDevices()
        {
            try
            {
                var devices = _repository.GetAllDevices();
                var result = Mapper.Map<IEnumerable<DeviceViewModel>>(devices);
                return Json(result);
            }
            catch (Exception)
            {
                _logger.LogError($"Failed to get data.");
                return Json("Bad request");
            }
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("addNewDevice")]
        public async Task<IActionResult> AddNewDevice([FromBody]DeviceViewModel device)
        {
            if (ModelState.IsValid)
            {
                Device newDevice = Mapper.Map<Device>(device);
                
                newDevice.LocationID = _repository
                                            .GetLocationByName(device.LocationName)
                                            .ID;

                newDevice.ModelID = _repository
                                                .GetModelByName(device.ModelName)
                                                .ID;

                _repository.AddNewDevice(newDevice);

                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/device/{newDevice}", newDevice);
                }

            }
            return BadRequest("Failed to add new device: device model not valid");
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("deleteDevice")]
        public async Task<IActionResult> DeleteDevice([FromBody]int id)
        {
            _repository.DeleteDevice(id);
            if (await _repository.SaveChangesAsync())
            {
                return Created($"api/device/{id}", id);
            }

            return BadRequest("Failed to delete the device.");
        }
        
        // POST: /<controller>/
        [HttpPost]
        [Route("updateDevice")]
        public async Task<IActionResult> UpdateDevice([FromBody]DeviceViewModel device)
        {
            if (ModelState.IsValid)
            {
                var deviceToUpdate = Mapper.Map<Device>(device);
                deviceToUpdate.LocationID = _repository
                                            .GetLocationByName(device.LocationName)
                                            .ID;

                deviceToUpdate.ModelID = _repository
                                                .GetModelByName(device.ModelName)
                                                .ID;
                _repository.UpdateDevice(deviceToUpdate);
                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/device/{device}", device);
                }
            }
            return BadRequest("Failed to update the device.");
        }
    }
}
