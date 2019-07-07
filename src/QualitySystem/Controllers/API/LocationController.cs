using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using QualitySystem.Models;
using AutoMapper;
using QualitySystem.ViewModels;
using Microsoft.AspNetCore.Authorization;

// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace QualitySystem.Controllers.API
{
    [Authorize]
    [Route("api/location")]
    public class LocationController : Controller
    {
        private ILogger<LocationController> _logger;
        private IQualitySystemRepository _repository;

        public LocationController(IQualitySystemRepository repository, ILogger<LocationController> logger)
        {
            _repository = repository;
            _logger = logger;
        }

        // GET: /<controller>/
        [HttpGet]
        [Route("getAllLocations")]
        public JsonResult GetAllLocations()
        {
            try
            {
                var locations = _repository.GetAllLocations();
                var result = Mapper.Map<IEnumerable<LocationViewModel>>(locations);
                
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
        [Route("addNewLocation")]
        public async Task<IActionResult> AddNewLocation([FromBody]LocationViewModel location)
        {
            if (ModelState.IsValid)
            {
                Location newLocation = Mapper.Map<Location>(location);
                
                _repository.AddNewLocation(newLocation);

                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/location/{newLocation}", newLocation);
                }

            }
            return BadRequest("Failed to add new location: location model not valid");
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("deleteLocation")]
        public async Task<IActionResult> DeleteLocation([FromBody]int id)
        {
            _repository.DeleteLocation(id);
            if (await _repository.SaveChangesAsync())
            {
                return Created($"api/location/{id}", id);
            }

            return BadRequest("Failed to delete the location.");
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("updateLocation")]
        public async Task<IActionResult> UpdateLocation([FromBody]LocationViewModel location)
        {
            if (ModelState.IsValid)
            {
                var locationToUpdate = Mapper.Map<Location>(location);
                _repository.UpdateLocation(locationToUpdate);
                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/location/{location}", location);
                }
            }
            return BadRequest("Failed to update the location.");
        }
    }
}
