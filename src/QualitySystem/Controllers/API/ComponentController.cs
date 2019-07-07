using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using QualitySystem.ViewModels;
using QualitySystem.Models;
using Microsoft.Extensions.Logging;
using AutoMapper;
using Microsoft.AspNetCore.Authorization;

// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace QualitySystem.Controllers.API
{
    [Authorize]
    [Route("api/component")]
    public class ComponentController : Controller
    {
        private ILogger<ComponentController> _logger;
        private IQualitySystemRepository _repository;

        public ComponentController(IQualitySystemRepository repository, ILogger<ComponentController> logger)
        {
            _repository = repository;
            _logger = logger;
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("addNewComponent")]
        public async Task<IActionResult> AddNewComponent([FromBody]ComponentViewModel component)
        {
            if (ModelState.IsValid)
            {
                Component newComponent = Mapper.Map<Component>(component);

                _repository.AddNewComponent(newComponent);

                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/component/{newComponent}", newComponent);
                }

            }
            return BadRequest("Failed to add new component: component model not valid");
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("deleteComponent")]
        public async Task<IActionResult> DeleteComponent([FromBody]int componentId)
        {
            _repository.DeleteComponent(componentId);
            if (await _repository.SaveChangesAsync())
            {
                return Created($"api/component/{componentId}", componentId);
            }

            return BadRequest("Failed to delete the component.");
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("updateComponent")]
        public async Task<IActionResult> UpdateComponent([FromBody]ComponentViewModel newComponent)
        {
            Component component = Mapper.Map<Component>(newComponent);
            _repository.UpdateComponent(component);
            try
            {
                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/component/{component}", component);
                }
            }
            catch (Exception) { }

            return BadRequest("Failed to Update the component.");
        }
    }
}
