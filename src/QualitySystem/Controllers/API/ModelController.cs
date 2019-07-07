using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using qualitysystem.Models;
using AutoMapper;
using qualitysystem.ViewModels;
using Microsoft.AspNetCore.Authorization;

// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace qualitysystem.Controllers.API
{
    [Authorize]
    [Route("api/model")]
    public class ModelController : Controller
    {
        private ILogger<LocationController> _logger;
        private IqualitysystemRepository _repository;

        public ModelController(IqualitysystemRepository repository, ILogger<LocationController> logger)
        {
            _repository = repository;
            _logger = logger;
        }

        // GET: /<controller>/
        [HttpGet]
        [Route("getAllModels")]
        public JsonResult GetAllModels()
        {
            try
            {
                var models = _repository.GetAllModels();
                var result = Mapper.Map<IEnumerable<ModelViewModel>>(models);
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
        [Route("addNewModel")]
        public async Task<IActionResult> AddNewModel([FromBody]ModelViewModel model)
        {
            if (ModelState.IsValid)
            {
                Model newModel = Mapper.Map<Model>(model);

                _repository.AddNewModel(newModel);

                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/model/{newModel}", newModel);
                }

            }
            return BadRequest("Failed to add new model: model not valid");
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("deleteModel")]
        public async Task<IActionResult> DeleteModel([FromBody]int id)
        {
            _repository.DeleteModel(id);
            if (await _repository.SaveChangesAsync())
            {
                return Created($"api/model/{id}", id);
            }

            return BadRequest("Failed to delete the model.");
        }

        // POST: /<controller>/
        [HttpPost]
        [Route("updateModel")]
        public async Task<IActionResult> UpdateLocation([FromBody]ModelViewModel model)
        {
            if (ModelState.IsValid)
            {
                var modelToUpdate = Mapper.Map<Model>(model);
                _repository.UpdateModel(modelToUpdate);
                if (await _repository.SaveChangesAsync())
                {
                    return Created($"api/model/{model}", model);
                }
            }
            return BadRequest("Failed to update the model.");
        }
    }
}
