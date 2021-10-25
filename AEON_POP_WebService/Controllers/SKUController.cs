using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using AEON_POP_WebService.Models;
using Microsoft.AspNetCore.Authorization;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace AEON_POP_WebService.Controllers
{
    [ServiceFilter(typeof(ClientIpCheckActionFilter))]
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class SKUController : ControllerBase
    {
        public SKUController(AppDb db)
        {
            Db = db;
        }
        public AppDb Db { get; }

        //[HttpGet("${sku}${store}")]
        [HttpGet("getone")]
        public async Task<IActionResult> GetOne(ParameterSKU parameter)
        {
            await Db.Connection.OpenAsync();
            var query = new SKUQuery(Db);
            var result = await query.FindOneAsync(parameter.SKU, parameter.Store);
            if (result is null)
                return new NotFoundResult();
            return new OkObjectResult(result);
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            await Db.Connection.OpenAsync();
            var query = new SKUQuery(Db);
            var result = await query.FindAllAsync();
            if (result is null)
                return new NotFoundResult();
            return new OkObjectResult(result);
        }

        [HttpPost("update")]
        public async Task<IActionResult> UpdateOne(ParameterUpdateSKU parameter)
        {
            await Db.Connection.OpenAsync();
            var query = new SKUQuery(Db);
            var result = await query.FindOneAsync(parameter.SKU, parameter.Store);
            if (result is null)
                return new NotFoundResult();
            result.P_Sku = parameter.SKU;
            result.P_Store = parameter.Store;
            result.P_Status = parameter.Status;

            await result.UpdateAsync();
            return new OkObjectResult(result);
        }

        public class ParameterSKU
        {
            public string SKU { get; set; }
            public string Store { get; set; }
        }
        public class ParameterUpdateSKU
        {
            public string SKU { get; set; }
            public string Store { get; set; }
            public string Status { get; set; }
        }
    }
}
