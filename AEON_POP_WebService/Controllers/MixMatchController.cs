﻿using System;
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
    public class MixMatchController : ControllerBase
    {
        public MixMatchController(AppDb db)
        {
            Db = db;
        }
        public AppDb Db { get; }

        [HttpGet("${tungay}${denngay}")]
        public async Task<IActionResult> GetOne(string tungay, string denngay)
        {
            await Db.Connection.OpenAsync();
            var query = new MixMatchQuery(Db);
            var result = await query.FindOneAsync(tungay, denngay);
            if (result is null)
                return new NotFoundResult();
            return new OkObjectResult(result);
        }

        [HttpGet]
        public async Task<IActionResult> GetAll()
        {
            await Db.Connection.OpenAsync();
            var query = new MixMatchQuery(Db);
            var result = await query.FindAllAsync();
            if (result is null)
                return new NotFoundResult();
            return new OkObjectResult(result);
        }
    }
}
