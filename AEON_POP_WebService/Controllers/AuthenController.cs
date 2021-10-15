using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;

namespace AEON_POP_WebService.Controllers
{
    [ServiceFilter(typeof(ClientIpCheckActionFilter))]
    [Route("api/[controller]")]
    [ApiController]
    public class AuthenController : ControllerBase
    {
        private readonly IConfiguration _config;
        public AuthenController(IConfiguration config)
        {
            _config = config;
        }

        [HttpPost("token")]
        public ActionResult getAccessToken(LoginModel model)
        {
            var tokenHandler = new JwtSecurityTokenHandler();
            //check user/pwd
            if (model.Username == "admin" && model.Password == "admin")
            {
                //generate token
                var Claims = new List<Claim>();
                Claims.Add(new Claim(ClaimTypes.Name, "abc"));
                Claims.Add(new Claim(ClaimTypes.Role, "admin"));

                //create token by handler
                var key = Encoding.UTF8.GetBytes(_config["JwtSettings:Key"]);
                var securityKey = new SymmetricSecurityKey(key);
                var credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256Signature);

                // create token by handler
                var access_token = new JwtSecurityToken(
                    issuer: _config["JwtSettings:Issuer"],
                    audience: _config["JwtSettings:Issuer"],
                    claims: Claims,
                    expires: DateTime.Now.AddDays(3),
                    signingCredentials: credentials
                    );

                return Ok(tokenHandler.WriteToken(access_token));
            }
            return BadRequest("Invalid user!");
        }

        public class LoginModel
        {
            public string Username { get; set; }
            public string Password { get; set; }
        }
    }
}
