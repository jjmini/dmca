using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Identity;
using QualitySystem.Models;
using QualitySystem.ViewModels;

// For more information on enabling MVC for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace QualitySystem.Controllers.Auth
{
    [Route("api/auth")]
    public class AuthController : Controller
    {
        private SignInManager<QualitySystemUser> _signInManager;

        public AuthController(SignInManager<QualitySystemUser> signInManager)
        {
            _signInManager = signInManager;
        }

        [HttpPost]
        [Route("login")]
        public async Task<JsonResult> Login([FromBody]LoginViewModel login)
        {
            if (ModelState.IsValid)
            {
                var signInResult = await _signInManager.PasswordSignInAsync(login.Username, login.Password, true, false);
                if (signInResult.Succeeded)
                    return Json(true);
            }
            return Json(false);
        }

        [HttpPost]
        [Route("logout")]
        public void Logout()
        {
            if (User.Identity.IsAuthenticated)
                _signInManager.SignOutAsync();
        }
    }
}
