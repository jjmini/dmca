using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QualitySystem.Models
{
    public class QualitySystemContextSeedData
    {
        private QualitySystemContext _context;
        private ILogger<QualitySystemContextSeedData> _logger;
        private UserManager<QualitySystemUser> _userManager;

        public QualitySystemContextSeedData(QualitySystemContext context, ILogger<QualitySystemContextSeedData> logger, UserManager<QualitySystemUser> userManager)
        {
            _context = context;
            _logger = logger;
            _userManager = userManager;
        }

        public async Task SeedData()
        {
            if (!_context.Users.Any())
            {
                var user = new QualitySystemUser()
                {
                    UserName = "aliasmael",
                    Email = "alilion22@ymail.com"
                };
                
                await _userManager.CreateAsync(user, "Ali@fci.7");
                await _context.SaveChangesAsync();
                
            }
            if (!_context.Devices.Any())
            {
                var firstDevice = new Device()
                {
                    SerialNumber = "XCF0056",
                    Type = DeviceType.PC,
                    Spare = false,
                    Broken = false,
                    Components = new List<Component>()
                    {
                        new Component() {Type = type.MotherBoared, Description = "G41" },
                        new Component() {Type = type.HardDiskDrive, Description = "250GB" },
                        new Component() {Type = type.RAM, Description = "4GB" },
                    },
                    Location = new Location()
                    {
                        Name = "Lab 1"
                    },
                    Model = new Model()
                    {
                        Name = "HP",
                        DateEntered = DateTime.UtcNow,
                        PageNumber = "144/1"
                    }
                };

                _context.Locations.Add(firstDevice.Location);
                _context.Models.Add(firstDevice.Model);
                _context.Devices.Add(firstDevice);
                _context.Components.AddRange(firstDevice.Components);
                await _context.SaveChangesAsync();
            }
        }
    }
}
