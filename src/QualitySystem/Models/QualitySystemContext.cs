using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace qualitysystem.Models
{
    public class qualitysystemContext : IdentityDbContext<qualitysystemUser>
    {
        public qualitysystemContext(DbContextOptions options) : base(options)
        {

        }

        public DbSet<Device> Devices { get; set; }
        public DbSet<Component> Components { get; set; }
        public DbSet<Location> Locations { get; set; }
        public DbSet<Model> Models { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);

            optionsBuilder.UseSqlServer("Server=(localdb)\\MSSQLLocalDB;Database=qualitysystemDB; Trusted_Connection=true;MultipleActiveResultSets=true;");
        }
    }
}
