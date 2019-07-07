using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QualitySystem.Models
{
    public class Location
    {
        public int ID { get; set; }
        public string Name { get; set; }

        public ICollection<Device> Devices { get; set; }
    }
}
