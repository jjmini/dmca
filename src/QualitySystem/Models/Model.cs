using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QualitySystem.Models
{
    public class Model
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public DateTime DateEntered { get; set; }
        public string PageNumber { get; set; }

        public ICollection<Device> Devices { get; set; }
    }
}
