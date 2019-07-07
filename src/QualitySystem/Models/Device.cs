using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QualitySystem.Models
{
    public enum DeviceType { PC, Laptop};
    public class Device
    {
        public int ID { get; set; }
        public DeviceType Type { get; set; }
        public string SerialNumber { get; set; }
        public bool Broken { get; set; }
        public bool Spare { get; set; }
        public int LocationID { get; set; }
        public int ModelID { get; set; }

        public Location Location { get; set; }
        public Model Model { get; set; }

        public ICollection<Component> Components { get; set; }
        
    }
}
