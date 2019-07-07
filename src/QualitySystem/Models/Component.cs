using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace QualitySystem.Models
{
    public enum type { PowerSupply, MotherBoared, HardDiskDrive, RAM, CDROMDrive, FloppyDrive, Monitor, Keyboared, Mouse}
    public class Component
    {
        public int ID { get; set; }
        public type Type { get; set; }
        public string Description { get; set; }
        public int DeviceID { get; set; }

        public Device Device { get; set; }
    }
}
