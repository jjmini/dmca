using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace qualitysystem.ViewModels
{
    public enum type { PowerSupply, MotherBoared, HardDiskDrive, RAM, CDROMDrive, FloppyDrive, Monitor, Keyboared, Mouse }
    public class ComponentViewModel
    {
        public int ID { get; set; }
        public type Type { get; set; }
        public string Description { get; set; }
        public int DeviceId { get; set; }
    }
}
