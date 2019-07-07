using qualitysystem.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace qualitysystem.ViewModels
{
    public enum DeviceType { PC, Laptop };
    public class DeviceViewModel
    {
        public int ID { get; set; }
        public DeviceType Type { get; set; }
        public string SerialNumber { get; set; }
        public bool Broken { get; set; }
        public bool Spare { get; set; }
        public string LocationName { get; set; }
        public string ModelName { get; set; }
        public DateTime ModelDateEntered { get; set; }
        public string ModelPageNumber { get; set; }

        public ICollection<ComponentViewModel> Components { get; set; }
    }
}
