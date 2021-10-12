using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AEON_POP_WebService.Models
{
    public class Hamper
    {
        internal AppDb Db { get; set; }

        public Hamper()
        {
        }

        internal Hamper(AppDb db)
        {
            Db = db;
        }

        public int HAMPER_CODE { get; set; }
        public string PACK_SKU { get; set; }
        public string DESCRIPTION { get; set; }
        public string PACK_TYPE { get; set; }
        public string SKU { get; set; }
        public int QTY_PER_SKU { get; set; }
        public string QTY_UOM { get; set; }
        public string STORE { get; set; }
        public string DECORATION_FLAG { get; set; }
        public string STATUS { get; set; }
        public string MODIFIED_DATE { get; set; }
        public string FILE_ID { get; set; }
        
    }
}
