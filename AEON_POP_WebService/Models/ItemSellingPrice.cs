using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AEON_POP_WebService.Models
{
    public class ItemSellingPrice
    {
        internal AppDb Db { get; set; }

        public ItemSellingPrice()
        {
        }

        internal ItemSellingPrice(AppDb db)
        {
            Db = db;
        }

        public int selling_price_no { get; set; }
        public string store { get; set; }
        public string sku { get; set; }
        public string description { get; set; }
        public string current_price { get; set; }
        public string promotion_flag { get; set; }
        public string promotion_retail { get; set; }
        public string member_retail { get; set; }
        public string member_promotion_flag { get; set; }
        public string member_promotion_retail { get; set; }
        public string file_id { get; set; }

    }
}
