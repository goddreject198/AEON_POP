using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AEON_POP_WinForm
{
    class Info_Class
    {
        public class ItemSellPrice
        {
            public string STORE { get; set; }
            public string SKU { get; set; }
            public string DESCRIPTION { get; set; }
            public string CURRENT_PRICE { get; set; }
            public string PROMOTION_FLAG { get; set; }
            public string PROMOTION_RETAIL { get; set; }
            public string MEMBER_RETAIL { get; set; }
            public string MEMBER_PROMOTION_FLAG { get; set; }
            public string MEMBER_PROMOTION_RETAIL { get; set; }
            public string FILE_ID { get; set; }
        }
    }
}
