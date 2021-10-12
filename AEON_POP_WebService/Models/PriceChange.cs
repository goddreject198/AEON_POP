using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AEON_POP_WebService.Models
{
    public class PriceChange
    {
        internal AppDb Db { get; set; }

        public PriceChange()
        {
        }

        internal PriceChange(AppDb db)
        {
            Db = db;
        }

        public int PRICE_ID { get; set; }
        public string PRICE_CHANGE_NO { get; set; }
        public string DEPARTMENT { get; set; }
        public string TRANS_TYPE { get; set; }
        public string REASON { get; set; }
        public string EVENT_ID { get; set; }
        public string PRICE_CHANGE_TYPE { get; set; }
        public string PRICE_CHANGE_TYPE_VALUE { get; set; }
        public string PROMOTION_TYPE { get; set; }
        public string START_DATE { get; set; }
        public string DAILY_START_TIME { get; set; }
        public string END_DATE { get; set; }
        public string DAILY_END_TIME { get; set; }
        public string STATUS { get; set; }
        public string STORE { get; set; }
        public string SKU { get; set; }
        public string LAST_SELL_PRICE { get; set; }
        public string LAST_SELL_UNIT { get; set; }
        public string NEW_SELL_PRICE { get; set; }
        public string CREATED_DATE { get; set; }
        public string MODIFIED_DATE { get; set; }
        public string FILE_ID { get; set; }
        
    }
}
