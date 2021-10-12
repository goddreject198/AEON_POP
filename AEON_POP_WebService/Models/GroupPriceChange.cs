using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AEON_POP_WebService.Models
{
    public class GroupPriceChange
    {
        internal AppDb Db { get; set; }

        public GroupPriceChange()
        {
        }

        internal GroupPriceChange(AppDb db)
        {
            Db = db;
        }

        public string PRICE_CHANGE_NO { get; set; }
        public string TRANS_TYPE { get; set; }
        public string START_DATE { get; set; }
        public string START_TIME { get; set; }
        public string END_DATE { get; set; }
        public string END_TIME { get; set; }
        public string CATEGORY { get; set; }
        public string STORE { get; set; }
        public int EVENT_ID { get; set; }
        public string EXCLUDE_SEASON_ID { get; set; }
        public string PRICE_CHANGE_TYPE { get; set; }
        public int PRICE_CHANGE_TYPE_VALUE { get; set; }
        public string REASON { get; set; }
        public string PROMOTION_TYPE { get; set; }
        public string STATUS { get; set; }
        public string CREATED_DATE { get; set; }
        public string MODIFIED_DATE { get; set; }
        public string FILE_ID { get; set; }
    }
}
