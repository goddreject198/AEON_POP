using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AEON_POP_WebService.Models
{
    public class MixMatch
    {
        internal AppDb Db { get; set; }

        public MixMatch()
        {
        }

        internal MixMatch(AppDb db)
        {
            Db = db;
        }

        public int MIX_MATCH_ID { get; set; }
        public string PROMO_NO { get; set; }
        public string PROMO_TYPE { get; set; }
        public string PROMO_DESC { get; set; }
        public string STATUS { get; set; }
        public string MAX_OR_PARTIAL { get; set; }
        public string START_DATE { get; set; }
        public string START_TIME { get; set; }
        public string END_DATE { get; set; }
        public string END_TIME { get; set; }
        public string TTL_PROMO_QTY { get; set; }
        public string TTL_PROMO_PRICE { get; set; }
        public string PLU_COUNT { get; set; }
        public int EVENT_ID { get; set; }
        public string STORE { get; set; }
        public string SKU { get; set; }
        public int SEQ { get; set; }
        public int NORMAL_PRICE { get; set; }
        public string SELL_UOM { get; set; }
        public int PROMO_QTY { get; set; }
        public int FOC_QTY { get; set; }
        public int PROMO_PRICE { get; set; }
        public string FOC_SKU { get; set; }
        public string MODIFIED_DATE { get; set; }
        public string FILE_ID { get; set; }
    }
}
