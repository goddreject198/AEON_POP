using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;

namespace AEON_POP_WebService.Models
{
    public class SKU
    {
        internal AppDb Db { get; set; }

        public string P_Sku { get; set; }
        public string P_Store { get; set; }
        public string P_Status { get; set; }


        public SKU()
        {
        }

        internal SKU(AppDb db)
        {
            Db = db;
        }

        public async Task UpdateAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"UPDATE `sku` SET STATUS = @status WHERE SKU_CODE = @sku AND STORE = @store;";
            BindParams(cmd);
            BindId(cmd);
            await cmd.ExecuteNonQueryAsync();
        }
        private void BindId(MySqlCommand cmd)
        {
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@sku",
                DbType = DbType.String,
                Value = P_Sku,
            });
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@store",
                DbType = DbType.String,
                Value = P_Store,
            });
        }
        private void BindParams(MySqlCommand cmd)
        {
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@status",
                DbType = DbType.String,
                Value = P_Status,
            });
            
        }

        public string SKU_CODE { get; set; }
        public string ITEM_DESC_VNM { get; set; }
        public string SKU_TYPE { get; set; }
        public string PURCHASE_METHOD { get; set; }
        public string BUSINESS_UNIT { get; set; }
        public string STORE { get; set; }
        public string LINE_ID { get; set; }
        public string DIVISION_ID { get; set; }
        public string GROUP_ID { get; set; }
        public string DEPT_ID { get; set; }
        public string CATEGORY_ID { get; set; }
        public string SUB_CATEGORY { get; set; }
        public string COLOUR_SIZE_GRID { get; set; }
        public string COLOUR { get; set; }
        public string SIZE_ID { get; set; }
        public string BARCODE { get; set; }
        public string POP1_DESC_VNM { get; set; }
        public string POP2_DESC_VNM { get; set; }
        public string SELLING_POINT1 { get; set; }
        public string SELLING_POINT2 { get; set; }
        public string SELLING_POINT3 { get; set; }
        public string SELLING_POINT4 { get; set; }
        public string SELLING_POINT5 { get; set; }
        public string CURRENT_PRICE { get; set; }
        public string RETAIL_UOM { get; set; }
        public string STATUS { get; set; }
        public string DATE_CREATE { get; set; }
        public string MODIFIED_DATE { get; set; }
        public string CLOSING_STOCK_QTY { get; set; }
        public string CLOSING_STOCK_RETAIL { get; set; }
        public string FILE_ID { get; set; }
    }
}
