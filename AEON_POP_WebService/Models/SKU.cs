using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using MySqlConnector;
using System.Text.Json.Serialization;

namespace AEON_POP_WebService.Models
{
    public class SKU
    {
        internal AppDb Db { get; set; }
        [JsonIgnore]
        public string P_Sku { get; set; }
        [JsonIgnore]
        public string P_Store { get; set; }
        [JsonIgnore]
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

        public string sku_code { get; set; }
        public string item_desc_vnm { get; set; }
        public string sku_type { get; set; }
        public string purchase_method { get; set; }
        public string business_unit { get; set; }
        public string store { get; set; }
        public string line_id { get; set; }
        public string division_id { get; set; }
        public string group_id { get; set; }
        public string dept_id { get; set; }
        public string category_id { get; set; }
        public string sub_category { get; set; }
        public string colour_size_grid { get; set; }
        public string colour { get; set; }
        public string size_id { get; set; }
        public string barcode { get; set; }
        public string pop1_desc_vnm { get; set; }
        public string pop2_desc_vnm { get; set; }
        public string selling_point1 { get; set; }
        public string selling_point2 { get; set; }
        public string selling_point3 { get; set; }
        public string selling_point4 { get; set; }
        public string selling_point5 { get; set; }
        public string current_price { get; set; }
        public string retail_uom { get; set; }
        public string status { get; set; }
        public string date_create { get; set; }
        public string modified_date { get; set; }
        public string closing_stock_qty { get; set; }
        public string closing_stock_retail { get; set; }
        public string file_id { get; set; }
    }
}
