using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;

namespace AEON_POP_WebService.Models
{
    public class SKUQuery
    {
        public AppDb Db { get; }

        public SKUQuery(AppDb db)
        {
            Db = db;
        }

        public async Task<SKU> FindOneAsync(string sku, string store)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `sku` WHERE SKU_CODE = @sku AND STORE = @store";
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@sku",
                DbType = DbType.String,
                Value = sku,
            });
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@store",
                DbType = DbType.String,
                Value = store,
            });
            var result = await ReadAllAsync(await cmd.ExecuteReaderAsync());
            return result.Count > 0 ? result[0] : null;
            //return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        public async Task<List<SKU>> FindAllAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `sku`";
            return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        private async Task<List<SKU>> ReadAllAsync(DbDataReader reader)
        {
            var posts = new List<SKU>();
            using (reader)
            {
                while (await reader.ReadAsync())
                {
                    var post = new SKU(Db)
                    {
                        sku_code = reader.GetString(0),
                        item_desc_vnm = reader.GetString(1),
                        sku_type = reader.GetString(2),
                        purchase_method = reader.GetString(3),
                        business_unit = reader.GetString(4),
                        store = reader.GetString(5),
                        line_id = reader.GetString(6),
                        division_id = reader.GetString(7),
                        group_id = reader.GetString(8),
                        dept_id = reader.GetString(9),
                        category_id = reader.GetString(10),
                        sub_category = reader.GetString(11),
                        colour_size_grid = reader.GetString(12),
                        colour = reader.GetString(13),
                        size_id = reader.GetString(14),
                        barcode = reader.GetString(15),
                        pop1_desc_vnm = reader.GetString(16),
                        pop2_desc_vnm = reader.GetString(17),
                        selling_point1 = reader.GetString(18),
                        selling_point2 = reader.GetString(19),
                        selling_point3 = reader.GetString(20),
                        selling_point4 = reader.GetString(21),
                        selling_point5 = reader.GetString(22),
                        current_price = reader.GetString(23),
                        retail_uom = reader.GetString(24),
                        status = reader.GetString(25),
                        date_create = reader.GetString(26),
                        modified_date = reader.GetString(27),
                        closing_stock_qty = reader.GetString(28),
                        closing_stock_retail = reader.GetString(29),
                        file_id = reader.GetString(30),
                    };
                    posts.Add(post);
                }
            }
            
            return posts;
        }
    }
}
