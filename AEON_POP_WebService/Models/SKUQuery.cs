using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        public async Task<List<SKU>> FindOneAsync(string tungay, string denngay)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `sku` WHERE STR_TO_DATE(DATE_CREATE, '%Y%m%d') BETWEEN @tungay AND @denngay";
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@tungay",
                DbType = DbType.String,
                Value = tungay,
            });
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@denngay",
                DbType = DbType.String,
                Value = denngay,
            });
            //var result = await ReadAllAsync(await cmd.ExecuteReaderAsync());
            //return result.Count > 0 ? result[0] : null;
            return await ReadAllAsync(await cmd.ExecuteReaderAsync());
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
                        SKU_CODE = reader.GetString(0),
                        ITEM_DESC_VNM = reader.GetString(1),
                        SKU_TYPE = reader.GetString(2),
                        PURCHASE_METHOD = reader.GetString(3),
                        BUSINESS_UNIT = reader.GetString(4),
                        STORE = reader.GetString(5),
                        LINE_ID = reader.GetString(6),
                        DIVISION_ID = reader.GetString(7),
                        GROUP_ID = reader.GetString(8),
                        DEPT_ID = reader.GetString(9),
                        CATEGORY_ID = reader.GetString(10),
                        SUB_CATEGORY = reader.GetString(11),
                        COLOUR_SIZE_GRID = reader.GetString(12),
                        COLOUR = reader.GetString(13),
                        SIZE_ID = reader.GetString(14),
                        BARCODE = reader.GetString(15),
                        POP1_DESC_VNM = reader.GetString(16),
                        POP2_DESC_VNM = reader.GetString(17),
                        SELLING_POINT1 = reader.GetString(18),
                        SELLING_POINT2 = reader.GetString(19),
                        SELLING_POINT3 = reader.GetString(20),
                        SELLING_POINT4 = reader.GetString(21),
                        SELLING_POINT5 = reader.GetString(22),
                        CURRENT_PRICE = reader.GetString(23),
                        RETAIL_UOM = reader.GetString(24),
                        STATUS = reader.GetString(25),
                        DATE_CREATE = reader.GetString(26),
                        MODIFIED_DATE = reader.GetString(27),
                        CLOSING_STOCK_QTY = reader.GetString(28),
                        CLOSING_STOCK_RETAIL = reader.GetString(29),
                        FILE_ID = reader.GetString(30),
                    };
                    posts.Add(post);
                }
            }
            return posts;
        }
    }
}
