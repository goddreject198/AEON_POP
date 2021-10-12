using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;


namespace AEON_POP_WebService.Models
{
    public class ItemSellingPriceQuery
    {
        public AppDb Db { get; }

        public ItemSellingPriceQuery(AppDb db)
        {
            Db = db;
        }
        public async Task<List<ItemSellingPrice>> FindOneAsync(string tungay, string denngay)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT T0.* FROM item_selling_price T0 INNER JOIN profit_files_log T1 ON T0.FILE_ID = T1.FILE_ID WHERE STR_TO_DATE(T1.FILE_DATE, '%Y%m%d') BETWEEN @tungay AND @denngay limit 1000";
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

        public async Task<List<ItemSellingPrice>> FindAllAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `item_selling_price`";
            return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        private async Task<List<ItemSellingPrice>> ReadAllAsync(DbDataReader reader)
        {
            var posts = new List<ItemSellingPrice>();
            using (reader)
            {
                while (await reader.ReadAsync())
                {
                    var post = new ItemSellingPrice(Db)
                    {
                        SELLING_PRICE_NO = reader.GetString(0),
                        STORE = reader.GetString(1),
                        SKU = reader.GetString(2),
                        DESCRIPTION = reader.GetString(3),
                        CURRENT_PRICE = reader.GetString(4),
                        PROMOTION_FLAG = reader.GetString(5),
                        PROMOTION_RETAIL = reader.GetString(6),
                        MEMBER_RETAIL = reader.GetString(7),
                        MEMBER_PROMOTION_FLAG = reader.GetString(8),
                        MEMBER_PROMOTION_RETAIL = reader.GetString(9),
                        FILE_ID = reader.GetString(10),
                    };
                    posts.Add(post);
                }
            }
            return posts;
        }
    }
}
