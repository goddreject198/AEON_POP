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
        public async Task<ItemSellingPrice> FindOneAsync(string store, string sku)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `item_sell_price` WHERE STORE = @store AND SKU = @sku ORDER BY File_ID DESC limit 1;";
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@store",
                DbType = DbType.String,
                Value = store,
            });
            cmd.Parameters.Add(new MySqlParameter
            {
                ParameterName = "@sku",
                DbType = DbType.String,
                Value = sku,
            });
            var result = await ReadAllAsync(await cmd.ExecuteReaderAsync());
            return result.Count > 0 ? result[0] : null;
            //return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        public async Task<List<ItemSellingPrice>> FindAllAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `item_sell_price` limit 1000";
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
                        selling_price_no = reader.GetInt32(0),
                        store = reader.GetString(1),
                        sku = reader.GetString(2),
                        description = reader.GetString(3),
                        current_price = reader.GetString(4),
                        promotion_flag = reader.GetString(5),
                        promotion_retail = reader.GetString(6),
                        member_retail = reader.GetString(7),
                        member_promotion_flag = reader.GetString(8),
                        member_promotion_retail = reader.GetString(9),
                        file_id = reader.GetString(10),
                    };
                    posts.Add(post);
                }
            }
            return posts;
        }
    }
}
