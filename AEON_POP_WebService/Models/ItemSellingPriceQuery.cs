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
                        SELLING_PRICE_NO = reader.GetInt32(0),
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
