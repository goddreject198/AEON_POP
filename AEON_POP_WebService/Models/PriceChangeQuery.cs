using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;

namespace AEON_POP_WebService.Models
{
    public class PriceChangeQuery
    {
        public AppDb Db { get; }

        public PriceChangeQuery(AppDb db)
        {
            Db = db;
        }
        public async Task<List<PriceChange>> FindOneAsync(string tungay, string denngay)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT T0.* FROM pricechange T0 INNER JOIN profit_files_log T1 ON T0.FILE_ID = T1.FILE_ID WHERE STR_TO_DATE(T1.FILE_DATE, '%Y%m%d') BETWEEN @tungay AND @denngay limit 1000";
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

        public async Task<List<PriceChange>> FindAllAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `pricechange`";
            return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        private async Task<List<PriceChange>> ReadAllAsync(DbDataReader reader)
        {
            var posts = new List<PriceChange>();
            using (reader)
            {
                while (await reader.ReadAsync())
                {
                    var post = new PriceChange(Db)
                    {
                        PRICE_ID = reader.GetInt32(0),
                        PRICE_CHANGE_NO = reader.GetString(1),
                        DEPARTMENT = reader.GetString(2),
                        TRANS_TYPE = reader.GetString(3),
                        REASON = reader.GetString(4),
                        EVENT_ID = reader.GetString(5),
                        PRICE_CHANGE_TYPE = reader.GetString(6),
                        PRICE_CHANGE_TYPE_VALUE = reader.GetString(7),
                        PROMOTION_TYPE = reader.GetString(8),
                        START_DATE = reader.GetString(9),
                        DAILY_START_TIME = reader.GetString(10),
                        END_DATE = reader.GetString(11),
                        DAILY_END_TIME = reader.GetString(12),
                        STATUS = reader.GetString(13),
                        STORE = reader.GetString(14),
                        SKU = reader.GetString(15),
                        LAST_SELL_PRICE = reader.GetString(16),
                        LAST_SELL_UNIT = reader.GetString(17),
                        NEW_SELL_PRICE = reader.GetString(18),
                        CREATED_DATE = reader.GetString(19),
                        MODIFIED_DATE = reader.GetString(20),
                        FILE_ID = reader.GetString(21),
                    };
                    posts.Add(post);
                }
            }
            return posts;
        }
    }
}
