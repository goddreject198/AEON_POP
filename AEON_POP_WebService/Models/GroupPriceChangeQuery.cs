using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;


namespace AEON_POP_WebService.Models
{
    public class GroupPriceChangeQuery
    {
        public AppDb Db { get; }

        public GroupPriceChangeQuery(AppDb db)
        {
            Db = db;
        }
        public async Task<List<GroupPriceChange>> FindOneAsync(string tungay, string denngay)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT T0.* FROM group_pricechange T0 INNER JOIN profit_files_log T1 ON T0.FILE_ID = T1.FILE_ID WHERE STR_TO_DATE(T1.FILE_DATE, '%Y%m%d') BETWEEN @tungay AND @denngay limit 1000";
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

        public async Task<List<GroupPriceChange>> FindAllAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `group_pricechange`";
            return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        private async Task<List<GroupPriceChange>> ReadAllAsync(DbDataReader reader)
        {
            var posts = new List<GroupPriceChange>();
            using (reader)
            {
                while (await reader.ReadAsync())
                {
                    var post = new GroupPriceChange(Db)
                    {
                        PRICE_CHANGE_NO = reader.GetString(0),
                        TRANS_TYPE = reader.GetString(1),
                        START_DATE = reader.GetString(2),
                        START_TIME = reader.GetString(3),
                        END_DATE = reader.GetString(4),
                        END_TIME = reader.GetString(5),
                        CATEGORY = reader.GetString(6),
                        STORE = reader.GetString(7),
                        EVENT_ID = reader.GetInt32(8),
                        EXCLUDE_SEASON_ID = reader.GetString(9),
                        PRICE_CHANGE_TYPE = reader.GetString(10),
                        PRICE_CHANGE_TYPE_VALUE = reader.GetInt32(11),
                        REASON = reader.GetString(12),
                        PROMOTION_TYPE = reader.GetString(13),
                        STATUS = reader.GetString(14),
                        CREATED_DATE = reader.GetString(15),
                        MODIFIED_DATE = reader.GetString(16),
                        FILE_ID = reader.GetString(17),
                    };
                    posts.Add(post);
                }
            }
            return posts;
        }
    }
}
