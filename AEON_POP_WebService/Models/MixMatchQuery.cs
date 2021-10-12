using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;

namespace AEON_POP_WebService.Models
{
    public class MixMatchQuery
    {
        public AppDb Db { get; }

        public MixMatchQuery(AppDb db)
        {
            Db = db;
        }
        public async Task<List<MixMatch>> FindOneAsync(string tungay, string denngay)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT T0.* FROM mix_match T0 INNER JOIN profit_files_log T1 ON T0.FILE_ID = T1.FILE_ID WHERE STR_TO_DATE(T1.FILE_DATE, '%Y%m%d') BETWEEN @tungay AND @denngay limit 1000";
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

        public async Task<List<MixMatch>> FindAllAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `mix_match` limit 1000";
            return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        private async Task<List<MixMatch>> ReadAllAsync(DbDataReader reader)
        {
            var posts = new List<MixMatch>();
            using (reader)
            {
                while (await reader.ReadAsync())
                {
                    var post = new MixMatch(Db)
                    {
                        MIX_MATCH_ID = reader.GetInt32(0),
                        PROMO_NO = reader.GetString(1),
                        PROMO_TYPE = reader.GetString(2),
                        PROMO_DESC = reader.GetString(3),
                        STATUS = reader.GetString(4),
                        MAX_OR_PARTIAL = reader.GetString(5),
                        START_DATE = reader.GetString(6),
                        START_TIME = reader.GetString(7),
                        END_DATE = reader.GetString(8),
                        END_TIME = reader.GetString(9),
                        TTL_PROMO_QTY = reader.GetString(10),
                        TTL_PROMO_PRICE = reader.GetString(11),
                        PLU_COUNT = reader.GetString(12),
                        EVENT_ID = reader.GetInt32(13),
                        STORE = reader.GetString(14),
                        SKU = reader.GetString(15),
                        SEQ = reader.GetInt32(16),
                        NORMAL_PRICE = reader.GetInt32(17),
                        SELL_UOM = reader.GetString(18),
                        PROMO_QTY = reader.GetInt32(19),
                        FOC_QTY = reader.GetInt32(20),
                        PROMO_PRICE = reader.GetInt32(21),
                        FOC_SKU = reader.GetString(22),
                        MODIFIED_DATE = reader.GetString(23),
                        FILE_ID = reader.GetString(24),
                    };
                    posts.Add(post);
                }
            }
            return posts;
        }
    }
}
