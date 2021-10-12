using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using MySqlConnector;

namespace AEON_POP_WebService.Models
{
    public class HamperQuery
    {
        public AppDb Db { get; }

        public HamperQuery(AppDb db)
        {
            Db = db;
        }
        public async Task<List<Hamper>> FindOneAsync(string tungay, string denngay)
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT T0.* FROM hamper T0 INNER JOIN profit_files_log T1 ON T0.FILE_ID = T1.FILE_ID WHERE STR_TO_DATE(T1.FILE_DATE, '%Y%m%d') BETWEEN @tungay AND @denngay limit 1000";
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

        public async Task<List<Hamper>> FindAllAsync()
        {
            using var cmd = Db.Connection.CreateCommand();
            cmd.CommandText = @"SELECT * FROM `hamper` limit 1000";
            return await ReadAllAsync(await cmd.ExecuteReaderAsync());
        }

        private async Task<List<Hamper>> ReadAllAsync(DbDataReader reader)
        {
            var posts = new List<Hamper>();
            using (reader)
            {
                while (await reader.ReadAsync())
                {
                    var post = new Hamper(Db)
                    {
                        HAMPER_CODE = reader.GetInt32(0),
                        PACK_SKU = reader.GetString(1),
                        DESCRIPTION = reader.GetString(2),
                        PACK_TYPE = reader.GetString(3),
                        SKU = reader.GetString(4),
                        QTY_PER_SKU = reader.GetInt32(5),
                        QTY_UOM = reader.GetString(6),
                        STORE = reader.GetString(7),
                        DECORATION_FLAG = reader.GetString(8),
                        STATUS = reader.GetString(9),
                        MODIFIED_DATE = reader.GetString(10),
                        FILE_ID = reader.GetString(11),
                    };
                    posts.Add(post);
                }
            }
            return posts;
        }
    }
}
