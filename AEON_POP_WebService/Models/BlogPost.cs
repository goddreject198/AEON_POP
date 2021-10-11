using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using MySqlConnector;

namespace AEON_POP_WebService.Models
{
    public class BlogPost
    {
        public int File_ID { get; set; }
        public string File_Date { get; set; }
        public string File_Name { get; set; }
        public string File_SysDate { get; set; }
        public string File_SysTime { get; set; }
        public string File_Messsage { get; set; }

        internal AppDb Db { get; set; }

        public BlogPost()
        {
        }

        internal BlogPost(AppDb db)
        {
            Db = db;
        }
    }
}
