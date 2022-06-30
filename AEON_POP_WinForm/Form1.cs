using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Net;
using WinSCP;
using System.Collections;
using RestSharp;

namespace AEON_POP_WinForm
{
    public partial class Form1 : Form
    {
        //private string connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3};old guids=true;", "139.180.214.252", "aeon_pop", "fpt", "fptpop@2021");
        //private string connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3};old guids=true;", "139.180.214.252", "aeon_pop_prd", "fpt", "fptpop@2021");
        private string connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3};old guids=true;", "localhost", "aeon_pop", "root", "qs0123123");

        //khai báo backgroundprocess
        private BackgroundWorker myWorker_ItemSellPrice = new BackgroundWorker();
        private BackgroundWorker myWorker_PostDataToMobile = new BackgroundWorker();

        public Form1()
        {
            InitializeComponent();
            //khai báo properties của background process 
            myWorker_ItemSellPrice.DoWork += new DoWorkEventHandler(myWorker_ItemSellPrice_DoWork);
            myWorker_ItemSellPrice.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_ItemSellPrice_RunWorkerCompleted);
            myWorker_ItemSellPrice.ProgressChanged += new ProgressChangedEventHandler(myWorker_ItemSellPrice_ProgressChanged);
            myWorker_ItemSellPrice.WorkerReportsProgress = true;
            myWorker_ItemSellPrice.WorkerSupportsCancellation = true;
            //khai báo properties của background process 
            myWorker_PostDataToMobile.DoWork += new DoWorkEventHandler(myWorker_PostDataToMobile_DoWork);
            myWorker_PostDataToMobile.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_PostDataToMobile_RunWorkerCompleted);
            myWorker_PostDataToMobile.ProgressChanged += new ProgressChangedEventHandler(myWorker_PostDataToMobile_ProgressChanged);
            myWorker_PostDataToMobile.WorkerReportsProgress = true;
            myWorker_PostDataToMobile.WorkerSupportsCancellation = true;
        }

        private void myWorker_PostDataToMobile_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_PostDataToMobile_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_PostDataToMobile_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                MySqlConnection connection = new MySqlConnection(connectionString);
                var sql_get_SKU = String.Format("SELECT distinct * FROM aeon_pop.sku_code_temp;");
                connection.Open();
                var cmd_get_fileID = new MySqlCommand(sql_get_SKU, connection);
                MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                MyAdapter.SelectCommand = cmd_get_fileID;
                DataTable dTable_SKUCode = new DataTable();
                MyAdapter.Fill(dTable_SKUCode);
                connection.Close();

                var client = new RestClient("http://45.77.34.122/thirdparty/sku/downloadtomobile");
                client.Timeout = -1;
                var request = new RestRequest(Method.POST);
                request.AddHeader("Authorization", "6d9bf625-7f54-452d-bc37-6e89f702c17a");
                request.AddHeader("Content-Type", "application/json");

                string body = "{\"skus\": [";
                string sku_code = "";
                for (int i = 0; i < dTable_SKUCode.Rows.Count; i++)
                {
                    sku_code += "\"" + dTable_SKUCode.Rows[i][0].ToString() + "\",";
                }
                sku_code = sku_code.Substring(0, sku_code.Length - 1);
                body += sku_code + "]}";

                request.AddParameter("application/json", body, ParameterType.RequestBody);
                IRestResponse response = client.Execute(request);
                MessageBox.Show(response.StatusCode + ": " + response.Content);

                if (response.IsSuccessful)
                {
                    var sql_delete_sku_code = String.Format("DELETE FROM `aeon_pop`.`sku_code_temp` WHERE SKU_CODE IN ({0});" , sku_code);
                    connection.Open();
                    MySqlCommand comm_sql_delete_sku_code = connection.CreateCommand();
                    comm_sql_delete_sku_code.CommandText = sql_delete_sku_code;
                    int kq = comm_sql_delete_sku_code.ExecuteNonQuery();
                    connection.Close();

                    MessageBox.Show(sql_delete_sku_code + ": " + kq);
                }    
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void myWorker_ItemSellPrice_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            
        }

        private void myWorker_ItemSellPrice_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            
        }

        private void myWorker_ItemSellPrice_DoWork(object sender, DoWorkEventArgs e)
        {
            string log_fileid = "";
            int lineeeeee = 0;
            try
            { 
                MySqlConnection connection = new MySqlConnection(connectionString);

                DirectoryInfo info = new DirectoryInfo(@"C:\Profit_Receive\");
                List<string> filesPath = info.GetFiles("*.csv")//.Where(x => x.CreationTime.Date == DateTime.Today.AddDays(0))
                                                  .Select(x => x.FullName)
                                                  .ToList();

                foreach (string pathtg in filesPath)
                {
                    string fileExt = Path.GetExtension(pathtg);
                    {
                        if (fileExt.CompareTo(".csv") == 0)
                        {
                            string date_now = DateTime.Now.ToString("yyyyMMdd");
                            string time_now = DateTime.Now.ToString("hhmmss");

                            //insert info file to table PROFIT_FILES
                            string filename = Path.GetFileName(pathtg);

                            if (filename.Length >= 6)
                            {
                                if (filename.Substring(0, 6) == "STORE_")
                                {
                                    #region STORE_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_Store = String.Format(@"INSERT INTO `aeon_pop`.`store_temp`
                                                                                    (`STORE_ID`,`STORE_NAME`,`STORE_BU`,`STORE_TAX_REG`,`STORE_DATE_OPEN`,`STORE_REGION`,`DELETED`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string STORE_ID = rows[0].ToString();

                                            string STORE_NAME = "";
                                            string temp_STORE_NAME = rows[1].ToString();
                                            if (temp_STORE_NAME.Contains("\""))
                                            {
                                                STORE_NAME = temp_STORE_NAME.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                STORE_NAME = rows[1].ToString();
                                            }
                                            string STORE_BU = rows[2].ToString();
                                            string STORE_TAX_REG = rows[3].ToString();
                                            string STORE_DATE_OPEN = rows[4].ToString();
                                            string STORE_REGION = rows[5].ToString();
                                            string DELETED = rows[6].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_Store += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}""),"
                                                                                        , STORE_ID, STORE_NAME, STORE_BU, STORE_TAX_REG, STORE_DATE_OPEN, STORE_REGION, DELETED, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data_Store = connection.CreateCommand();
                                                sql_insert_data_Store = sql_insert_data_Store.Substring(0, sql_insert_data_Store.Length - 1);
                                                comm_sql_insert_data_Store.CommandText = sql_insert_data_Store;
                                                int kq = comm_sql_insert_data_Store.ExecuteNonQuery();
                                                connection.Close();

                                                sql_insert_data_Store = String.Format(@"INSERT INTO `aeon_pop`.`store_temp`
                                                                                    (`STORE_ID`,`STORE_NAME`,`STORE_BU`,`STORE_TAX_REG`,`STORE_DATE_OPEN`,`STORE_REGION`,`DELETED`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data_Store = connection.CreateCommand();
                                            sql_insert_data_Store = sql_insert_data_Store.Substring(0, sql_insert_data_Store.Length - 1);
                                            comm_sql_insert_data_Store.CommandText = sql_insert_data_Store;
                                            int kq = comm_sql_insert_data_Store.ExecuteNonQuery();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 5)
                            {
                                if (filename.Substring(0, 5) == "LINE_")
                                {
                                    #region LINE_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_Line = String.Format(@"INSERT INTO `aeon_pop`.`line_temp`
                                                                                    (`LINE_ID`,`LINE_NAME`,`DELETED`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string LINE_ID = rows[0].ToString();

                                            string LINE_NAME = "";
                                            string temp_LINE_NAME = rows[1].ToString();
                                            if (temp_LINE_NAME.Contains("\""))
                                            {
                                                LINE_NAME = temp_LINE_NAME.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                LINE_NAME = rows[1].ToString();
                                            }
                                            string DELETED = rows[2].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_Line += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}""),"
                                                                                        , LINE_ID, LINE_NAME, DELETED, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data_Store = connection.CreateCommand();
                                                sql_insert_data_Line = sql_insert_data_Line.Substring(0, sql_insert_data_Line.Length - 1);
                                                comm_sql_insert_data_Store.CommandText = sql_insert_data_Line;
                                                int kq = comm_sql_insert_data_Store.ExecuteNonQuery();
                                                connection.Close();

                                                sql_insert_data_Line = String.Format(@"INSERT INTO `aeon_pop`.`line_temp`
                                                                                    (`LINE_ID`,`LINE_NAME`,`DELETED`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data_Store = connection.CreateCommand();
                                            sql_insert_data_Line = sql_insert_data_Line.Substring(0, sql_insert_data_Line.Length - 1);
                                            comm_sql_insert_data_Store.CommandText = sql_insert_data_Line;
                                            int kq = comm_sql_insert_data_Store.ExecuteNonQuery();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 9)
                            {
                                if (filename.Substring(0, 9) == "DIVISION_")
                                {
                                    #region DIVISION_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_Division = String.Format(@"INSERT INTO `aeon_pop`.`division_temp`
                                                                                    (`DIV_ID`,`DIV_NAME`,`LINE_ID`,`DELETED`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string DIV_ID = rows[0].ToString();

                                            string DIV_NAME = "";
                                            string temp_DIV_NAME = rows[1].ToString();
                                            if (temp_DIV_NAME.Contains("\""))
                                            {
                                                DIV_NAME = temp_DIV_NAME.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                DIV_NAME = rows[1].ToString();
                                            }
                                            string LINE_ID = rows[2].ToString();
                                            string DELETED = rows[3].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_Division += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}""),"
                                                                                        , DIV_ID, DIV_NAME, LINE_ID, DELETED, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data_Store = connection.CreateCommand();
                                                sql_insert_data_Division = sql_insert_data_Division.Substring(0, sql_insert_data_Division.Length - 1);
                                                comm_sql_insert_data_Store.CommandText = sql_insert_data_Division;
                                                int kq = comm_sql_insert_data_Store.ExecuteNonQuery();
                                                connection.Close();

                                                sql_insert_data_Division = String.Format(@"INSERT INTO `aeon_pop`.`division_temp`
                                                                                    (`DIV_ID`,`DIV_NAME`,`LINE_ID`,`DELETED`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data_Store = connection.CreateCommand();
                                            sql_insert_data_Division = sql_insert_data_Division.Substring(0, sql_insert_data_Division.Length - 1);
                                            comm_sql_insert_data_Store.CommandText = sql_insert_data_Division;
                                            int kq = comm_sql_insert_data_Store.ExecuteNonQuery();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 6)
                            {
                                if (filename.Substring(0, 6) == "GROUP_")
                                {
                                    #region GROUP_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_Group = String.Format(@"INSERT INTO `aeon_pop`.`group_temp`
                                                                                    (`GROUP_ID`,`GROUP_NAME`,`DIV_ID`,`DELETED`,`LINE_ID`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string GROUP_ID = rows[0].ToString();

                                            string GROUP_NAME = "";
                                            string temp_GROUP_NAME = rows[1].ToString();
                                            if (temp_GROUP_NAME.Contains("\""))
                                            {
                                                GROUP_NAME = temp_GROUP_NAME.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                GROUP_NAME = rows[1].ToString();
                                            }
                                            string DIV_ID = rows[2].ToString();
                                            string DELETED = rows[3].ToString();
                                            string LINE_ID = rows[4].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_Group += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}""),"
                                                                                        , GROUP_ID, GROUP_NAME, DIV_ID, DELETED, LINE_ID, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                                sql_insert_data_Group = sql_insert_data_Group.Substring(0, sql_insert_data_Group.Length - 1);
                                                comm_sql_insert_data.CommandText = sql_insert_data_Group;
                                                int kq = comm_sql_insert_data.ExecuteNonQuery();
                                                connection.Close();

                                                sql_insert_data_Group = String.Format(@"INSERT INTO `aeon_pop`.`group_temp`
                                                                                    (`GROUP_ID`,`GROUP_NAME`,`DIV_ID`,`DELETED`,`LINE_ID`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                            sql_insert_data_Group = sql_insert_data_Group.Substring(0, sql_insert_data_Group.Length - 1);
                                            comm_sql_insert_data.CommandText = sql_insert_data_Group;
                                            int kq = comm_sql_insert_data.ExecuteNonQuery();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 5)
                            {
                                if (filename.Substring(0, 5) == "DEPT_")
                                {
                                    #region DEPT_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_Dept = String.Format(@"INSERT INTO `aeon_pop`.`department_temp`
                                                                                    (`DEPT_ID`,`DEPT_NAME`,`GROUP_ID`,`DELETED`,`DEPT_TYPE`,`PERISHABLE`,`COSTING_METHOD`,`MATERIAL_FLAG`,`REBATE_TYPE`
                                                                                    ,`ORDER_CHKLST_CTRL`,`PREFIX`,`EXPIRY_DATE_CTRL`,`PRINT_ORDER_BOOK`,`CDO_DEPT`,`MOMMY_CARD_DEPT`,`FOOD_CARD_DEPT`,`AUTO_REPLENISH_DEPT`
                                                                                    ,`SCHEMATIC_DEPT`,`PRINT_REMARK_IN_PO`,`PRINT_REMARK_IN_OC`,`DLOAD_PO_REMARK_TO_EDI`,`INDICATION_IN_ORD_CHKLST`,`DIV_ID`,`LINE_ID`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string DEPT_ID = rows[0].ToString();

                                            string DEPT_NAME = "";
                                            string temp_DEPT_NAME = rows[1].ToString();
                                            if (temp_DEPT_NAME.Contains("\""))
                                            {
                                                DEPT_NAME = temp_DEPT_NAME.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                DEPT_NAME = rows[1].ToString();
                                            }
                                            string GROUP_ID = rows[2].ToString();
                                            string DELETED = rows[3].ToString();
                                            string DEPT_TYPE = rows[4].ToString();
                                            string PERISHABLE = rows[5].ToString();
                                            string COSTING_METHOD = rows[6].ToString();
                                            string MATERIAL_FLAG = rows[7].ToString();
                                            string REBATE_TYPE = rows[8].ToString();
                                            string ORDER_CHKLST_CTRL = rows[9].ToString();
                                            string PREFIX = rows[10].ToString();
                                            string EXPIRY_DATE_CTRL = rows[11].ToString();
                                            string PRINT_ORDER_BOOK = rows[12].ToString();
                                            string CDO_DEPT = rows[13].ToString();
                                            string MOMMY_CARD_DEPT = rows[14].ToString();
                                            string FOOD_CARD_DEPT = rows[15].ToString();
                                            string AUTO_REPLENISH_DEPT = rows[16].ToString();
                                            string SCHEMATIC_DEPT = rows[17].ToString();
                                            string PRINT_REMARK_IN_PO = rows[18].ToString();
                                            string PRINT_REMARK_IN_OC = rows[19].ToString();
                                            string DLOAD_PO_REMARK_TO_EDI = rows[20].ToString();
                                            string INDICATION_IN_ORD_CHKLST = rows[21].ToString();
                                            string DIV_ID = rows[22].ToString();
                                            string LINE_ID = rows[23].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_Dept += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                    , ""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                    , ""{20}"",""{21}"",""{22}"",""{23}"",""{24}""),"
                                                                                        , DEPT_ID, DEPT_NAME, GROUP_ID, DELETED, DEPT_TYPE, PERISHABLE, COSTING_METHOD, MATERIAL_FLAG, REBATE_TYPE, ORDER_CHKLST_CTRL
                                                                                        , PREFIX, EXPIRY_DATE_CTRL, PRINT_ORDER_BOOK, CDO_DEPT, MOMMY_CARD_DEPT, FOOD_CARD_DEPT, AUTO_REPLENISH_DEPT, SCHEMATIC_DEPT, PRINT_REMARK_IN_PO
                                                                                        , PRINT_REMARK_IN_OC, DLOAD_PO_REMARK_TO_EDI, INDICATION_IN_ORD_CHKLST, DIV_ID, LINE_ID, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                                sql_insert_data_Dept = sql_insert_data_Dept.Substring(0, sql_insert_data_Dept.Length - 1);
                                                comm_sql_insert_data.CommandText = sql_insert_data_Dept;
                                                int kq = comm_sql_insert_data.ExecuteNonQuery();
                                                connection.Close();

                                                sql_insert_data_Dept = String.Format(@"INSERT INTO `aeon_pop`.`department_temp`
                                                                                    (`DEPT_ID`,`DEPT_NAME`,`GROUP_ID`,`DELETED`,`DEPT_TYPE`,`PERISHABLE`,`COSTING_METHOD`,`MATERIAL_FLAG`,`REBATE_TYPE`
                                                                                    ,`ORDER_CHKLST_CTRL`,`PREFIX`,`EXPIRY_DATE_CTRL`,`PRINT_ORDER_BOOK`,`CDO_DEPT`,`MOMMY_CARD_DEPT`,`FOOD_CARD_DEPT`,`AUTO_REPLENISH_DEPT`
                                                                                    ,`SCHEMATIC_DEPT`,`PRINT_REMARK_IN_PO`,`PRINT_REMARK_IN_OC`,`DLOAD_PO_REMARK_TO_EDI`,`INDICATION_IN_ORD_CHKLST`,`DIV_ID`,`LINE_ID`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                            sql_insert_data_Dept = sql_insert_data_Dept.Substring(0, sql_insert_data_Dept.Length - 1);
                                            comm_sql_insert_data.CommandText = sql_insert_data_Dept;
                                            int kq = comm_sql_insert_data.ExecuteNonQuery();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 9)
                            {
                                if (filename.Substring(0, 9) == "CATEGORY_")
                                {
                                    #region CATEGORY_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_Category = String.Format(@"INSERT INTO `aeon_pop`.`category_temp`
                                                                                    (`CATEGORY_ID`,`CATEGORY_NAME`,`DEPT_ID`,`DELETED`,`AUTO_PA`,`POS_FLAG`,`PWP_EXCLUSION`,`AGE_STOCK_RETEN_PERIOD`,`MBR_DISC_FLAG`
                                                                                    ,`MBR_DISC_PERC`,`MOMMY_DISC_PERC`,`HS_CODE`,`MSDS_CODE`,`GROUP_ID`,`DIV_ID`,`LINE_ID`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string CATEGORY_ID = rows[0].ToString();

                                            string CATEGORY_NAME = "";
                                            string temp_CATEGORY_NAME = rows[1].ToString();
                                            if (temp_CATEGORY_NAME.Contains("\""))
                                            {
                                                CATEGORY_NAME = temp_CATEGORY_NAME.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                CATEGORY_NAME = rows[1].ToString();
                                            }
                                            string DEPT_ID = rows[2].ToString();
                                            string DELETED = rows[3].ToString();
                                            string AUTO_PA = rows[4].ToString();
                                            string POS_FLAG = rows[5].ToString();
                                            string PWP_EXCLUSION = rows[6].ToString();
                                            string AGE_STOCK_RETEN_PERIOD = rows[7].ToString();
                                            string MBR_DISC_FLAG = rows[8].ToString();
                                            string MBR_DISC_PERC = rows[9].ToString();
                                            string MOMMY_DISC_PERC = rows[10].ToString();
                                            string HS_CODE = rows[11].ToString();
                                            string MSDS_CODE = rows[12].ToString();
                                            string GROUP_ID = rows[13].ToString();
                                            string DIV_ID = rows[14].ToString();
                                            string LINE_ID = rows[15].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_Category += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                    , ""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}""),"
                                                                                        , CATEGORY_ID, CATEGORY_NAME, DEPT_ID, DELETED, AUTO_PA, POS_FLAG, PWP_EXCLUSION, AGE_STOCK_RETEN_PERIOD, MBR_DISC_FLAG, MBR_DISC_PERC
                                                                                        , MOMMY_DISC_PERC, HS_CODE, MSDS_CODE, GROUP_ID, DIV_ID, LINE_ID, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                                sql_insert_data_Category = sql_insert_data_Category.Substring(0, sql_insert_data_Category.Length - 1);
                                                comm_sql_insert_data.CommandText = sql_insert_data_Category;
                                                int kq = comm_sql_insert_data.ExecuteNonQuery();
                                                connection.Close();

                                                sql_insert_data_Category = String.Format(@"INSERT INTO `aeon_pop`.`category_temp`
                                                                                    (`CATEGORY_ID`,`CATEGORY_NAME`,`DEPT_ID`,`DELETED`,`AUTO_PA`,`POS_FLAG`,`PWP_EXCLUSION`,`AGE_STOCK_RETEN_PERIOD`,`MBR_DISC_FLAG`
                                                                                    ,`MBR_DISC_PERC`,`MOMMY_DISC_PERC`,`HS_CODE`,`MSDS_CODE`,`GROUP_ID`,`DIV_ID`,`LINE_ID`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                            sql_insert_data_Category = sql_insert_data_Category.Substring(0, sql_insert_data_Category.Length - 1);
                                            comm_sql_insert_data.CommandText = sql_insert_data_Category;
                                            int kq = comm_sql_insert_data.ExecuteNonQuery();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 10)
                            {
                                if (filename.Substring(0, 10) == "SCATEGORY_")
                                {
                                    #region SCATEGORY_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_SubCategory = String.Format(@"INSERT INTO `aeon_pop`.`sub_category_temp`
                                                                                    (`SUBCATEGORY_ID`,`SUBCATEGORY_NAME`,`CATEGORY_ID`,`DELETED`,`DEPT_ID`,`GROUP_ID`,`DIV_ID`,`LINE_ID`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string SUBCATEGORY_ID = rows[0].ToString();

                                            string SUBCATEGORY_NAME = "";
                                            string temp_SUBCATEGORY_NAME = rows[1].ToString();
                                            if (temp_SUBCATEGORY_NAME.Contains("\""))
                                            {
                                                SUBCATEGORY_NAME = temp_SUBCATEGORY_NAME.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SUBCATEGORY_NAME = rows[1].ToString();
                                            }
                                            string CATEGORY_ID = rows[2].ToString();
                                            string DELETED = rows[3].ToString();
                                            string DEPT_ID = rows[4].ToString();
                                            string GROUP_ID = rows[5].ToString();
                                            string DIV_ID = rows[6].ToString();
                                            string LINE_ID = rows[7].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_SubCategory += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}""),"
                                                                                        , SUBCATEGORY_ID, SUBCATEGORY_NAME, CATEGORY_ID, DELETED, DEPT_ID, GROUP_ID, DIV_ID, LINE_ID, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                                sql_insert_data_SubCategory = sql_insert_data_SubCategory.Substring(0, sql_insert_data_SubCategory.Length - 1);
                                                comm_sql_insert_data.CommandText = sql_insert_data_SubCategory;
                                                int kq = comm_sql_insert_data.ExecuteNonQuery();
                                                connection.Close();

                                                sql_insert_data_SubCategory = String.Format(@"INSERT INTO `aeon_pop`.`sub_category_temp`
                                                                                    (`SUBCATEGORY_ID`,`SUBCATEGORY_NAME`,`CATEGORY_ID`,`DELETED`,`DEPT_ID`,`GROUP_ID`,`DIV_ID`,`LINE_ID`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data = connection.CreateCommand();
                                            sql_insert_data_SubCategory = sql_insert_data_SubCategory.Substring(0, sql_insert_data_SubCategory.Length - 1);
                                            comm_sql_insert_data.CommandText = sql_insert_data_SubCategory;
                                            int kq = comm_sql_insert_data.ExecuteNonQuery();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 5)
                            {
                                if (filename.Substring(0, 5) == "ITEM_")
                                {
                                    #region SKU new insert DB
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();

                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_SKU = String.Format(@"INSERT INTO `aeon_pop`.`sku_temp`
                                                                                    (`SKU_CODE`,`ITEM_DESC_VNM`,`PACK_ITEM`,`PERISH_ITEM`,`NON_INVENTORY`,`NON_PLU`,`MOMMY_ITEM`
                                                                                    ,`FOOD_ITEM`,`MEMBER_DISC_ITEM`,`SUPER_SAVER_ITEM`,`AUTO_REPLENISH_ITEM`,`PURCHASE_METHOD`
                                                                                    ,`LINE_ID`,`DIVISION_ID`,`GROUP_ID`,`DEPT_ID`,`CATEGORY_ID`,`SUB_CATEGORY`
                                                                                    ,`COLOUR_SIZE_GRID`,`COLOUR`,`SIZE_ID`,`POP1_DESC_VNM`,`POP2_DESC_VNM`,`POP3_DESC_VNM`
                                                                                    ,`SELLING_POINT1`,`SELLING_POINT2`,`SELLING_POINT3`,`SELLING_POINT4`,`SELLING_POINT5`
                                                                                    ,`RETAIL_UOM`,`STATUS`,`ACTIVED`,`DELETED`,`DATE_CREATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\","").Split(',');
                                            line++;

                                            //get data
                                            string SKU_CODE = rows[0].ToString();

                                            string ITEM_DESC_VNM = "";
                                            string temp_desc = rows[4].ToString();
                                            ITEM_DESC_VNM = temp_desc.Contains("\"") ? temp_desc.Replace("\"", "\"\"") : rows[4].ToString();
                                            ITEM_DESC_VNM = temp_desc.Contains(@"@@") ? temp_desc.Replace(@"@@", ",") : ITEM_DESC_VNM;

                                            string PACK_ITEM = "";
                                            string temp_pack_item = rows[37].ToString();
                                            PACK_ITEM = temp_pack_item.Contains("\"") ? temp_pack_item.Replace("\"", "\"\"") : rows[37].ToString();
                                            PACK_ITEM = temp_pack_item.Contains(@"@@") ? temp_pack_item.Replace(@"@@", ",") : PACK_ITEM;

                                            string PERISH_ITEM = "";
                                            string temp_perish_item = rows[38].ToString();
                                            PERISH_ITEM = temp_perish_item.Contains("\"") ? temp_perish_item.Replace("\"", "\"\"") : rows[38].ToString();
                                            PERISH_ITEM = temp_perish_item.Contains(@"@@") ? temp_perish_item.Replace(@"@@", ",") : PERISH_ITEM;

                                            string NON_INVENTORY = "";
                                            string temp_NON_INVENTORY = rows[39].ToString();
                                            NON_INVENTORY = temp_NON_INVENTORY.Contains("\"") ? temp_NON_INVENTORY.Replace("\"", "\"\"") : rows[39].ToString();
                                            NON_INVENTORY = temp_NON_INVENTORY.Contains(@"@@") ? temp_NON_INVENTORY.Replace(@"@@", ",") : NON_INVENTORY;
                                            //string NON_INVENTORY = result.NON_INVENTORY;

                                            string NON_PLU = "";
                                            string temp_NON_PLU = rows[41].ToString();
                                            NON_PLU = temp_NON_PLU.Contains("\"") ? temp_NON_PLU.Replace("\"", "\"\"") : rows[41].ToString();
                                            NON_PLU = temp_NON_PLU.Contains(@"@@") ? temp_NON_PLU.Replace(@"@@", ",") : NON_PLU;
                                            //string NON_PLU = result.NON_PLU;

                                            string MOMMY_ITEM = "";
                                            string temp_MOMMY_ITEM = rows[42].ToString();
                                            MOMMY_ITEM = temp_MOMMY_ITEM.Contains("\"") ? temp_MOMMY_ITEM.Replace("\"", "\"\"") : rows[42].ToString();
                                            MOMMY_ITEM = temp_MOMMY_ITEM.Contains(@"@@") ? temp_MOMMY_ITEM.Replace(@"@@", ",") : MOMMY_ITEM;
                                            //string MOMMY_ITEM = result.MOMMY_ITEM;

                                            string FOOD_ITEM = "";
                                            string temp_FOOD_ITEM = rows[43].ToString();
                                            FOOD_ITEM = temp_FOOD_ITEM.Contains("\"") ? temp_FOOD_ITEM.Replace("\"", "\"\"") : rows[43].ToString();
                                            FOOD_ITEM = temp_FOOD_ITEM.Contains(@"@@") ? temp_FOOD_ITEM.Replace(@"@@", ",") : FOOD_ITEM;
                                            //string FOOD_ITEM = result.FOOD_ITEM;

                                            string MEMBER_DISC_ITEM = "";
                                            string temp_MEMBER_DISC_ITEM = rows[44].ToString();
                                            MEMBER_DISC_ITEM = temp_MEMBER_DISC_ITEM.Contains("\"") ? temp_MEMBER_DISC_ITEM.Replace("\"", "\"\"") : rows[44].ToString();
                                            MEMBER_DISC_ITEM = temp_MEMBER_DISC_ITEM.Contains(@"@@") ? temp_MEMBER_DISC_ITEM.Replace(@"@@", ",") : MEMBER_DISC_ITEM;
                                            //string MEMBER_DISC_ITEM = result.MEMBER_DISC_ITEM;

                                            string SUPER_SAVER_ITEM = "";
                                            string temp_SUPER_SAVER_ITEM = rows[45].ToString();
                                            SUPER_SAVER_ITEM = temp_SUPER_SAVER_ITEM.Contains("\"") ? temp_SUPER_SAVER_ITEM.Replace("\"", "\"\"") : rows[45].ToString();
                                            SUPER_SAVER_ITEM = temp_SUPER_SAVER_ITEM.Contains(@"@@") ? temp_SUPER_SAVER_ITEM.Replace(@"@@", ",") : SUPER_SAVER_ITEM;
                                            //string SUPER_SAVER_ITEM = result.SUPER_SAVER_ITEM;

                                            string AUTO_REPLENISH_ITEM = "";
                                            string temp_AUTO_REPLENISH_ITEM = rows[47].ToString();
                                            AUTO_REPLENISH_ITEM = temp_AUTO_REPLENISH_ITEM.Contains("\"") ? temp_AUTO_REPLENISH_ITEM.Replace("\"", "\"\"") : rows[47].ToString();
                                            AUTO_REPLENISH_ITEM = temp_AUTO_REPLENISH_ITEM.Contains(@"@@") ? temp_AUTO_REPLENISH_ITEM.Replace(@"@@", ",") : AUTO_REPLENISH_ITEM;
                                            //string AUTO_REPLENISH_ITEM = result.AUTO_REPLENISH_ITEM;

                                            string PURCHASE_METHOD = "";
                                            string temp_PURCHASE_METHOD = rows[29].ToString();
                                            PURCHASE_METHOD = temp_PURCHASE_METHOD.Contains("\"") ? temp_PURCHASE_METHOD.Replace("\"", "\"\"") : rows[29].ToString();
                                            PURCHASE_METHOD = temp_PURCHASE_METHOD.Contains(@"@@") ? temp_PURCHASE_METHOD.Replace(@"@@", ",") : PURCHASE_METHOD;
                                            //string PURCHASE_METHOD = rows[29].ToString();

                                            string LINE_ID = "";
                                            string temp_LINE_ID = rows[23].ToString();
                                            LINE_ID = temp_LINE_ID.Contains("\"") ? temp_LINE_ID.Replace("\"", "\"\"") : rows[23].ToString();
                                            LINE_ID = temp_LINE_ID.Contains(@"@@") ? temp_LINE_ID.Replace(@"@@", ",") : LINE_ID;
                                            //string LINE_ID = rows[23].ToString();

                                            string DIVISION_ID = "";
                                            string temp_DIVISION_ID = rows[22].ToString();
                                            DIVISION_ID = temp_DIVISION_ID.Contains("\"") ? temp_DIVISION_ID.Replace("\"", "\"\"") : rows[22].ToString();
                                            DIVISION_ID = temp_DIVISION_ID.Contains(@"@@") ? temp_DIVISION_ID.Replace(@"@@", ",") : DIVISION_ID;
                                            //string DIVISION_ID = rows[22].ToString();

                                            string GROUP_ID = "";
                                            string temp_GROUP_ID = rows[24].ToString();
                                            GROUP_ID = temp_GROUP_ID.Contains("\"") ? temp_GROUP_ID.Replace("\"", "\"\"") : rows[24].ToString();
                                            GROUP_ID = temp_GROUP_ID.Contains(@"@@") ? temp_GROUP_ID.Replace(@"@@", ",") : GROUP_ID;
                                            //string GROUP_ID = rows[24].ToString();

                                            string DEPT_ID = "";
                                            string temp_DEPT_ID = rows[17].ToString();
                                            DEPT_ID = temp_DEPT_ID.Contains("\"") ? temp_DEPT_ID.Replace("\"", "\"\"") : rows[17].ToString();
                                            DEPT_ID = temp_DEPT_ID.Contains(@"@@") ? temp_DEPT_ID.Replace(@"@@", ",") : DEPT_ID;
                                            //string DEPT_ID = rows[17].ToString();

                                            string CATEGORY_ID = "";
                                            string temp_CATEGORY_ID = rows[19].ToString();
                                            CATEGORY_ID = temp_CATEGORY_ID.Contains("\"") ? temp_CATEGORY_ID.Replace("\"", "\"\"") : rows[19].ToString();
                                            CATEGORY_ID = temp_CATEGORY_ID.Contains(@"@@") ? temp_CATEGORY_ID.Replace(@"@@", ",") : CATEGORY_ID;
                                            //string CATEGORY_ID = rows[19].ToString();

                                            string SUB_CATEGORY = "";
                                            string temp_SUB_CATEGORY = rows[58].ToString();
                                            SUB_CATEGORY = temp_SUB_CATEGORY.Contains("\"") ? temp_SUB_CATEGORY.Replace("\"", "\"\"") : rows[58].ToString();
                                            SUB_CATEGORY = temp_SUB_CATEGORY.Contains(@"@@") ? temp_SUB_CATEGORY.Replace(@"@@", ",") : SUB_CATEGORY;
                                            //string SUB_CATEGORY = rows[58].ToString();
                                            string COLOUR_SIZE_GRID = "";
                                            string temp_COLOUR_SIZE_GRID = rows[26].ToString();
                                            COLOUR_SIZE_GRID = temp_COLOUR_SIZE_GRID.Contains("\"") ? temp_COLOUR_SIZE_GRID.Replace("\"", "\"\"") : rows[26].ToString();
                                            COLOUR_SIZE_GRID = temp_COLOUR_SIZE_GRID.Contains(@"@@") ? temp_COLOUR_SIZE_GRID.Replace(@"@@", ",") : COLOUR_SIZE_GRID;
                                            //string COLOUR_SIZE_GRID = rows[26].ToString();
                                            string COLOUR = "";
                                            string temp_COLOUR = rows[27].ToString();
                                            COLOUR = temp_COLOUR.Contains("\"") ? temp_COLOUR.Replace("\"", "\"\"") : rows[27].ToString();
                                            COLOUR = temp_COLOUR.Contains(@"@@") ? temp_COLOUR.Replace(@"@@", ",") : COLOUR;
                                            //string COLOUR = rows[27].ToString();
                                            string SIZE_ID = "";
                                            string temp_SIZE_ID = rows[28].ToString();
                                            SIZE_ID = temp_SIZE_ID.Contains("\"") ? temp_SIZE_ID.Replace("\"", "\"\"") : rows[28].ToString();
                                            SIZE_ID = temp_SIZE_ID.Contains(@"@@") ? temp_SIZE_ID.Replace(@"@@", ",") : SIZE_ID;
                                            //string SIZE_ID = rows[28].ToString();

                                            string POP1_DESC_VNM = "";
                                            string temp_POP1_DESC_VNM = rows[14].ToString();
                                            POP1_DESC_VNM = temp_POP1_DESC_VNM.Contains("\"") ? temp_POP1_DESC_VNM.Replace("\"", "\"\"") : rows[14].ToString();
                                            POP1_DESC_VNM = temp_POP1_DESC_VNM.Contains(@"@@") ? temp_POP1_DESC_VNM.Replace(@"@@", ",") : POP1_DESC_VNM;
                                            //string POP1_DESC_VNM = result.POP1_DESC_VNM;
                                            string POP2_DESC_VNM = "";
                                            string temp_POP2_DESC_VNM = rows[15].ToString();
                                            POP2_DESC_VNM = temp_POP2_DESC_VNM.Contains("\"") ? temp_POP2_DESC_VNM.Replace("\"", "\"\"") : rows[15].ToString();
                                            POP2_DESC_VNM = temp_POP2_DESC_VNM.Contains(@"@@") ? temp_POP2_DESC_VNM.Replace(@"@@", ",") : POP2_DESC_VNM;
                                            //string POP2_DESC_VNM = result.POP2_DESC_VNM;
                                            string POP3_DESC_VNM = "";
                                            string temp_POP3_DESC_VNM = rows[84].ToString();
                                            POP3_DESC_VNM = temp_POP3_DESC_VNM.Contains("\"") ? temp_POP3_DESC_VNM.Replace("\"", "\"\"") : rows[84].ToString();
                                            POP3_DESC_VNM = temp_POP3_DESC_VNM.Contains(@"@@") ? temp_POP3_DESC_VNM.Replace(@"@@", ",") : POP3_DESC_VNM;
                                            //string POP3_DESC_VNM = result.POP3_DESC_VNM;
                                            string SELLING_POINT1 = "";
                                            string temp_SELLING_POINT1 = rows[85].ToString();
                                            SELLING_POINT1 = temp_SELLING_POINT1.Contains("\"") ? temp_SELLING_POINT1.Replace("\"", "\"\"") : rows[85].ToString();
                                            SELLING_POINT1 = temp_SELLING_POINT1.Contains(@"@@") ? temp_SELLING_POINT1.Replace(@"@@", ",") : SELLING_POINT1;
                                            //string SELLING_POINT1 = rows[85].ToString();
                                            string SELLING_POINT2 = "";
                                            string temp_SELLING_POINT2 = rows[86].ToString();
                                            SELLING_POINT2 = temp_SELLING_POINT2.Contains("\"") ? temp_SELLING_POINT2.Replace("\"", "\"\"") : rows[86].ToString();
                                            SELLING_POINT2 = temp_SELLING_POINT2.Contains(@"@@") ? temp_SELLING_POINT2.Replace(@"@@", ",") : SELLING_POINT2;
                                            //string SELLING_POINT2 = rows[86].ToString();
                                            string SELLING_POINT3 = "";
                                            string temp_SELLING_POINT3 = rows[87].ToString();
                                            SELLING_POINT3 = temp_SELLING_POINT3.Contains("\"") ? temp_SELLING_POINT3.Replace("\"", "\"\"") : rows[87].ToString();
                                            SELLING_POINT3 = temp_SELLING_POINT3.Contains(@"@@") ? temp_SELLING_POINT3.Replace(@"@@", ",") : SELLING_POINT3;
                                            //string SELLING_POINT3 = rows[87].ToString();
                                            string SELLING_POINT4 = "";
                                            string temp_SELLING_POINT4 = rows[88].ToString();
                                            SELLING_POINT4 = temp_SELLING_POINT4.Contains("\"") ? temp_SELLING_POINT4.Replace("\"", "\"\"") : rows[88].ToString();
                                            SELLING_POINT4 = temp_SELLING_POINT4.Contains(@"@@") ? temp_SELLING_POINT4.Replace(@"@@", ",") : SELLING_POINT4;
                                            //string SELLING_POINT4 = rows[88].ToString();
                                            string SELLING_POINT5 = "";
                                            string temp_SELLING_POINT5 = rows[89].ToString();
                                            SELLING_POINT5 = temp_SELLING_POINT5.Contains("\"") ? temp_SELLING_POINT5.Replace("\"", "\"\"") : rows[89].ToString();
                                            SELLING_POINT5 = temp_SELLING_POINT5.Contains(@"@@") ? temp_SELLING_POINT5.Replace(@"@@", ",") : SELLING_POINT5;
                                            //string SELLING_POINT5 = rows[89].ToString();
                                            string RETAIL_UOM = "";
                                            string temp_RETAIL_UOM = rows[62].ToString();
                                            RETAIL_UOM = temp_RETAIL_UOM.Contains("\"") ? temp_RETAIL_UOM.Replace("\"", "\"\"") : rows[62].ToString();
                                            //string RETAIL_UOM = rows[62].ToString();

                                            string STATUS = "";
                                            string temp_STATUS = rows[56].ToString();
                                            STATUS = temp_STATUS.Contains("\"") ? temp_STATUS.Replace("\"", "\"\"") : rows[56].ToString();
                                            //string STATUS = rows[56].ToString();
                                            string ACTIVED = rows[49].ToString();
                                            string DELETED = rows[56].ToString();

                                            string DATE_CREATE = rows[1].ToString();
                                            string MODIFIED_DATE = rows[83].ToString();
                                            string FILE_ID = log_fileid;


                                            sql_insert_data_SKU += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}"",""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}"",""{20}"",""{21}"",""{22}"",""{23}"",""{24}"",""{25}"",""{26}"",""{27}"",""{28}"",""{29}"",""{30}"",""{31}"",""{32}"",""{33}"",""{34}"",""{35}""),"
                                                                                        , SKU_CODE, ITEM_DESC_VNM, PACK_ITEM, PERISH_ITEM, NON_INVENTORY, NON_PLU, MOMMY_ITEM
                                                                                        , FOOD_ITEM, MEMBER_DISC_ITEM, SUPER_SAVER_ITEM, AUTO_REPLENISH_ITEM, PURCHASE_METHOD
                                                                                        , LINE_ID, DIVISION_ID, GROUP_ID, DEPT_ID, CATEGORY_ID, SUB_CATEGORY, COLOUR_SIZE_GRID
                                                                                        , COLOUR, SIZE_ID, POP1_DESC_VNM, POP2_DESC_VNM, POP3_DESC_VNM, SELLING_POINT1
                                                                                        , SELLING_POINT2, SELLING_POINT3, SELLING_POINT4, SELLING_POINT5, RETAIL_UOM, STATUS, ACTIVED, DELETED
                                                                                        , DATE_CREATE, MODIFIED_DATE, FILE_ID);

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                MySqlCommand comm_sql_insert_data_SKU = connection.CreateCommand();
                                                sql_insert_data_SKU = sql_insert_data_SKU.Substring(0, sql_insert_data_SKU.Length - 1);
                                                comm_sql_insert_data_SKU.CommandText = sql_insert_data_SKU;
                                                int kq = comm_sql_insert_data_SKU.ExecuteNonQuery();
                                                connection.Close();

                                                //connection.Open();
                                                //sql_insert_data_SKU = sql_insert_data_SKU.Substring(0, sql_insert_data_SKU.Length - 1);
                                                //var cmd_insert_data_SKU = new MySqlCommand(sql_insert_data_SKU, connection);
                                                //MySqlDataReader rdr_insert_data_SKU = cmd_insert_data_SKU.ExecuteReader();
                                                //connection.Close();

                                                sql_insert_data_SKU = String.Format(@"INSERT INTO `aeon_pop`.`sku_temp`
                                                                                    (`SKU_CODE`,`ITEM_DESC_VNM`,`PACK_ITEM`,`PERISH_ITEM`,`NON_INVENTORY`,`NON_PLU`,`MOMMY_ITEM`
                                                                                    ,`FOOD_ITEM`,`MEMBER_DISC_ITEM`,`SUPER_SAVER_ITEM`,`AUTO_REPLENISH_ITEM`,`PURCHASE_METHOD`
                                                                                    ,`LINE_ID`,`DIVISION_ID`,`GROUP_ID`,`DEPT_ID`,`CATEGORY_ID`,`SUB_CATEGORY`
                                                                                    ,`COLOUR_SIZE_GRID`,`COLOUR`,`SIZE_ID`,`POP1_DESC_VNM`,`POP2_DESC_VNM`,`POP3_DESC_VNM`
                                                                                    ,`SELLING_POINT1`,`SELLING_POINT2`,`SELLING_POINT3`,`SELLING_POINT4`,`SELLING_POINT5`
                                                                                    ,`RETAIL_UOM`,`STATUS`,`ACTIVED`,`DELETED`,`DATE_CREATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            MySqlCommand comm_sql_insert_data_SKU = connection.CreateCommand();
                                            sql_insert_data_SKU = sql_insert_data_SKU.Substring(0, sql_insert_data_SKU.Length - 1);
                                            comm_sql_insert_data_SKU.CommandText = sql_insert_data_SKU;
                                            comm_sql_insert_data_SKU.ExecuteNonQuery();
                                            connection.Close();
                                        }    
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region SKU old
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu
                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_sku = String.Format(@"SELECT * 
                                                                                    FROM
                                                                                    (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(SKU_CODE) ORDER BY FILE_ID DESC) AS row_num
                                                                                    FROM aeon_pop.sku) T0
                                                                                    WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_sku = new MySqlCommand(sql_get_cur_sku, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_sku = new MySqlDataAdapter();
                                                                        MyAdapter_cur_sku.SelectCommand = cmd_get_cur_sku;
                                                                        DataTable dTable_SKU_Cur = new DataTable();
                                                                        MyAdapter_cur_sku.Fill(dTable_SKU_Cur);
                                                                        connection.Close();

                                                                        //get dữ liệu mới
                                                                        DataTable dTable_SKU_New = ConvertCSVtoDataTable_SKU(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_SKU_New.AsEnumerable()
                                                                                           join table2 in dTable_SKU_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["SKU"] == null ? String.Empty : table1["SKU"].ToString()
                                                                                           }
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["SKU_CODE"] == null ? String.Empty : table2["SKU_CODE"].ToString()
                                                                                           }
                                                                                           into _Table3
                                                                                           from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["ITEM_DESC_VNM"].ToString()) != table1["ITEM_DESC_VNM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PACK_ITEM"].ToString()) != table1["PACK_ITEM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PERISH_ITEM"].ToString()) != table1["PERISH_ITEM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["NON_INVENTORY"].ToString()) != table1["NON_INVENTORY"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["NON_PLU"].ToString()) != table1["NON_PLU"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MOMMY_ITEM"].ToString()) != table1["MOMMY_ITEM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["FOOD_ITEM"].ToString()) != table1["FOOD_ITEM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MEMBER_DISC_ITEM"].ToString()) != table1["MEMBER_DISC_ITEM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SUPER_SAVER_ITEM"].ToString()) != table1["SUPER_SAVER_ITEM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["AUTO_REPLENISH_ITEM"].ToString()) != table1["AUTO_REPLENISH_ITEM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PURCHASE_METHOD"].ToString()) != table1["PURCHASE_METHOD"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["LINE_ID"].ToString()) != table1["LINE_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DIVISION_ID"].ToString()) != table1["DIVISION_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["GROUP_ID"].ToString()) != table1["GROUP_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DEPT_ID"].ToString()) != table1["DEPT_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["CATEGORY_ID"].ToString()) != table1["CATEGORY_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SUB_CATEGORY"].ToString()) != table1["SUB_CATEGORY"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["COLOUR_SIZE_GRID"].ToString()) != table1["COLOUR_SIZE_GRID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["COLOUR"].ToString()) != table1["COLOUR"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SIZE_ID"].ToString()) != table1["SIZE_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["POP1_DESC_VNM"].ToString()) != table1["POP1_DESC_VNM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["POP2_DESC_VNM"].ToString()) != table1["POP2_DESC_VNM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["POP3_DESC_VNM"].ToString()) != table1["POP3_DESC_VNM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SELLING_POINT1"].ToString()) != table1["SELLING_POINT1"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SELLING_POINT2"].ToString()) != table1["SELLING_POINT2"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SELLING_POINT3"].ToString()) != table1["SELLING_POINT3"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SELLING_POINT4"].ToString()) != table1["SELLING_POINT4"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SELLING_POINT5"].ToString()) != table1["SELLING_POINT5"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["RETAIL_UOM"].ToString()) != table1["RETAIL_UOM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["STATUS"].ToString()) != table1["DELETED"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DATE_CREATE"].ToString()) != table1["DATE_CREATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MODIFIED_DATE"].ToString()) != table1["MODIFIED_DATE"].ToString())
                                                                                                    )
                                                                                           select new
                                                                                           {
                                                                                               SKU_CODE = table1 == null || table1["SKU"] == null ? string.Empty : table1["SKU"].ToString(),
                                                                                               ITEM_DESC_VNM = table1 == null || table1["ITEM_DESC_VNM"] == null ? string.Empty : table1["ITEM_DESC_VNM"].ToString(),
                                                                                               PACK_ITEM = table1 == null || table1["PACK_ITEM"] == null ? string.Empty : table1["PACK_ITEM"].ToString(),
                                                                                               PERISH_ITEM = table1 == null || table1["PERISH_ITEM"] == null ? string.Empty : table1["PERISH_ITEM"].ToString(),
                                                                                               NON_INVENTORY = table1 == null || table1["NON_INVENTORY"] == null ? string.Empty : table1["NON_INVENTORY"].ToString(),
                                                                                               NON_PLU = table1 == null || table1["NON_PLU"] == null ? string.Empty : table1["NON_PLU"].ToString(),
                                                                                               MOMMY_ITEM = table1 == null || table1["MOMMY_ITEM"] == null ? string.Empty : table1["MOMMY_ITEM"].ToString(),
                                                                                               FOOD_ITEM = table1 == null || table1["FOOD_ITEM"] == null ? string.Empty : table1["FOOD_ITEM"].ToString(),
                                                                                               MEMBER_DISC_ITEM = table1 == null || table1["MEMBER_DISC_ITEM"] == null ? string.Empty : table1["MEMBER_DISC_ITEM"].ToString(),
                                                                                               SUPER_SAVER_ITEM = table1 == null || table1["SUPER_SAVER_ITEM"] == null ? string.Empty : table1["SUPER_SAVER_ITEM"].ToString(),
                                                                                               AUTO_REPLENISH_ITEM = table1 == null || table1["AUTO_REPLENISH_ITEM"] == null ? string.Empty : table1["AUTO_REPLENISH_ITEM"].ToString(),
                                                                                               PURCHASE_METHOD = table1 == null || table1["PURCHASE_METHOD"] == null ? string.Empty : table1["PURCHASE_METHOD"].ToString(),
                                                                                               LINE_ID = table1 == null || table1["LINE_ID"] == null ? string.Empty : table1["LINE_ID"].ToString(),
                                                                                               DIVISION_ID = table1 == null || table1["DIVISION_ID"] == null ? string.Empty : table1["DIVISION_ID"].ToString(),
                                                                                               GROUP_ID = table1 == null || table1["GROUP_ID"] == null ? string.Empty : table1["GROUP_ID"].ToString(),
                                                                                               DEPT_ID = table1 == null || table1["DEPT_ID"] == null ? string.Empty : table1["DEPT_ID"].ToString(),
                                                                                               CATEGORY_ID = table1 == null || table1["CATEGORY_ID"] == null ? string.Empty : table1["CATEGORY_ID"].ToString(),
                                                                                               SUB_CATEGORY = table1 == null || table1["SUB_CATEGORY"] == null ? string.Empty : table1["SUB_CATEGORY"].ToString(),
                                                                                               COLOUR_SIZE_GRID = table1 == null || table1["COLOUR_SIZE_GRID"] == null ? string.Empty : table1["COLOUR_SIZE_GRID"].ToString(),
                                                                                               COLOUR = table1 == null || table1["COLOUR"] == null ? string.Empty : table1["COLOUR"].ToString(),
                                                                                               SIZE_ID = table1 == null || table1["SIZE_ID"] == null ? string.Empty : table1["SIZE_ID"].ToString(),
                                                                                               POP1_DESC_VNM = table1 == null || table1["POP1_DESC_VNM"] == null ? string.Empty : table1["POP1_DESC_VNM"].ToString(),
                                                                                               POP2_DESC_VNM = table1 == null || table1["POP2_DESC_VNM"] == null ? string.Empty : table1["POP2_DESC_VNM"].ToString(),
                                                                                               POP3_DESC_VNM = table1 == null || table1["POP3_DESC_VNM"] == null ? string.Empty : table1["POP3_DESC_VNM"].ToString(),
                                                                                               SELLING_POINT1 = table1 == null || table1["SELLING_POINT1"] == null ? string.Empty : table1["SELLING_POINT1"].ToString(),
                                                                                               SELLING_POINT2 = table1 == null || table1["SELLING_POINT2"] == null ? string.Empty : table1["SELLING_POINT2"].ToString(),
                                                                                               SELLING_POINT3 = table1 == null || table1["SELLING_POINT3"] == null ? string.Empty : table1["SELLING_POINT3"].ToString(),
                                                                                               SELLING_POINT4 = table1 == null || table1["SELLING_POINT4"] == null ? string.Empty : table1["SELLING_POINT4"].ToString(),
                                                                                               SELLING_POINT5 = table1 == null || table1["SELLING_POINT5"] == null ? string.Empty : table1["SELLING_POINT5"].ToString(),
                                                                                               RETAIL_UOM = table1 == null || table1["RETAIL_UOM"] == null ? string.Empty : table1["RETAIL_UOM"].ToString(),
                                                                                               STATUS = table1 == null || table1["DELETED"] == null ? string.Empty : table1["DELETED"].ToString(),
                                                                                               DATE_CREATE = table1 == null || table1["DATE_CREATE"] == null ? string.Empty : table1["DATE_CREATE"].ToString(),
                                                                                               MODIFIED_DATE = table1 == null || table1["MODIFIED_DATE"] == null ? string.Empty : table1["MODIFIED_DATE"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table
                                                                        var sql_insert_data_SKU = String.Format(@"INSERT INTO `aeon_pop`.`sku`
                                                                                                                        (`SKU_CODE`,`ITEM_DESC_VNM`,`PACK_ITEM`,`PERISH_ITEM`,`NON_INVENTORY`,`NON_PLU`,`MOMMY_ITEM`
                                                                                                                        ,`FOOD_ITEM`,`MEMBER_DISC_ITEM`,`SUPER_SAVER_ITEM`,`AUTO_REPLENISH_ITEM`,`PURCHASE_METHOD`
                                                                                                                        ,`LINE_ID`,`DIVISION_ID`,`GROUP_ID`,`DEPT_ID`,`CATEGORY_ID`,`SUB_CATEGORY`
                                                                                                                        ,`COLOUR_SIZE_GRID`,`COLOUR`,`SIZE_ID`,`POP1_DESC_VNM`,`POP2_DESC_VNM`,`POP3_DESC_VNM`
                                                                                                                        ,`SELLING_POINT1`,`SELLING_POINT2`,`SELLING_POINT3`,`SELLING_POINT4`,`SELLING_POINT5`
                                                                                                                        ,`RETAIL_UOM`,`STATUS`,`DATE_CREATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                                                        int line = 0;
                                                                        foreach (var result in result_table)
                                                                        {
                                                                            line++;
                                                                            //get data
                                                                            string SKU_CODE = result.SKU_CODE;
                                                                            //string ITEM_DESC_VNM = result.ITEM_DESC_VNM;
                                                                            //string PACK_ITEM = result.PACK_ITEM;
                                                                            //string PERISH_ITEM = result.PERISH_ITEM;

                                                                            string ITEM_DESC_VNM = "";
                                                                            string temp_desc = result.ITEM_DESC_VNM;
                                                                            if (temp_desc.Contains("\""))
                                                                            {
                                                                                ITEM_DESC_VNM = temp_desc.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                ITEM_DESC_VNM = result.ITEM_DESC_VNM;
                                                                            }

                                                                            string PACK_ITEM = "";
                                                                            string temp_pack_item = result.PACK_ITEM;
                                                                            if (temp_pack_item.Contains("\""))
                                                                            {
                                                                                PACK_ITEM = temp_pack_item.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                PACK_ITEM = result.PACK_ITEM;
                                                                            }

                                                                            string PERISH_ITEM = "";
                                                                            string temp_perish_item = result.PERISH_ITEM;
                                                                            if (temp_perish_item.Contains("\""))
                                                                            {
                                                                                PERISH_ITEM = temp_perish_item.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                PERISH_ITEM = result.PERISH_ITEM;
                                                                            }

                                                                            string NON_INVENTORY = "";
                                                                            string temp_NON_INVENTORY = result.NON_INVENTORY;
                                                                            if (temp_NON_INVENTORY.Contains("\""))
                                                                            {
                                                                                NON_INVENTORY = temp_NON_INVENTORY.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                NON_INVENTORY = result.NON_INVENTORY;
                                                                            }
                                                                            //string NON_INVENTORY = result.NON_INVENTORY;

                                                                            string NON_PLU = "";
                                                                            string temp_NON_PLU = result.NON_PLU;
                                                                            if (temp_NON_PLU.Contains("\""))
                                                                            {
                                                                                NON_PLU = temp_NON_PLU.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                NON_PLU = result.NON_PLU;
                                                                            }
                                                                            //string NON_PLU = result.NON_PLU;

                                                                            string MOMMY_ITEM = "";
                                                                            string temp_MOMMY_ITEM = result.MOMMY_ITEM;
                                                                            if (temp_MOMMY_ITEM.Contains("\""))
                                                                            {
                                                                                MOMMY_ITEM = temp_MOMMY_ITEM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                MOMMY_ITEM = result.MOMMY_ITEM;
                                                                            }
                                                                            //string MOMMY_ITEM = result.MOMMY_ITEM;

                                                                            string FOOD_ITEM = "";
                                                                            string temp_FOOD_ITEM = result.FOOD_ITEM;
                                                                            if (temp_FOOD_ITEM.Contains("\""))
                                                                            {
                                                                                FOOD_ITEM = temp_FOOD_ITEM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                FOOD_ITEM = result.FOOD_ITEM;
                                                                            }
                                                                            //string FOOD_ITEM = result.FOOD_ITEM;

                                                                            string MEMBER_DISC_ITEM = "";
                                                                            string temp_MEMBER_DISC_ITEM = result.MEMBER_DISC_ITEM;
                                                                            if (temp_MEMBER_DISC_ITEM.Contains("\""))
                                                                            {
                                                                                MEMBER_DISC_ITEM = temp_MEMBER_DISC_ITEM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                MEMBER_DISC_ITEM = result.MEMBER_DISC_ITEM;
                                                                            }
                                                                            //string MEMBER_DISC_ITEM = result.MEMBER_DISC_ITEM;

                                                                            string SUPER_SAVER_ITEM = "";
                                                                            string temp_SUPER_SAVER_ITEM = result.SUPER_SAVER_ITEM;
                                                                            if (temp_SUPER_SAVER_ITEM.Contains("\""))
                                                                            {
                                                                                SUPER_SAVER_ITEM = temp_SUPER_SAVER_ITEM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                SUPER_SAVER_ITEM = result.SUPER_SAVER_ITEM;
                                                                            }
                                                                            //string SUPER_SAVER_ITEM = result.SUPER_SAVER_ITEM;

                                                                            string AUTO_REPLENISH_ITEM = "";
                                                                            string temp_AUTO_REPLENISH_ITEM = result.AUTO_REPLENISH_ITEM;
                                                                            if (temp_AUTO_REPLENISH_ITEM.Contains("\""))
                                                                            {
                                                                                AUTO_REPLENISH_ITEM = temp_AUTO_REPLENISH_ITEM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                AUTO_REPLENISH_ITEM = result.AUTO_REPLENISH_ITEM;
                                                                            }
                                                                            //string AUTO_REPLENISH_ITEM = result.AUTO_REPLENISH_ITEM;
                                                                            string PURCHASE_METHOD = result.PURCHASE_METHOD;
                                                                            string LINE_ID = result.LINE_ID;
                                                                            string DIVISION_ID = result.DIVISION_ID;
                                                                            string GROUP_ID = result.GROUP_ID;
                                                                            string DEPT_ID = result.DEPT_ID;
                                                                            string CATEGORY_ID = result.CATEGORY_ID;
                                                                            string SUB_CATEGORY = result.SUB_CATEGORY;
                                                                            string COLOUR_SIZE_GRID = result.COLOUR_SIZE_GRID;
                                                                            string COLOUR = result.COLOUR;
                                                                            string SIZE_ID = result.SIZE_ID;

                                                                            string POP1_DESC_VNM = "";
                                                                            string temp_POP1_DESC_VNM = result.POP1_DESC_VNM;
                                                                            if (temp_POP1_DESC_VNM.Contains("\""))
                                                                            {
                                                                                POP1_DESC_VNM = temp_POP1_DESC_VNM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                POP1_DESC_VNM = result.POP1_DESC_VNM;
                                                                            }
                                                                            //string POP1_DESC_VNM = result.POP1_DESC_VNM;
                                                                            string POP2_DESC_VNM = "";
                                                                            string temp_POP2_DESC_VNM = result.POP2_DESC_VNM;
                                                                            if (temp_POP2_DESC_VNM.Contains("\""))
                                                                            {
                                                                                POP2_DESC_VNM = temp_POP2_DESC_VNM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                POP2_DESC_VNM = result.POP2_DESC_VNM;
                                                                            }
                                                                            //string POP2_DESC_VNM = result.POP2_DESC_VNM;
                                                                            string POP3_DESC_VNM = "";
                                                                            string temp_POP3_DESC_VNM = result.POP3_DESC_VNM;
                                                                            if (temp_POP3_DESC_VNM.Contains("\""))
                                                                            {
                                                                                POP3_DESC_VNM = temp_POP3_DESC_VNM.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                POP3_DESC_VNM = result.POP3_DESC_VNM;
                                                                            }
                                                                            //string POP3_DESC_VNM = result.POP3_DESC_VNM;
                                                                            string SELLING_POINT1 = result.SELLING_POINT1;
                                                                            string SELLING_POINT2 = result.SELLING_POINT2;
                                                                            string SELLING_POINT3 = result.SELLING_POINT3;
                                                                            string SELLING_POINT4 = result.SELLING_POINT4;
                                                                            string SELLING_POINT5 = result.SELLING_POINT5;
                                                                            string RETAIL_UOM = result.RETAIL_UOM;


                                                                            string STATUS = result.STATUS;
                                                                            string DATE_CREATE = result.DATE_CREATE;
                                                                            string MODIFIED_DATE = result.MODIFIED_DATE;
                                                                            string FILE_ID = log_fileid;


                                                                            sql_insert_data_SKU += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                                                    ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                                                    ,""{20}"",""{21}"",""{22}"",""{23}"",""{24}"",""{25}"",""{26}"",""{27}"",""{28}"",""{29}""
                                                                                                                    ,""{30}"",""{31}"",""{32}"",""{33}""),"
                                                                                                                        , SKU_CODE, ITEM_DESC_VNM, PACK_ITEM, PERISH_ITEM, NON_INVENTORY, NON_PLU, MOMMY_ITEM
                                                                                                                        , FOOD_ITEM, MEMBER_DISC_ITEM, SUPER_SAVER_ITEM, AUTO_REPLENISH_ITEM, PURCHASE_METHOD
                                                                                                                        , LINE_ID, DIVISION_ID, GROUP_ID, DEPT_ID, CATEGORY_ID, SUB_CATEGORY, COLOUR_SIZE_GRID
                                                                                                                        , COLOUR, SIZE_ID, POP1_DESC_VNM, POP2_DESC_VNM, POP3_DESC_VNM, SELLING_POINT1
                                                                                                                        , SELLING_POINT2, SELLING_POINT3, SELLING_POINT4, SELLING_POINT5, RETAIL_UOM, STATUS
                                                                                                                        , DATE_CREATE, MODIFIED_DATE, FILE_ID);

                                                                            if (line == 1000)
                                                                            {
                                                                                connection.Open();
                                                                                MySqlCommand comm_sql_insert_data_SKU = connection.CreateCommand();
                                                                                sql_insert_data_SKU = sql_insert_data_SKU.Substring(0, sql_insert_data_SKU.Length - 1);
                                                                                comm_sql_insert_data_SKU.CommandText = sql_insert_data_SKU;
                                                                                comm_sql_insert_data_SKU.ExecuteNonQuery();
                                                                                connection.Close();

                                                                                //connection.Open();
                                                                                //sql_insert_data_SKU = sql_insert_data_SKU.Substring(0, sql_insert_data_SKU.Length - 1);
                                                                                //var cmd_insert_data_SKU = new MySqlCommand(sql_insert_data_SKU, connection);
                                                                                //MySqlDataReader rdr_insert_data_SKU = cmd_insert_data_SKU.ExecuteReader();
                                                                                //connection.Close();

                                                                                sql_insert_data_SKU = String.Format(@"INSERT INTO `aeon_pop`.`sku`
                                                                                                                        (`SKU_CODE`,`ITEM_DESC_VNM`,`PACK_ITEM`,`PERISH_ITEM`,`NON_INVENTORY`,`NON_PLU`,`MOMMY_ITEM`
                                                                                                                        ,`FOOD_ITEM`,`MEMBER_DISC_ITEM`,`SUPER_SAVER_ITEM`,`AUTO_REPLENISH_ITEM`,`PURCHASE_METHOD`
                                                                                                                        ,`LINE_ID`,`DIVISION_ID`,`GROUP_ID`,`DEPT_ID`,`CATEGORY_ID`,`SUB_CATEGORY`
                                                                                                                        ,`COLOUR_SIZE_GRID`,`COLOUR`,`SIZE_ID`,`POP1_DESC_VNM`,`POP2_DESC_VNM`,`POP3_DESC_VNM`
                                                                                                                        ,`SELLING_POINT1`,`SELLING_POINT2`,`SELLING_POINT3`,`SELLING_POINT4`,`SELLING_POINT5`
                                                                                                                        ,`RETAIL_UOM`,`STATUS`,`DATE_CREATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");
                                                                                line = 0;
                                                                            }
                                                                        }

                                                                        if (result_table.Count() > 0 && line != 0)
                                                                        {
                                                                            connection.Open();
                                                                            MySqlCommand comm_sql_insert_data_SKU = connection.CreateCommand();
                                                                            sql_insert_data_SKU = sql_insert_data_SKU.Substring(0, sql_insert_data_SKU.Length - 1);
                                                                            comm_sql_insert_data_SKU.CommandText = sql_insert_data_SKU;
                                                                            comm_sql_insert_data_SKU.ExecuteNonQuery();
                                                                            connection.Close();

                                                                            //connection.Open();
                                                                            //sql_insert_data_SKU = sql_insert_data_SKU.Substring(0, sql_insert_data_SKU.Length - 1);
                                                                            //var cmd_insert_data_SKU = new MySqlCommand(sql_insert_data_SKU, connection);
                                                                            //MySqlDataReader rdr_insert_data_SKU = cmd_insert_data_SKU.ExecuteReader();
                                                                            //connection.Close();
                                                                        }

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                                                        */
                                    #endregion old
                                }
                            }
                            if (filename.Length >= 10)
                            {
                                if (filename.Substring(0, 10) == "HAMPERMST_")
                                {
                                    #region Hamper new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table hamper
                                        var sql_insert_data_Hamper = String.Format(@"INSERT INTO `aeon_pop`.`hamper`(`PACK_SKU`,`DESCRIPTION`,`PACK_TYPE`,`SKU`,`QTY_PER_SKU`,`QTY_UOM`,`STORE`
                                                                                            ,`DECORATION_FLAG`,`STATUS`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Split(',');
                                            line++;

                                            //get data
                                            string PACK_SKU = rows[0].ToString();
                                            string DESCRIPTION = "";
                                            string temp_desc = rows[2].ToString();
                                            if (temp_desc.Contains("\""))
                                            {
                                                DESCRIPTION = temp_desc.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                DESCRIPTION = rows[2].ToString();
                                            }

                                            string PACK_TYPE = rows[3].ToString();
                                            string SKU = rows[5].ToString();
                                            string QTY_PER_SKU = rows[6].ToString();
                                            string QTY_UOM = rows[7].ToString();
                                            string STORE = rows[1].ToString();
                                            string DECORATION_FLAG = rows[8].ToString();
                                            string STATUS = rows[4].ToString();
                                            string MODIFIED_DATE = rows[9].ToString();
                                            string FILE_ID = log_fileid;

                                            sql_insert_data_Hamper += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}"",""{10}""),"
                                                                                        , PACK_SKU, DESCRIPTION, PACK_TYPE, SKU, QTY_PER_SKU, QTY_UOM, STORE
                                                                                        , DECORATION_FLAG, STATUS, MODIFIED_DATE, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_Hamper = sql_insert_data_Hamper.Substring(0, sql_insert_data_Hamper.Length - 1);
                                                var cmd_insert_data_Hamper = new MySqlCommand(sql_insert_data_Hamper, connection);
                                                MySqlDataReader rdr_insert_data_Hamper = cmd_insert_data_Hamper.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_Hamper = String.Format(@"INSERT INTO `aeon_pop`.`hamper`(`PACK_SKU`,`DESCRIPTION`,`PACK_TYPE`,`SKU`,`QTY_PER_SKU`,`QTY_UOM`,`STORE`
                                                                                            ,`DECORATION_FLAG`,`STATUS`,`MODIFIED_DATE`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_Hamper = sql_insert_data_Hamper.Substring(0, sql_insert_data_Hamper.Length - 1);
                                            var cmd_insert_data_Hamper = new MySqlCommand(sql_insert_data_Hamper, connection);
                                            MySqlDataReader rdr_insert_data_Hamper = cmd_insert_data_Hamper.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region Hamper old
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu
                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_hamper = String.Format(@"SELECT * 
                                                                                    FROM
                                                                                    (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(STORE, SKU, PACK_SKU) ORDER BY FILE_ID DESC) AS row_num
                                                                                    FROM aeon_pop.hamper) T0
                                                                                    WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_hamper = new MySqlCommand(sql_get_cur_hamper, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_hamper = new MySqlDataAdapter();
                                                                        MyAdapter_cur_hamper.SelectCommand = cmd_get_cur_hamper;
                                                                        DataTable dTable_Hamper_Cur = new DataTable();
                                                                        MyAdapter_cur_hamper.Fill(dTable_Hamper_Cur);
                                                                        connection.Close();

                                                                        //get dữ liệu mới
                                                                        DataTable dTable_Hamper_New = ConvertCSVtoDataTable_Hamper(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_Hamper_New.AsEnumerable()
                                                                                           join table2 in dTable_Hamper_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["STORE"] == null ? String.Empty : table1["STORE"].ToString(),
                                                                                               con2 = table1["SKU"] == null ? String.Empty : table1["SKU"].ToString(),
                                                                                               con3 = table1["PACK_SKU"] == null ? String.Empty : table1["PACK_SKU"].ToString()
                                                                                           }
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["STORE"] == null ? String.Empty : table2["STORE"].ToString(),
                                                                                               con2 = table2["SKU"] == null ? String.Empty : table2["SKU"].ToString(),
                                                                                               con3 = table2["PACK_SKU"] == null ? String.Empty : table2["PACK_SKU"].ToString()
                                                                                           }
                                                                                           into _Table3
                                                                                           from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DESCRIPTION"].ToString()) != table1["DESCRIPTION"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PACK_TYPE"].ToString()) != table1["PACK_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["QTY_PER_SKU"].ToString()) != table1["QTY_PER_SKU"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["QTY_UOM"].ToString()) != table1["QTY_UOM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DECORATION_FLAG"].ToString()) != table1["DECORATION_FLAG"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["STATUS"].ToString()) != table1["STATUS"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MODIFIED_DATE"].ToString()) != table1["MODIFIED_DATE"].ToString())
                                                                                                    )
                                                                                           select new
                                                                                           {
                                                                                               PACK_SKU = table1 == null || table1["PACK_SKU"] == null ? string.Empty : table1["PACK_SKU"].ToString(),
                                                                                               DESCRIPTION = table1 == null || table1["DESCRIPTION"] == null ? string.Empty : table1["DESCRIPTION"].ToString(),
                                                                                               PACK_TYPE = table1 == null || table1["PACK_TYPE"] == null ? string.Empty : table1["PACK_TYPE"].ToString(),
                                                                                               SKU = table1 == null || table1["SKU"] == null ? string.Empty : table1["SKU"].ToString(),
                                                                                               QTY_PER_SKU = table1 == null || table1["QTY_PER_SKU"] == null ? string.Empty : table1["QTY_PER_SKU"].ToString(),
                                                                                               QTY_UOM = table1 == null || table1["QTY_UOM"] == null ? string.Empty : table1["QTY_UOM"].ToString(),
                                                                                               STORE = table1 == null || table1["STORE"] == null ? string.Empty : table1["STORE"].ToString(),
                                                                                               DECORATION_FLAG = table1 == null || table1["DECORATION_FLAG"] == null ? string.Empty : table1["DECORATION_FLAG"].ToString(),
                                                                                               STATUS = table1 == null || table1["STATUS"] == null ? string.Empty : table1["STATUS"].ToString(),
                                                                                               MODIFIED_DATE = table1 == null || table1["MODIFIED_DATE"] == null ? string.Empty : table1["MODIFIED_DATE"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table ITEMSELLPRICE
                                                                        var sql_insert_data_Hamper = String.Format(@"INSERT INTO `aeon_pop`.`hamper`(`PACK_SKU`,`DESCRIPTION`,`PACK_TYPE`,`SKU`,`QTY_PER_SKU`,`QTY_UOM`,`STORE`
                                                                                                                                ,`DECORATION_FLAG`,`STATUS`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                                                        foreach (var result in result_table)
                                                                        {
                                                                            //get data
                                                                            string PACK_SKU = result.PACK_SKU;
                                                                            string DESCRIPTION = "";
                                                                            string temp_desc = result.DESCRIPTION;
                                                                            if (temp_desc.Contains("\""))
                                                                            {
                                                                                DESCRIPTION = temp_desc.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                DESCRIPTION = result.DESCRIPTION;
                                                                            }

                                                                            string PACK_TYPE = result.PACK_TYPE;
                                                                            string SKU = result.SKU;
                                                                            string QTY_PER_SKU = result.QTY_PER_SKU;
                                                                            string QTY_UOM = result.QTY_UOM;
                                                                            string STORE = result.STORE;
                                                                            string DECORATION_FLAG = result.DECORATION_FLAG;
                                                                            string STATUS = result.STATUS;
                                                                            string MODIFIED_DATE = result.MODIFIED_DATE;
                                                                            string FILE_ID = log_fileid;

                                                                            sql_insert_data_Hamper += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}"",""{10}""),"
                                                                                                                        , PACK_SKU, DESCRIPTION, PACK_TYPE, SKU, QTY_PER_SKU, QTY_UOM, STORE
                                                                                                                        , DECORATION_FLAG, STATUS, MODIFIED_DATE, FILE_ID);
                                                                        }

                                                                        if (result_table.Count() > 0)
                                                                        {
                                                                            connection.Open();
                                                                            sql_insert_data_Hamper = sql_insert_data_Hamper.Substring(0, sql_insert_data_Hamper.Length - 1);
                                                                            var cmd_insert_data_Hamper = new MySqlCommand(sql_insert_data_Hamper, connection);
                                                                            MySqlDataReader rdr_insert_data_Hamper = cmd_insert_data_Hamper.ExecuteReader();
                                                                            connection.Close();
                                                                        }

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                                                        */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 13)
                            {
                                if (filename.Substring(0, 13) == "ITEMPRICECHG_")
                                {
                                    #region ItemPriceChange new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table hamper
                                        var sql_insert_data_ItemPriceChange = String.Format(@"INSERT INTO `aeon_pop`.`pricechange_temp`(`PRICE_CHANGE_NO`,`DEPARTMENT`,`TRANS_TYPE`,`REASON`,`EVENT_ID`
                                                                                            ,`PRICE_CHANGE_TYPE`,`PRICE_CHANGE_TYPE_VALUE`,`PROMOTION_TYPE`,`START_DATE`,`DAILY_START_TIME`,`END_DATE`
                                                                                            ,`DAILY_END_TIME`,`STATUS`,`STORE`,`SKU`,`LAST_SELL_PRICE`,`LAST_SELL_UNIT`,`NEW_SELL_PRICE`,`CREATED_DATE`
                                                                                            ,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Split(',');
                                            line++;
                                            lineeeeee++;

                                            //get data
                                            string PRICE_CHANGE_NO = rows[0].ToString();
                                            string DEPARTMENT = rows[5].ToString();
                                            string TRANS_TYPE = rows[2].ToString();
                                            string REASON = rows[3].ToString();
                                            string EVENT_ID = rows[13].ToString();
                                            string PRICE_CHANGE_TYPE = rows[12].ToString();
                                            string PRICE_CHANGE_TYPE_VALUE = rows[24].ToString();
                                            string PROMOTION_TYPE = rows[15].ToString();
                                            string START_DATE = rows[6].ToString();
                                            string DAILY_START_TIME = rows[8].ToString();
                                            string END_DATE = rows[7].ToString();
                                            string DAILY_END_TIME = rows[9].ToString();
                                            string STATUS = rows[11].ToString();
                                            string STORE = rows[16].ToString();
                                            string SKU = rows[17].ToString();
                                            string LAST_SELL_PRICE = rows[18].ToString();
                                            string LAST_SELL_UNIT = rows[19].ToString();
                                            string NEW_SELL_PRICE = rows[20].ToString();
                                            string CREATED_DATE = rows[1].ToString();
                                            string MODIFIED_DATE = rows[25].ToString();

                                            string FILE_ID = log_fileid;

                                            sql_insert_data_ItemPriceChange += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}"",""{10}""
                                                                                                    ,""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}"",""{20}""),"
                                                                                        , PRICE_CHANGE_NO, DEPARTMENT, TRANS_TYPE, REASON, EVENT_ID, PRICE_CHANGE_TYPE, PRICE_CHANGE_TYPE_VALUE, PROMOTION_TYPE
                                                                                        , START_DATE, DAILY_START_TIME, END_DATE, DAILY_END_TIME, STATUS, STORE, SKU, LAST_SELL_PRICE, LAST_SELL_UNIT, NEW_SELL_PRICE
                                                                                        , CREATED_DATE, MODIFIED_DATE, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_ItemPriceChange = sql_insert_data_ItemPriceChange.Substring(0, sql_insert_data_ItemPriceChange.Length - 1);
                                                var cmd_insert_data_ItemPriceChange = new MySqlCommand(sql_insert_data_ItemPriceChange, connection);
                                                MySqlDataReader rdr_insert_data_ItemPriceChange = cmd_insert_data_ItemPriceChange.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_ItemPriceChange = String.Format(@"INSERT INTO `aeon_pop`.`pricechange_temp`(`PRICE_CHANGE_NO`,`DEPARTMENT`,`TRANS_TYPE`,`REASON`,`EVENT_ID`
                                                                                            ,`PRICE_CHANGE_TYPE`,`PRICE_CHANGE_TYPE_VALUE`,`PROMOTION_TYPE`,`START_DATE`,`DAILY_START_TIME`,`END_DATE`
                                                                                            ,`DAILY_END_TIME`,`STATUS`,`STORE`,`SKU`,`LAST_SELL_PRICE`,`LAST_SELL_UNIT`,`NEW_SELL_PRICE`,`CREATED_DATE`
                                                                                            ,`MODIFIED_DATE`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_ItemPriceChange = sql_insert_data_ItemPriceChange.Substring(0, sql_insert_data_ItemPriceChange.Length - 1);
                                            var cmd_insert_data_ItemPriceChange = new MySqlCommand(sql_insert_data_ItemPriceChange, connection);
                                            MySqlDataReader rdr_insert_data_ItemPriceChange = cmd_insert_data_ItemPriceChange.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region ItemPriceChange
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu

                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_itempricechange = String.Format(@"SELECT * 
                                                                                    FROM
                                                                                    (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(STORE, PRICE_CHANGE_NO, SKU) ORDER BY FILE_ID DESC) AS row_num
                                                                                    FROM aeon_pop.pricechange) T0
                                                                                    WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_itempricechange = new MySqlCommand(sql_get_cur_itempricechange, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_itempricechange = new MySqlDataAdapter();
                                                                        MyAdapter_cur_itempricechange.SelectCommand = cmd_get_cur_itempricechange;
                                                                        MyAdapter_cur_itempricechange.SelectCommand.CommandTimeout = 300;
                                                                        DataTable dTable_ItemPriceChange_Cur = new DataTable();
                                                                        MyAdapter_cur_itempricechange.Fill(dTable_ItemPriceChange_Cur);
                                                                        connection.Close();

                                                                        //get dữ liệu mới
                                                                        DataTable dTable_ItemPriceChange_New = ConvertCSVtoDataTable_ItemPriceChange(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_ItemPriceChange_New.AsEnumerable()
                                                                                           join table2 in dTable_ItemPriceChange_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["STORE"] == null ? String.Empty : table1["STORE"].ToString(),
                                                                                               con2 = table1["PRICE_CHANGE_NO"] == null ? String.Empty : table1["PRICE_CHANGE_NO"].ToString(),
                                                                                               con3 = table1["SKU"] == null ? String.Empty : table1["SKU"].ToString()
                                                                                           }
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["STORE"] == null ? String.Empty : table2["STORE"].ToString(),
                                                                                               con2 = table2["PRICE_CHANGE_NO"] == null ? String.Empty : table2["PRICE_CHANGE_NO"].ToString(),
                                                                                               con3 = table2["SKU"] == null ? String.Empty : table2["SKU"].ToString()
                                                                                           }
                                                                                           into _Table3
                                                                                           from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DEPARTMENT"].ToString()) != table1["DEPARTMENT"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["TRANS_TYPE"].ToString()) != table1["TRANS_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["REASON"].ToString()) != table1["REASON"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["EVENT_ID"].ToString()) != table1["EVENT_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PRICE_CHANGE_TYPE"].ToString()) != table1["PRICE_CHANGE_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PRICE_CHANGE_TYPE_VALUE"].ToString()) != table1["PRICE_CHANGE_TYPE_VALUE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMOTION_TYPE"].ToString()) != table1["PROMOTION_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["START_DATE"].ToString()) != table1["START_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DAILY_START_TIME"].ToString()) != table1["DAILY_START_TIME"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["END_DATE"].ToString()) != table1["END_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DAILY_END_TIME"].ToString()) != table1["DAILY_END_TIME"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["STATUS"].ToString()) != table1["STATUS"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["LAST_SELL_PRICE"].ToString()) != table1["LAST_SELL_PRICE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["LAST_SELL_UNIT"].ToString()) != table1["LAST_SELL_UNIT"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["NEW_SELL_PRICE"].ToString()) != table1["NEW_SELL_PRICE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["CREATED_DATE"].ToString()) != table1["CREATED_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MODIFIED_DATE"].ToString()) != table1["MODIFIED_DATE"].ToString())
                                                                                                     )
                                                                                           select new
                                                                                           {
                                                                                               PRICE_CHANGE_NO = table1 == null || table1["PRICE_CHANGE_NO"] == null ? string.Empty : table1["PRICE_CHANGE_NO"].ToString(),
                                                                                               DEPARTMENT = table1 == null || table1["DEPARTMENT"] == null ? string.Empty : table1["DEPARTMENT"].ToString(),
                                                                                               TRANS_TYPE = table1 == null || table1["TRANS_TYPE"] == null ? string.Empty : table1["TRANS_TYPE"].ToString(),
                                                                                               REASON = table1 == null || table1["REASON"] == null ? string.Empty : table1["REASON"].ToString(),
                                                                                               EVENT_ID = table1 == null || table1["EVENT_ID"] == null ? string.Empty : table1["EVENT_ID"].ToString(),
                                                                                               PRICE_CHANGE_TYPE = table1 == null || table1["PRICE_CHANGE_TYPE"] == null ? string.Empty : table1["PRICE_CHANGE_TYPE"].ToString(),
                                                                                               PRICE_CHANGE_TYPE_VALUE = table1 == null || table1["PRICE_CHANGE_TYPE_VALUE"] == null ? string.Empty : table1["PRICE_CHANGE_TYPE_VALUE"].ToString(),
                                                                                               PROMOTION_TYPE = table1 == null || table1["PROMOTION_TYPE"] == null ? string.Empty : table1["PROMOTION_TYPE"].ToString(),
                                                                                               START_DATE = table1 == null || table1["START_DATE"] == null ? string.Empty : table1["START_DATE"].ToString(),
                                                                                               DAILY_START_TIME = table1 == null || table1["DAILY_START_TIME"] == null ? string.Empty : table1["DAILY_START_TIME"].ToString(),
                                                                                               END_DATE = table1 == null || table1["END_DATE"] == null ? string.Empty : table1["END_DATE"].ToString(),
                                                                                               DAILY_END_TIME = table1 == null || table1["DAILY_END_TIME"] == null ? string.Empty : table1["DAILY_END_TIME"].ToString(),
                                                                                               STATUS = table1 == null || table1["STATUS"] == null ? string.Empty : table1["STATUS"].ToString(),
                                                                                               STORE = table1 == null || table1["STORE"] == null ? string.Empty : table1["STORE"].ToString(),
                                                                                               SKU = table1 == null || table1["SKU"] == null ? string.Empty : table1["SKU"].ToString(),
                                                                                               LAST_SELL_PRICE = table1 == null || table1["LAST_SELL_PRICE"] == null ? string.Empty : table1["LAST_SELL_PRICE"].ToString(),
                                                                                               LAST_SELL_UNIT = table1 == null || table1["LAST_SELL_UNIT"] == null ? string.Empty : table1["LAST_SELL_UNIT"].ToString(),
                                                                                               NEW_SELL_PRICE = table1 == null || table1["NEW_SELL_PRICE"] == null ? string.Empty : table1["NEW_SELL_PRICE"].ToString(),
                                                                                               CREATED_DATE = table1 == null || table1["CREATED_DATE"] == null ? string.Empty : table1["CREATED_DATE"].ToString(),
                                                                                               MODIFIED_DATE = table1 == null || table1["MODIFIED_DATE"] == null ? string.Empty : table1["MODIFIED_DATE"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table 
                                                                        var sql_insert_data_ItemPriceChange = String.Format(@"INSERT INTO `aeon_pop`.`pricechange`(`PRICE_CHANGE_NO`,`DEPARTMENT`,`TRANS_TYPE`,`REASON`,`EVENT_ID`
                                                                                                                                ,`PRICE_CHANGE_TYPE`,`PRICE_CHANGE_TYPE_VALUE`,`PROMOTION_TYPE`,`START_DATE`,`DAILY_START_TIME`,`END_DATE`
                                                                                                                                ,`DAILY_END_TIME`,`STATUS`,`STORE`,`SKU`,`LAST_SELL_PRICE`,`LAST_SELL_UNIT`,`NEW_SELL_PRICE`,`CREATED_DATE`
                                                                                                                                ,`MODIFIED_DATE`,`FILE_ID`)VALUES");
                                                                        foreach (var result in result_table)
                                                                        {
                                                                            //get data
                                                                            string PRICE_CHANGE_NO = result.PRICE_CHANGE_NO;
                                                                            string DEPARTMENT = result.DEPARTMENT;
                                                                            string TRANS_TYPE = result.TRANS_TYPE;
                                                                            string REASON = result.REASON;
                                                                            string EVENT_ID = result.EVENT_ID;
                                                                            string PRICE_CHANGE_TYPE = result.PRICE_CHANGE_TYPE;
                                                                            string PRICE_CHANGE_TYPE_VALUE = result.PRICE_CHANGE_TYPE_VALUE;
                                                                            string PROMOTION_TYPE = result.PROMOTION_TYPE;
                                                                            string START_DATE = result.START_DATE;
                                                                            string DAILY_START_TIME = result.DAILY_START_TIME;
                                                                            string END_DATE = result.END_DATE;
                                                                            string DAILY_END_TIME = result.DAILY_END_TIME;
                                                                            string STATUS = result.STATUS;
                                                                            string STORE = result.STORE;
                                                                            string SKU = result.SKU;
                                                                            string LAST_SELL_PRICE = result.LAST_SELL_PRICE;
                                                                            string LAST_SELL_UNIT = result.LAST_SELL_UNIT;
                                                                            string NEW_SELL_PRICE = result.NEW_SELL_PRICE;
                                                                            string CREATED_DATE = result.CREATED_DATE;
                                                                            string MODIFIED_DATE = result.MODIFIED_DATE;

                                                                            string FILE_ID = log_fileid;

                                                                            sql_insert_data_ItemPriceChange += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}"",""{10}""
                                                                                                                                        ,""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}"",""{20}""),"
                                                                                                                        , PRICE_CHANGE_NO, DEPARTMENT, TRANS_TYPE, REASON, EVENT_ID, PRICE_CHANGE_TYPE, PRICE_CHANGE_TYPE_VALUE, PROMOTION_TYPE
                                                                                                                        , START_DATE, DAILY_START_TIME, END_DATE, DAILY_END_TIME, STATUS, STORE, SKU, LAST_SELL_PRICE, LAST_SELL_UNIT, NEW_SELL_PRICE
                                                                                                                        , CREATED_DATE, MODIFIED_DATE, FILE_ID);
                                                                        }

                                                                        if (result_table.Count() > 0)
                                                                        {
                                                                            connection.Open();
                                                                            sql_insert_data_ItemPriceChange = sql_insert_data_ItemPriceChange.Substring(0, sql_insert_data_ItemPriceChange.Length - 1);
                                                                            var cmd_insert_data_ItemPriceChange = new MySqlCommand(sql_insert_data_ItemPriceChange, connection);
                                                                            MySqlDataReader rdr_insert_data_ItemPriceChange = cmd_insert_data_ItemPriceChange.ExecuteReader();
                                                                            connection.Close();
                                                                        }

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                                                        */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 9)
                            {
                                if (filename.Substring(0, 9) == "MIXMATCH_")
                                {
                                    #region MIXMATCH_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_MixMatch = String.Format(@"INSERT INTO `aeon_pop`.`mix_match`(`PROMO_NO`,`PROMO_TYPE`,`PROMO_DESC`,`STATUS`,`MAX_OR_PARTIAL`
                                                                                                ,`START_DATE`,`START_TIME`,`END_DATE`,`END_TIME`,`TTL_PROMO_QTY`,`TTL_PROMO_PRICE`,`PLU_COUNT`,`EVENT_ID`,`STORE`
                                                                                                ,`SKU`,`SEQ`,`NORMAL_PRICE`,`SELL_UOM`,`PROMO_QTY`,`FOC_QTY`,`PROMO_PRICE`,`FOC_SKU`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Split(',');
                                            line++;

                                            //get data
                                            string PROMO_NO = rows[0].ToString();
                                            string PROMO_TYPE = rows[1].ToString();
                                            string PROMO_DESC = "";
                                            string temp_desc = rows[2].ToString();
                                            if (temp_desc.Contains("\""))
                                            {
                                                PROMO_DESC = temp_desc.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                PROMO_DESC = rows[2].ToString();
                                            }
                                            string STATUS = rows[7].ToString();
                                            string MAX_OR_PARTIAL = rows[11].ToString();
                                            string START_DATE = rows[3].ToString();
                                            string START_TIME = rows[5].ToString();
                                            string END_DATE = rows[4].ToString();
                                            string END_TIME = rows[6].ToString();
                                            string TTL_PROMO_QTY = rows[24].ToString();
                                            string TTL_PROMO_PRICE = rows[25].ToString();
                                            string PLU_COUNT = rows[26].ToString();
                                            string EVENT_ID = rows[14].ToString();
                                            string STORE = rows[15].ToString();
                                            string SKU = rows[16].ToString();
                                            string SEQ = rows[17].ToString();
                                            string NORMAL_PRICE = rows[18].ToString();
                                            string SELL_UOM = rows[19].ToString();
                                            string PROMO_QTY = rows[20].ToString();
                                            string FOC_QTY = rows[22].ToString();
                                            string PROMO_PRICE = rows[21].ToString();
                                            string FOC_SKU = rows[27].ToString();
                                            string MODIFIED_DATE = rows[28].ToString();

                                            string FILE_ID = log_fileid;

                                            sql_insert_data_MixMatch += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                        ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                        ,""{20}"",""{21}"",""{22}"",""{23}""),"
                                                                                        , PROMO_NO, PROMO_TYPE, PROMO_DESC, STATUS, MAX_OR_PARTIAL, START_DATE, START_TIME, END_DATE, END_TIME
                                                                                        , TTL_PROMO_QTY, TTL_PROMO_PRICE, PLU_COUNT, EVENT_ID, STORE, SKU, SEQ, NORMAL_PRICE, SELL_UOM, PROMO_QTY
                                                                                        , FOC_QTY, PROMO_PRICE, FOC_SKU, MODIFIED_DATE, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_MixMatch = sql_insert_data_MixMatch.Substring(0, sql_insert_data_MixMatch.Length - 1);
                                                var cmd_insert_data_MixMatch = new MySqlCommand(sql_insert_data_MixMatch, connection);
                                                MySqlDataReader rdr_insert_data_MixMatch = cmd_insert_data_MixMatch.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_MixMatch = String.Format(@"INSERT INTO `aeon_pop`.`mix_match`(`PROMO_NO`,`PROMO_TYPE`,`PROMO_DESC`,`STATUS`,`MAX_OR_PARTIAL`
                                                                                                ,`START_DATE`,`START_TIME`,`END_DATE`,`END_TIME`,`TTL_PROMO_QTY`,`TTL_PROMO_PRICE`,`PLU_COUNT`,`EVENT_ID`,`STORE`
                                                                                                ,`SKU`,`SEQ`,`NORMAL_PRICE`,`SELL_UOM`,`PROMO_QTY`,`FOC_QTY`,`PROMO_PRICE`,`FOC_SKU`,`MODIFIED_DATE`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_MixMatch = sql_insert_data_MixMatch.Substring(0, sql_insert_data_MixMatch.Length - 1);
                                            var cmd_insert_data_MixMatch = new MySqlCommand(sql_insert_data_MixMatch, connection);
                                            MySqlDataReader rdr_insert_data_MixMatch = cmd_insert_data_MixMatch.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region MixMatch old
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu
                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_mixmatch = String.Format(@"SELECT * 
                                                                                    FROM
                                                                                    (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(STORE, SKU, PROMO_NO) ORDER BY FILE_ID DESC) AS row_num
                                                                                    FROM aeon_pop.mix_match) T0
                                                                                    WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_mixmatch = new MySqlCommand(sql_get_cur_mixmatch, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_mixmatch = new MySqlDataAdapter();
                                                                        MyAdapter_cur_mixmatch.SelectCommand = cmd_get_cur_mixmatch;
                                                                        DataTable dTable_MixMatch_Cur = new DataTable();
                                                                        MyAdapter_cur_mixmatch.Fill(dTable_MixMatch_Cur);
                                                                        connection.Close();

                                                                        //get dữ liệu mới
                                                                        DataTable dTable_MixMatch_New = ConvertCSVtoDataTable_MixMatch(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_MixMatch_New.AsEnumerable()
                                                                                           join table2 in dTable_MixMatch_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["STORE"] == null ? String.Empty : table1["STORE"].ToString(),
                                                                                               con2 = table1["SKU"] == null ? String.Empty : table1["SKU"].ToString(),
                                                                                               con3 = table1["PROMO_NO"] == null ? String.Empty : table1["PROMO_NO"].ToString()
                                                                                           }
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["STORE"] == null ? String.Empty : table2["STORE"].ToString(),
                                                                                               con2 = table2["SKU"] == null ? String.Empty : table2["SKU"].ToString(),
                                                                                               con3 = table2["PROMO_NO"] == null ? String.Empty : table2["PROMO_NO"].ToString()
                                                                                           }
                                                                                           into _Table3
                                                                                           from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMO_TYPE"].ToString()) != table1["PROMO_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMO_DESC"].ToString()) != table1["PROMO_DESC"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["STATUS"].ToString()) != table1["STATUS"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MAX_OR_PARTIAL"].ToString()) != table1["MAX_OR_PARTIAL"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["START_DATE"].ToString()) != table1["START_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["START_TIME"].ToString()) != table1["START_TIME"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["END_DATE"].ToString()) != table1["END_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["END_TIME"].ToString()) != table1["END_TIME"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["TTL_PROMO_QTY"].ToString()) != table1["TTL_PROMO_QTY"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["TTL_PROMO_PRICE"].ToString()) != table1["TTL_PROMO_PRICE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PLU_COUNT"].ToString()) != table1["PLU_COUNT"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["EVENT_ID"].ToString()) != table1["EVENT_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SEQ"].ToString()) != table1["SEQ"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["NORMAL_PRICE"].ToString()) != table1["NORMAL_PRICE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SELL_UOM"].ToString()) != table1["SELL_UOM"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMO_QTY"].ToString()) != table1["PROMO_QTY"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["FOC_QTY"].ToString()) != table1["FOC_QTY"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMO_PRICE"].ToString()) != table1["PROMO_PRICE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["FOC_SKU"].ToString()) != table1["FOC_SKU"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MODIFIED_DATE"].ToString()) != table1["MODIFIED_DATE"].ToString())
                                                                                                    )
                                                                                           select new
                                                                                           {
                                                                                               PROMO_NO = table1 == null || table1["PROMO_NO"] == null ? string.Empty : table1["PROMO_NO"].ToString(),
                                                                                               PROMO_TYPE = table1 == null || table1["PROMO_TYPE"] == null ? string.Empty : table1["PROMO_TYPE"].ToString(),
                                                                                               PROMO_DESC = table1 == null || table1["PROMO_DESC"] == null ? string.Empty : table1["PROMO_DESC"].ToString(),
                                                                                               STATUS = table1 == null || table1["STATUS"] == null ? string.Empty : table1["STATUS"].ToString(),
                                                                                               MAX_OR_PARTIAL = table1 == null || table1["MAX_OR_PARTIAL"] == null ? string.Empty : table1["MAX_OR_PARTIAL"].ToString(),
                                                                                               START_DATE = table1 == null || table1["START_DATE"] == null ? string.Empty : table1["START_DATE"].ToString(),
                                                                                               START_TIME = table1 == null || table1["START_TIME"] == null ? string.Empty : table1["START_TIME"].ToString(),
                                                                                               END_DATE = table1 == null || table1["END_DATE"] == null ? string.Empty : table1["END_DATE"].ToString(),
                                                                                               END_TIME = table1 == null || table1["END_TIME"] == null ? string.Empty : table1["END_TIME"].ToString(),
                                                                                               TTL_PROMO_QTY = table1 == null || table1["TTL_PROMO_QTY"] == null ? string.Empty : table1["TTL_PROMO_QTY"].ToString(),
                                                                                               TTL_PROMO_PRICE = table1 == null || table1["TTL_PROMO_PRICE"] == null ? string.Empty : table1["TTL_PROMO_PRICE"].ToString(),
                                                                                               PLU_COUNT = table1 == null || table1["PLU_COUNT"] == null ? string.Empty : table1["PLU_COUNT"].ToString(),
                                                                                               EVENT_ID = table1 == null || table1["EVENT_ID"] == null ? string.Empty : table1["EVENT_ID"].ToString(),
                                                                                               STORE = table1 == null || table1["STORE"] == null ? string.Empty : table1["STORE"].ToString(),
                                                                                               SKU = table1 == null || table1["SKU"] == null ? string.Empty : table1["SKU"].ToString(),
                                                                                               SEQ = table1 == null || table1["SEQ"] == null ? string.Empty : table1["SEQ"].ToString(),
                                                                                               NORMAL_PRICE = table1 == null || table1["NORMAL_PRICE"] == null ? string.Empty : table1["NORMAL_PRICE"].ToString(),
                                                                                               SELL_UOM = table1 == null || table1["SELL_UOM"] == null ? string.Empty : table1["SELL_UOM"].ToString(),
                                                                                               PROMO_QTY = table1 == null || table1["PROMO_QTY"] == null ? string.Empty : table1["PROMO_QTY"].ToString(),
                                                                                               FOC_QTY = table1 == null || table1["FOC_QTY"] == null ? string.Empty : table1["FOC_QTY"].ToString(),
                                                                                               PROMO_PRICE = table1 == null || table1["PROMO_PRICE"] == null ? string.Empty : table1["PROMO_PRICE"].ToString(),
                                                                                               FOC_SKU = table1 == null || table1["FOC_SKU"] == null ? string.Empty : table1["FOC_SKU"].ToString(),
                                                                                               MODIFIED_DATE = table1 == null || table1["MODIFIED_DATE"] == null ? string.Empty : table1["MODIFIED_DATE"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table
                                                                        var sql_insert_data_MixMatch = String.Format(@"INSERT INTO `aeon_pop`.`mix_match`(`PROMO_NO`,`PROMO_TYPE`,`PROMO_DESC`,`STATUS`,`MAX_OR_PARTIAL`
                                                                                                                                    ,`START_DATE`,`START_TIME`,`END_DATE`,`END_TIME`,`TTL_PROMO_QTY`,`TTL_PROMO_PRICE`,`PLU_COUNT`,`EVENT_ID`,`STORE`
                                                                                                                                    ,`SKU`,`SEQ`,`NORMAL_PRICE`,`SELL_UOM`,`PROMO_QTY`,`FOC_QTY`,`PROMO_PRICE`,`FOC_SKU`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                                                        foreach (var result in result_table)
                                                                        {
                                                                            //get data
                                                                            string PROMO_NO = result.PROMO_NO;
                                                                            string PROMO_TYPE = result.PROMO_TYPE;
                                                                            string PROMO_DESC = "";
                                                                            string temp_desc = result.PROMO_DESC;
                                                                            if (temp_desc.Contains("\""))
                                                                            {
                                                                                PROMO_DESC = temp_desc.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                PROMO_DESC = result.PROMO_DESC;
                                                                            }
                                                                            string STATUS = result.STATUS;
                                                                            string MAX_OR_PARTIAL = result.MAX_OR_PARTIAL;
                                                                            string START_DATE = result.START_DATE;
                                                                            string START_TIME = result.START_TIME;
                                                                            string END_DATE = result.END_DATE;
                                                                            string END_TIME = result.END_TIME;
                                                                            string TTL_PROMO_QTY = result.TTL_PROMO_QTY;
                                                                            string TTL_PROMO_PRICE = result.TTL_PROMO_PRICE;
                                                                            string PLU_COUNT = result.PLU_COUNT;
                                                                            string EVENT_ID = result.EVENT_ID;
                                                                            string STORE = result.STORE;
                                                                            string SKU = result.SKU;
                                                                            string SEQ = result.SEQ;
                                                                            string NORMAL_PRICE = result.NORMAL_PRICE;
                                                                            string SELL_UOM = result.SELL_UOM;
                                                                            string PROMO_QTY = result.PROMO_QTY;
                                                                            string FOC_QTY = result.FOC_QTY;
                                                                            string PROMO_PRICE = result.PROMO_PRICE;
                                                                            string FOC_SKU = result.FOC_SKU;
                                                                            string MODIFIED_DATE = result.MODIFIED_DATE;

                                                                            string FILE_ID = log_fileid;

                                                                            sql_insert_data_MixMatch += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                                                            ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                                                            ,""{20}"",""{21}"",""{22}"",""{23}""),"
                                                                                                                        , PROMO_NO, PROMO_TYPE, PROMO_DESC, STATUS, MAX_OR_PARTIAL, START_DATE, START_TIME, END_DATE, END_TIME
                                                                                                                        , TTL_PROMO_QTY, TTL_PROMO_PRICE, PLU_COUNT, EVENT_ID, STORE, SKU, SEQ, NORMAL_PRICE, SELL_UOM, PROMO_QTY
                                                                                                                        , FOC_QTY, PROMO_PRICE, FOC_SKU, MODIFIED_DATE, FILE_ID);
                                                                        }

                                                                        if (result_table.Count() > 0)
                                                                        {
                                                                            connection.Open();
                                                                            sql_insert_data_MixMatch = sql_insert_data_MixMatch.Substring(0, sql_insert_data_MixMatch.Length - 1);
                                                                            var cmd_insert_data_MixMatch = new MySqlCommand(sql_insert_data_MixMatch, connection);
                                                                            MySqlDataReader rdr_insert_data_MixMatch = cmd_insert_data_MixMatch.ExecuteReader();
                                                                            connection.Close();
                                                                        }

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                                                        */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 12)
                            {
                                if (filename.Substring(0, 12) == "GRPPRICECHG_")
                                {
                                    #region GRPPRICECHG_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_GroupPriceChange = String.Format(@"INSERT INTO `aeon_pop`.`group_pricechange`(`PRICE_CHANGE_NO`,`TRANS_TYPE`,`START_DATE`,`START_TIME`
                                                                                            ,`END_DATE`,`END_TIME`,`CATEGORY`,`STORE`,`EVENT_ID`,`EXCLUDE_SEASON_ID`,`PRICE_CHANGE_TYPE`
                                                                                            ,`PRICE_CHANGE_TYPE_VALUE`,`REASON`,`PROMOTION_TYPE`,`STATUS`,`CREATED_DATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Split(',');
                                            line++;

                                            //get data
                                            string PRICE_CHANGE_NO = rows[0].ToString();
                                            string TRANS_TYPE = rows[2].ToString();
                                            string START_DATE = rows[5].ToString();
                                            string START_TIME = rows[7].ToString();
                                            string END_DATE = rows[6].ToString();
                                            string END_TIME = rows[8].ToString();
                                            string CATEGORY = rows[16].ToString();
                                            string STORE = rows[15].ToString();
                                            string EVENT_ID = rows[9].ToString();
                                            string EXCLUDE_SEASON_ID = rows[10].ToString();
                                            string PRICE_CHANGE_TYPE = rows[12].ToString();
                                            string PRICE_CHANGE_TYPE_VALUE = rows[13].ToString();
                                            string REASON = rows[3].ToString();
                                            string PROMOTION_TYPE = rows[14].ToString();
                                            string STATUS = rows[11].ToString();
                                            string CREATED_DATE = rows[1].ToString();
                                            string MODIFIED_DATE = rows[18].ToString();
                                            string FILE_ID = log_fileid;

                                            sql_insert_data_GroupPriceChange += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                            ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}""),"
                                                                                        , PRICE_CHANGE_NO, TRANS_TYPE, START_DATE, START_TIME, END_DATE, END_TIME, CATEGORY
                                                                                        , STORE, EVENT_ID, EXCLUDE_SEASON_ID, PRICE_CHANGE_TYPE, PRICE_CHANGE_TYPE_VALUE, REASON
                                                                                        , PROMOTION_TYPE, STATUS, CREATED_DATE, MODIFIED_DATE, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_GroupPriceChange = sql_insert_data_GroupPriceChange.Substring(0, sql_insert_data_GroupPriceChange.Length - 1);
                                                var cmd_insert_data_GroupPriceChange = new MySqlCommand(sql_insert_data_GroupPriceChange, connection);
                                                MySqlDataReader rdr_insert_data_GroupPriceChange = cmd_insert_data_GroupPriceChange.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_GroupPriceChange = String.Format(@"INSERT INTO `aeon_pop`.`group_pricechange`(`PRICE_CHANGE_NO`,`TRANS_TYPE`,`START_DATE`,`START_TIME`
                                                                                            ,`END_DATE`,`END_TIME`,`CATEGORY`,`STORE`,`EVENT_ID`,`EXCLUDE_SEASON_ID`,`PRICE_CHANGE_TYPE`
                                                                                            ,`PRICE_CHANGE_TYPE_VALUE`,`REASON`,`PROMOTION_TYPE`,`STATUS`,`CREATED_DATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_GroupPriceChange = sql_insert_data_GroupPriceChange.Substring(0, sql_insert_data_GroupPriceChange.Length - 1);
                                            var cmd_insert_data_GroupPriceChange = new MySqlCommand(sql_insert_data_GroupPriceChange, connection);
                                            MySqlDataReader rdr_insert_data_GroupPriceChange = cmd_insert_data_GroupPriceChange.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region Group_PriceChange
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu
                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_grouppricechange = String.Format(@"SELECT * 
                                                                                    FROM
                                                                                    (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(STORE, PRICE_CHANGE_NO) ORDER BY FILE_ID DESC) AS row_num
                                                                                    FROM aeon_pop.group_pricechange) T0
                                                                                    WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_grouppricechange = new MySqlCommand(sql_get_cur_grouppricechange, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_grouppricechange = new MySqlDataAdapter();
                                                                        MyAdapter_cur_grouppricechange.SelectCommand = cmd_get_cur_grouppricechange;
                                                                        DataTable dTable_GroupPriceChange_Cur = new DataTable();
                                                                        MyAdapter_cur_grouppricechange.Fill(dTable_GroupPriceChange_Cur);
                                                                        connection.Close();

                                                                        //get dữ liệu mới
                                                                        DataTable dTable_GroupPriceChange_New = ConvertCSVtoDataTable_GroupPriceChange(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_GroupPriceChange_New.AsEnumerable()
                                                                                           join table2 in dTable_GroupPriceChange_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["STORE"] == null ? String.Empty : table1["STORE"].ToString(),
                                                                                               con2 = table1["PRICE_CHANGE_NO"] == null ? String.Empty : table1["PRICE_CHANGE_NO"].ToString()
                                                                                           }
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["STORE"] == null ? String.Empty : table2["STORE"].ToString(),
                                                                                               con2 = table2["PRICE_CHANGE_NO"] == null ? String.Empty : table2["PRICE_CHANGE_NO"].ToString()
                                                                                           }
                                                                                           into _Table3
                                                                                           from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["TRANS_TYPE"].ToString()) != table1["TRANS_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["START_DATE"].ToString()) != table1["START_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["START_TIME"].ToString()) != table1["START_TIME"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["END_DATE"].ToString()) != table1["END_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["END_TIME"].ToString()) != table1["END_TIME"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["CATEGORY"].ToString()) != table1["CATEGORY"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["EVENT_ID"].ToString()) != table1["EVENT_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["EXCLUDE_SEASON_ID"].ToString()) != table1["EXCLUDE_SEASON_ID"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PRICE_CHANGE_TYPE"].ToString()) != table1["PRICE_CHANGE_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PRICE_CHANGE_TYPE_VALUE"].ToString()) != table1["PRICE_CHANGE_TYPE_VALUE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["REASON"].ToString()) != table1["REASON"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMOTION_TYPE"].ToString()) != table1["PROMOTION_TYPE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["STATUS"].ToString()) != table1["STATUS"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["CREATED_DATE"].ToString()) != table1["CREATED_DATE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MODIFIED_DATE"].ToString()) != table1["MODIFIED_DATE"].ToString())
                                                                                                    )
                                                                                           select new
                                                                                           {
                                                                                               PRICE_CHANGE_NO = table1 == null || table1["PRICE_CHANGE_NO"] == null ? string.Empty : table1["PRICE_CHANGE_NO"].ToString(),
                                                                                               TRANS_TYPE = table1 == null || table1["TRANS_TYPE"] == null ? string.Empty : table1["TRANS_TYPE"].ToString(),
                                                                                               START_DATE = table1 == null || table1["START_DATE"] == null ? string.Empty : table1["START_DATE"].ToString(),
                                                                                               START_TIME = table1 == null || table1["START_TIME"] == null ? string.Empty : table1["START_TIME"].ToString(),
                                                                                               END_DATE = table1 == null || table1["END_DATE"] == null ? string.Empty : table1["END_DATE"].ToString(),
                                                                                               END_TIME = table1 == null || table1["END_TIME"] == null ? string.Empty : table1["END_TIME"].ToString(),
                                                                                               CATEGORY = table1 == null || table1["CATEGORY"] == null ? string.Empty : table1["CATEGORY"].ToString(),
                                                                                               STORE = table1 == null || table1["STORE"] == null ? string.Empty : table1["STORE"].ToString(),
                                                                                               EVENT_ID = table1 == null || table1["EVENT_ID"] == null ? string.Empty : table1["EVENT_ID"].ToString(),
                                                                                               EXCLUDE_SEASON_ID = table1 == null || table1["EXCLUDE_SEASON_ID"] == null ? string.Empty : table1["EXCLUDE_SEASON_ID"].ToString(),
                                                                                               PRICE_CHANGE_TYPE = table1 == null || table1["PRICE_CHANGE_TYPE"] == null ? string.Empty : table1["PRICE_CHANGE_TYPE"].ToString(),
                                                                                               PRICE_CHANGE_TYPE_VALUE = table1 == null || table1["PRICE_CHANGE_TYPE_VALUE"] == null ? string.Empty : table1["PRICE_CHANGE_TYPE_VALUE"].ToString(),
                                                                                               REASON = table1 == null || table1["REASON"] == null ? string.Empty : table1["REASON"].ToString(),
                                                                                               PROMOTION_TYPE = table1 == null || table1["PROMOTION_TYPE"] == null ? string.Empty : table1["PROMOTION_TYPE"].ToString(),
                                                                                               STATUS = table1 == null || table1["STATUS"] == null ? string.Empty : table1["STATUS"].ToString(),
                                                                                               CREATED_DATE = table1 == null || table1["CREATED_DATE"] == null ? string.Empty : table1["CREATED_DATE"].ToString(),
                                                                                               MODIFIED_DATE = table1 == null || table1["MODIFIED_DATE"] == null ? string.Empty : table1["MODIFIED_DATE"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table
                                                                        var sql_insert_data_GroupPriceChange = String.Format(@"INSERT INTO `aeon_pop`.`group_pricechange`(`PRICE_CHANGE_NO`,`TRANS_TYPE`,`START_DATE`,`START_TIME`
                                                                                                                                ,`END_DATE`,`END_TIME`,`CATEGORY`,`STORE`,`EVENT_ID`,`EXCLUDE_SEASON_ID`,`PRICE_CHANGE_TYPE`
                                                                                                                                ,`PRICE_CHANGE_TYPE_VALUE`,`REASON`,`PROMOTION_TYPE`,`STATUS`,`CREATED_DATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                                                        foreach (var result in result_table)
                                                                        {
                                                                            //get data
                                                                            string PRICE_CHANGE_NO = result.PRICE_CHANGE_NO;
                                                                            string TRANS_TYPE = result.TRANS_TYPE;
                                                                            string START_DATE = result.START_DATE;
                                                                            string START_TIME = result.START_TIME;
                                                                            string END_DATE = result.END_DATE;
                                                                            string END_TIME = result.END_TIME;
                                                                            string CATEGORY = result.CATEGORY;
                                                                            string STORE = result.STORE;
                                                                            string EVENT_ID = result.EVENT_ID;
                                                                            string EXCLUDE_SEASON_ID = result.EXCLUDE_SEASON_ID;
                                                                            string PRICE_CHANGE_TYPE = result.PRICE_CHANGE_TYPE;
                                                                            string PRICE_CHANGE_TYPE_VALUE = result.PRICE_CHANGE_TYPE_VALUE;
                                                                            string REASON = result.REASON;
                                                                            string PROMOTION_TYPE = result.PROMOTION_TYPE;
                                                                            string STATUS = result.STATUS;
                                                                            string CREATED_DATE = result.CREATED_DATE;
                                                                            string MODIFIED_DATE = result.MODIFIED_DATE;
                                                                            string FILE_ID = log_fileid;

                                                                            sql_insert_data_GroupPriceChange += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                                                                ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}""),"
                                                                                                                        , PRICE_CHANGE_NO, TRANS_TYPE, START_DATE, START_TIME, END_DATE, END_TIME, CATEGORY
                                                                                                                        , STORE, EVENT_ID, EXCLUDE_SEASON_ID, PRICE_CHANGE_TYPE, PRICE_CHANGE_TYPE_VALUE, REASON
                                                                                                                        , PROMOTION_TYPE, STATUS, CREATED_DATE, MODIFIED_DATE, FILE_ID);
                                                                        }

                                                                        if (result_table.Count() > 0)
                                                                        {
                                                                            connection.Open();
                                                                            sql_insert_data_GroupPriceChange = sql_insert_data_GroupPriceChange.Substring(0, sql_insert_data_GroupPriceChange.Length - 1);
                                                                            var cmd_insert_data_GroupPriceChange = new MySqlCommand(sql_insert_data_GroupPriceChange, connection);
                                                                            MySqlDataReader rdr_insert_data_GroupPriceChange = cmd_insert_data_GroupPriceChange.ExecuteReader();
                                                                            connection.Close();
                                                                        }

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                    */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 14)
                            {
                                if (filename.Substring(0, 14) == "ITEMSELLPRICE_")
                                {
                                    #region ITEMSELLPRICE_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table ITEMSELLPRICE
                                        var sql_insert_data_ITEMSELLPRICE = String.Format(@"INSERT INTO `aeon_pop`.`item_sell_price_temp`(`STORE`,`SKU`
                                                                                            ,`DESCRIPTION`,`CURRENT_PRICE`,`PROMOTION_FLAG`,`PROMOTION_RETAIL`,`MEMBER_RETAIL`
                                                                                            ,`MEMBER_PROMOTION_FLAG`,`MEMBER_PROMOTION_RETAIL`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;
                                            lineeeeee++;

                                            //get data
                                            string STORE = rows[0].ToString();
                                            string SKU = rows[1].ToString();
                                            string DESCRIPTION = "";
                                            string temp_desc = rows[2].ToString();
                                            if (temp_desc.Contains("\""))
                                            {
                                                DESCRIPTION = temp_desc.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                DESCRIPTION = rows[2].ToString();
                                            }
                                            

                                            string CURRENT_PRICE = rows[3].ToString();
                                            string PROMOTION_FLAG = rows[4].ToString();
                                            string PROMOTION_RETAIL = rows[5].ToString();
                                            string MEMBER_RETAIL = rows[6].ToString();
                                            string MEMBER_PROMOTION_FLAG = rows[7].ToString();
                                            string MEMBER_PROMOTION_RETAIL = rows[8].ToString();
                                            string FILE_ID = log_fileid;

                                            sql_insert_data_ITEMSELLPRICE += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""),"
                                                                                        , STORE, SKU, DESCRIPTION, CURRENT_PRICE, PROMOTION_FLAG, PROMOTION_RETAIL, MEMBER_RETAIL
                                                                                        , MEMBER_PROMOTION_FLAG, MEMBER_PROMOTION_RETAIL, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_ITEMSELLPRICE = sql_insert_data_ITEMSELLPRICE.Substring(0, sql_insert_data_ITEMSELLPRICE.Length - 1);
                                                var cmd_insert_data_ITEMSELLPRICE = new MySqlCommand(sql_insert_data_ITEMSELLPRICE, connection);
                                                MySqlDataReader rdr_insert_data_ITEMSELLPRICE = cmd_insert_data_ITEMSELLPRICE.ExecuteReader();
                                                connection.Close();

                                                sql_insert_data_ITEMSELLPRICE = String.Format(@"INSERT INTO `aeon_pop`.`item_sell_price_temp`(`STORE`,`SKU`
                                                                                            ,`DESCRIPTION`,`CURRENT_PRICE`,`PROMOTION_FLAG`,`PROMOTION_RETAIL`,`MEMBER_RETAIL`
                                                                                            ,`MEMBER_PROMOTION_FLAG`,`MEMBER_PROMOTION_RETAIL`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_ITEMSELLPRICE = sql_insert_data_ITEMSELLPRICE.Substring(0, sql_insert_data_ITEMSELLPRICE.Length - 1);
                                            var cmd_insert_data_ITEMSELLPRICE = new MySqlCommand(sql_insert_data_ITEMSELLPRICE, connection);
                                            MySqlDataReader rdr_insert_data_ITEMSELLPRICE = cmd_insert_data_ITEMSELLPRICE.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region ITEMSELLPRICE old
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu
                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_itemsellprice = String.Format(@"SELECT * 
                                                                                                                        FROM
                                                                                                                        (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(STORE, SKU) ORDER BY FILE_ID DESC) AS row_num
                                                                                                                        FROM AEON_POP.item_sell_price) T0
                                                                                                                        WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_itemsellprice = new MySqlCommand(sql_get_cur_itemsellprice, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_itemsellprice = new MySqlDataAdapter();
                                                                        MyAdapter_cur_itemsellprice.SelectCommand = cmd_get_cur_itemsellprice;
                                                                        DataTable dTable_ItemSellPrice_Cur = new DataTable();
                                                                        MyAdapter_cur_itemsellprice.Fill(dTable_ItemSellPrice_Cur);
                                                                        connection.Close();


                                                                        //get dữ liệu mới
                                                                        DataTable dTable_ItemSellPrice_New = ConvertCSVtoDataTable_ItemSellPrice(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_ItemSellPrice_New.AsEnumerable()
                                                                                           join table2 in dTable_ItemSellPrice_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["STORE"] == null ? String.Empty : table1["STORE"].ToString(),
                                                                                               con2 = table1["SKU"] == null ? String.Empty : table1["SKU"].ToString()
                                                                                           }
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["STORE"] == null ? String.Empty : table2["STORE"].ToString(),
                                                                                               con2 = table2["SKU"] == null ? String.Empty : table2["SKU"].ToString()
                                                                                           }
                                                                                           into _Table3 from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["CURRENT_PRICE"].ToString()) != table1["CURRENT_PRICE"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMOTION_FLAG"].ToString()) != table1["PROMOTION_FLAG"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PROMOTION_RETAIL"].ToString()) != table1["PROMOTION_RETAIL"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MEMBER_RETAIL"].ToString()) != table1["MEMBER_RETAIL"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MEMBER_PROMOTION_FLAG"].ToString()) != table1["MEMBER_PROMOTION_FLAG"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["MEMBER_PROMOTION_RETAIL"].ToString()) != table1["MEMBER_PROMOTION_RETAIL"].ToString())
                                                                                                    )
                                                                                           select new 
                                                                                           {
                                                                                               STORE = table1 == null || table1["STORE"] == null ? string.Empty : table1["STORE"].ToString(),
                                                                                               SKU = table1 == null || table1["SKU"] == null ? string.Empty : table1["SKU"].ToString(),
                                                                                               DESCRIPTION = table1 == null || table1["DESCRIPTION"] == null ? string.Empty : table1["DESCRIPTION"].ToString(),
                                                                                               CURRENT_PRICE = table1 == null || table1["CURRENT_PRICE"] == null ? string.Empty : table1["CURRENT_PRICE"].ToString(),
                                                                                               PROMOTION_FLAG = table1 == null || table1["PROMOTION_FLAG"] == null ? string.Empty : table1["PROMOTION_FLAG"].ToString(),
                                                                                               PROMOTION_RETAIL = table1 == null || table1["PROMOTION_RETAIL"] == null ? string.Empty : table1["PROMOTION_RETAIL"].ToString(),
                                                                                               MEMBER_RETAIL = table1 == null || table1["MEMBER_RETAIL"] == null ? string.Empty : table1["MEMBER_RETAIL"].ToString(),
                                                                                               MEMBER_PROMOTION_FLAG = table1 == null || table1["MEMBER_PROMOTION_FLAG"] == null ? string.Empty : table1["MEMBER_PROMOTION_FLAG"].ToString(),
                                                                                               MEMBER_PROMOTION_RETAIL = table1 == null || table1["MEMBER_PROMOTION_RETAIL"] == null ? string.Empty : table1["MEMBER_PROMOTION_RETAIL"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table ITEMSELLPRICE
                                                                        var sql_insert_data_ITEMSELLPRICE = String.Format(@"INSERT INTO `aeon_pop`.`item_sell_price`(`STORE`,`SKU`
                                                                                                                                ,`DESCRIPTION`,`CURRENT_PRICE`,`PROMOTION_FLAG`,`PROMOTION_RETAIL`,`MEMBER_RETAIL`
                                                                                                                                ,`MEMBER_PROMOTION_FLAG`,`MEMBER_PROMOTION_RETAIL`,`FILE_ID`)VALUES");

                                                                        int line = 0;
                                                                        foreach (var result in result_table)
                                                                        {
                                                                            line++;
                                                                            //get data
                                                                            string STORE = result.STORE;
                                                                            string SKU = result.SKU;
                                                                            string DESCRIPTION = "";
                                                                            string temp_desc = result.DESCRIPTION;
                                                                            if (temp_desc.Contains("\""))
                                                                            {
                                                                                DESCRIPTION = temp_desc.Replace("\"", "\"\"");
                                                                            }
                                                                            else
                                                                            {
                                                                                DESCRIPTION = result.DESCRIPTION;
                                                                            }

                                                                            string CURRENT_PRICE = result.CURRENT_PRICE;
                                                                            string PROMOTION_FLAG = result.PROMOTION_FLAG;
                                                                            string PROMOTION_RETAIL = result.PROMOTION_RETAIL;
                                                                            string MEMBER_RETAIL = result.MEMBER_RETAIL;
                                                                            string MEMBER_PROMOTION_FLAG = result.MEMBER_PROMOTION_FLAG;
                                                                            string MEMBER_PROMOTION_RETAIL = result.MEMBER_PROMOTION_RETAIL;
                                                                            string FILE_ID = log_fileid;

                                                                            sql_insert_data_ITEMSELLPRICE += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""),"
                                                                                                                        , STORE, SKU, DESCRIPTION, CURRENT_PRICE, PROMOTION_FLAG, PROMOTION_RETAIL, MEMBER_RETAIL
                                                                                                                        , MEMBER_PROMOTION_FLAG, MEMBER_PROMOTION_RETAIL, FILE_ID);

                                                                            if (line == 1000)
                                                                            {
                                                                                connection.Open();
                                                                                sql_insert_data_ITEMSELLPRICE = sql_insert_data_ITEMSELLPRICE.Substring(0, sql_insert_data_ITEMSELLPRICE.Length - 1);
                                                                                var cmd_insert_data_ITEMSELLPRICE = new MySqlCommand(sql_insert_data_ITEMSELLPRICE, connection);
                                                                                MySqlDataReader rdr_insert_data_ITEMSELLPRICE = cmd_insert_data_ITEMSELLPRICE.ExecuteReader();
                                                                                connection.Close();

                                                                                sql_insert_data_ITEMSELLPRICE = String.Format(@"INSERT INTO `aeon_pop`.`item_sell_price`(`STORE`,`SKU`
                                                                                                                                ,`DESCRIPTION`,`CURRENT_PRICE`,`PROMOTION_FLAG`,`PROMOTION_RETAIL`,`MEMBER_RETAIL`
                                                                                                                                ,`MEMBER_PROMOTION_FLAG`,`MEMBER_PROMOTION_RETAIL`,`FILE_ID`)VALUES");
                                                                                line = 0;
                                                                            }
                                                                        }

                                                                        if (result_table.Count() > 0 && line != 0)
                                                                        {
                                                                            connection.Open();
                                                                            sql_insert_data_ITEMSELLPRICE = sql_insert_data_ITEMSELLPRICE.Substring(0, sql_insert_data_ITEMSELLPRICE.Length - 1);
                                                                            var cmd_insert_data_ITEMSELLPRICE = new MySqlCommand(sql_insert_data_ITEMSELLPRICE, connection);
                                                                            MySqlDataReader rdr_insert_data_ITEMSELLPRICE = cmd_insert_data_ITEMSELLPRICE.ExecuteReader();
                                                                            connection.Close();
                                                                        }    

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                                                        */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 12)
                            {
                                if (filename.Substring(0, 12) == "ITEMBARCODE_")
                                {
                                    #region ITEMBARCODE_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_BarCode = String.Format(@"INSERT INTO `aeon_pop`.`barcode`(`BUSINESS_UNIT`,`SKU`
                                                                                            ,`BARCODE`,`IN_HOUSE_FLAG`,`PRIMARY_FLAG`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Split(',');
                                            line++;

                                            //get data
                                            string BUSINESS_UNIT = rows[0].ToString();
                                            string SKU = rows[1].ToString();
                                            string BARCODE = rows[2].ToString();
                                            string IN_HOUSE_FLAG = rows[3].ToString();
                                            string PRIMARY_FLAG = rows[4].ToString();
                                            string FILE_ID = log_fileid;

                                            sql_insert_data_BarCode += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}""),"
                                                                                        , BUSINESS_UNIT, SKU, BARCODE, IN_HOUSE_FLAG, PRIMARY_FLAG, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_BarCode = sql_insert_data_BarCode.Substring(0, sql_insert_data_BarCode.Length - 1);
                                                var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_BarCode, connection);
                                                MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                                connection.Close();

                                                sql_insert_data_BarCode = String.Format(@"INSERT INTO `aeon_pop`.`barcode`(`BUSINESS_UNIT`,`SKU`
                                                                                            ,`BARCODE`,`IN_HOUSE_FLAG`,`PRIMARY_FLAG`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_BarCode = sql_insert_data_BarCode.Substring(0, sql_insert_data_BarCode.Length - 1);
                                            var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_BarCode, connection);
                                            MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region ITEMBARCODE
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu
                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_barcode = String.Format(@"SELECT * 
                                                                                                                        FROM
                                                                                                                        (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(BUSINESS_UNIT, SKU, BARCODE) ORDER BY FILE_ID DESC) AS row_num
                                                                                                                        FROM aeon_pop.barcode) T0
                                                                                                                        WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_barcode = new MySqlCommand(sql_get_cur_barcode, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_barcode = new MySqlDataAdapter();
                                                                        MyAdapter_cur_barcode.SelectCommand = cmd_get_cur_barcode;
                                                                        DataTable dTable_BarCode_Cur = new DataTable();
                                                                        MyAdapter_cur_barcode.Fill(dTable_BarCode_Cur);
                                                                        connection.Close();


                                                                        //get dữ liệu mới
                                                                        DataTable dTable_BarCode_New = ConvertCSVtoDataTable_BarCode(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_BarCode_New.AsEnumerable()
                                                                                           join table2 in dTable_BarCode_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["BUSINESS_UNIT"] == null ? String.Empty : table1["BUSINESS_UNIT"].ToString(),
                                                                                               con2 = table1["SKU"] == null ? String.Empty : table1["SKU"].ToString(),
                                                                                               con3 = table1["BARCODE"] == null ? String.Empty : table1["BARCODE"].ToString()
                                                                                           }
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["BUSINESS_UNIT"] == null ? String.Empty : table2["BUSINESS_UNIT"].ToString(),
                                                                                               con2 = table2["SKU"] == null ? String.Empty : table2["SKU"].ToString(),
                                                                                               con3 = table2["BARCODE"] == null ? String.Empty : table2["BARCODE"].ToString()
                                                                                           }
                                                                                           into _Table3
                                                                                           from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["IN_HOUSE_FLAG"].ToString()) != table1["IN_HOUSE_FLAG"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["PRIMARY_FLAG"].ToString()) != table1["PRIMARY_FLAG"].ToString())
                                                                                                    )
                                                                                           select new
                                                                                           {
                                                                                               BUSINESS_UNIT = table1 == null || table1["BUSINESS_UNIT"] == null ? string.Empty : table1["BUSINESS_UNIT"].ToString(),
                                                                                               SKU = table1 == null || table1["SKU"] == null ? string.Empty : table1["SKU"].ToString(),
                                                                                               BARCODE = table1 == null || table1["BARCODE"] == null ? string.Empty : table1["BARCODE"].ToString(),
                                                                                               IN_HOUSE_FLAG = table1 == null || table1["IN_HOUSE_FLAG"] == null ? string.Empty : table1["IN_HOUSE_FLAG"].ToString(),
                                                                                               PRIMARY_FLAG = table1 == null || table1["PRIMARY_FLAG"] == null ? string.Empty : table1["PRIMARY_FLAG"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table
                                                                        var sql_insert_data_BarCode = String.Format(@"INSERT INTO `aeon_pop`.`barcode`(`BUSINESS_UNIT`,`SKU`
                                                                                                                                ,`BARCODE`,`IN_HOUSE_FLAG`,`PRIMARY_FLAG`,`FILE_ID`)VALUES");
                                                                        foreach (var result in result_table)
                                                                        {
                                                                            //get data
                                                                            string BUSINESS_UNIT = result.BUSINESS_UNIT;
                                                                            string SKU = result.SKU;
                                                                            string BARCODE = result.BARCODE;
                                                                            string IN_HOUSE_FLAG = result.IN_HOUSE_FLAG;
                                                                            string PRIMARY_FLAG = result.PRIMARY_FLAG;
                                                                            string FILE_ID = log_fileid;

                                                                            sql_insert_data_BarCode += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}""),"
                                                                                                                        , BUSINESS_UNIT, SKU, BARCODE, IN_HOUSE_FLAG, PRIMARY_FLAG, FILE_ID);
                                                                        }

                                                                        if (result_table.Count() > 0)
                                                                        {
                                                                            connection.Open();
                                                                            sql_insert_data_BarCode = sql_insert_data_BarCode.Substring(0, sql_insert_data_BarCode.Length - 1);
                                                                            var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_BarCode, connection);
                                                                            MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                                                            connection.Close();
                                                                        }

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                                                        */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 10)
                            {
                                if (filename.Substring(0, 10) == "ITEMSUPPL_")//ITEMSUPPL_ 
                                {
                                    #region ITEMSUPPL_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_item_supplier_contract = String.Format(@"INSERT INTO `aeon_pop`.`item_supplier_contract_temp`(`SKU`,`SUPPLIER`,`CONTRACT_NO`,`STORE`,`DEFAULT_STORE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Split(',');
                                            line++;

                                            //get data
                                            string SKU = rows[0].ToString();
                                            string SUPPLIER = rows[1].ToString();
                                            string CONTRACT_NO = rows[2].ToString();
                                            string STORE = rows[3].ToString();
                                            string DEFAULT_STORE = rows[4].ToString();
                                            string FILE_ID = log_fileid;

                                            sql_insert_data_item_supplier_contract += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}""),"
                                                                                        , SKU, SUPPLIER, CONTRACT_NO, STORE, DEFAULT_STORE, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_item_supplier_contract = sql_insert_data_item_supplier_contract.Substring(0, sql_insert_data_item_supplier_contract.Length - 1);
                                                var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_item_supplier_contract, connection);
                                                MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                                connection.Close();

                                                sql_insert_data_item_supplier_contract = String.Format(@"INSERT INTO `aeon_pop`.`item_supplier_contract_temp`(`SKU`,`SUPPLIER`,`CONTRACT_NO`,`STORE`,`DEFAULT_STORE`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_item_supplier_contract = sql_insert_data_item_supplier_contract.Substring(0, sql_insert_data_item_supplier_contract.Length - 1);
                                            var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_item_supplier_contract, connection);
                                            MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion

                                    #region ITEM_SUPPLIER_CONTRACT
                                    /*
                                                                        var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                                                         , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                                                         , filename
                                                                                         , date_now
                                                                                         , time_now
                                                                                         , "Inprocess");
                                                                        connection.Open();
                                                                        var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                                                        MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                                                        connection.Close();

                                                                        //get File_ID
                                                                        var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                                                        connection.Open();
                                                                        var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                                                        MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                                                        MyAdapter.SelectCommand = cmd_get_fileID;
                                                                        DataTable dTable_FileID = new DataTable();
                                                                        MyAdapter.Fill(dTable_FileID);
                                                                        connection.Close();

                                                                        log_fileid = dTable_FileID.Rows[0][0].ToString();

                                                                        #region xử lý dữ liệu
                                                                        //get dữ liệu hiện tại
                                                                        var sql_get_cur_contract = String.Format(@"SELECT * 
                                                                                                                        FROM
                                                                                                                        (SELECT *, ROW_NUMBER() OVER(PARTITION BY CONCAT(SKU) ORDER BY FILE_ID DESC) AS row_num
                                                                                                                        FROM aeon_pop.item_supplier_contract) T0
                                                                                                                        WHERE T0.row_num = ""1"";");
                                                                        connection.Open();
                                                                        var cmd_get_cur_contract = new MySqlCommand(sql_get_cur_contract, connection);
                                                                        MySqlDataAdapter MyAdapter_cur_contract = new MySqlDataAdapter();
                                                                        MyAdapter_cur_contract.SelectCommand = cmd_get_cur_contract;
                                                                        DataTable dTable_Contract_Cur = new DataTable();
                                                                        MyAdapter_cur_contract.Fill(dTable_Contract_Cur);
                                                                        connection.Close();


                                                                        //get dữ liệu mới
                                                                        DataTable dTable_Contract_New = ConvertCSVtoDataTable_Contract(pathtg);

                                                                        //linq xử lý, lọc dữ liệu cần lấy
                                                                        var result_table = from table1 in dTable_Contract_New.AsEnumerable()
                                                                                           join table2 in dTable_Contract_Cur.AsEnumerable()
                                                                                           on new
                                                                                           {
                                                                                               con1 = table1["SKU"] == null ? String.Empty : table1["SKU"].ToString(),
                                                                                               con2 = table1["STORE"] == null ? String.Empty : table1["STORE"].ToString(),

                                                                                           }   
                                                                                           equals new
                                                                                           {
                                                                                               con1 = table2["SKU"] == null ? String.Empty : table2["SKU"].ToString(),
                                                                                               con2 = table2["STORE"] == null ? String.Empty : table2["STORE"].ToString(),
                                                                                           }
                                                                                           into _Table3
                                                                                           from table3 in _Table3.DefaultIfEmpty()
                                                                                           where (((table3 == null || table3[0] == null ? String.Empty : table3[0].ToString()) == "")
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["SUPPLIER"].ToString()) != table1["SUPPLIER"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["CONTRACT_NO"].ToString()) != table1["CONTRACT_NO"].ToString())
                                                                                                    || ((table3 == null || table3[0] == null ? String.Empty : table3["DEFAULT_STORE"].ToString()) != table1["DEFAULT_STORE"].ToString())
                                                                                                    )
                                                                                           select new
                                                                                           {
                                                                                               SKU = table1 == null || table1["SKU"] == null ? string.Empty : table1["SKU"].ToString(),
                                                                                               SUPPLIER = table1 == null || table1["SUPPLIER"] == null ? string.Empty : table1["SUPPLIER"].ToString(),
                                                                                               CONTRACT_NO = table1 == null || table1["CONTRACT_NO"] == null ? string.Empty : table1["CONTRACT_NO"].ToString(),
                                                                                               STORE = table1 == null || table1["STORE"] == null ? string.Empty : table1["STORE"].ToString(),
                                                                                               DEFAULT_STORE = table1 == null || table1["DEFAULT_STORE"] == null ? string.Empty : table1["DEFAULT_STORE"].ToString(),
                                                                                           };
                                                                        #endregion

                                                                        //insert data to table
                                                                        var sql_insert_data_item_supplier_contract = String.Format(@"INSERT INTO `aeon_pop`.`item_supplier_contract`(`SKU`,`SUPPLIER`,`CONTRACT_NO`,`STORE`,`DEFAULT_STORE`,`FILE_ID`)VALUES");
                                                                        foreach (var result in result_table)
                                                                        {
                                                                            //get data
                                                                            string SKU = result.SKU;
                                                                            string SUPPLIER = result.SUPPLIER;
                                                                            string CONTRACT_NO = result.CONTRACT_NO;
                                                                            string STORE = result.STORE;
                                                                            string DEFAULT_STORE = result.DEFAULT_STORE;
                                                                            string FILE_ID = log_fileid;

                                                                            sql_insert_data_item_supplier_contract += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}""),"
                                                                                                                        , SKU, SUPPLIER, CONTRACT_NO, STORE, DEFAULT_STORE, FILE_ID);
                                                                        }

                                                                        if (result_table.Count() > 0)
                                                                        {
                                                                            connection.Open();
                                                                            sql_insert_data_item_supplier_contract = sql_insert_data_item_supplier_contract.Substring(0, sql_insert_data_item_supplier_contract.Length - 1);
                                                                            var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_item_supplier_contract, connection);
                                                                            MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                                                            connection.Close();
                                                                        }

                                                                        //move file to folder backup
                                                                        String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                                                        bool exist = Directory.Exists(dirBackup);
                                                                        if (!exist)
                                                                        {
                                                                            // Tạo thư mục.
                                                                            Directory.CreateDirectory(dirBackup);
                                                                        }
                                                                        string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                                                        File.Move(pathtg, dirPathBackup);


                                                                        //update info file to log_file
                                                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                                                             , log_fileid);
                                                                        connection.Open();
                                                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                                                        connection.Close();
                                                                        */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 9)
                            {
                                if (filename.Substring(0, 9) == "SUPPLIER_")
                                {
                                    #region SUPPLIER_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
                                                     , filename.Substring(filename.LastIndexOf("_") + 1, filename.Length - filename.LastIndexOf("_") - 8)
                                                     , filename
                                                     , date_now
                                                     , time_now
                                                     , "Inprocess");
                                    connection.Open();
                                    var cmd_insert_profit_file = new MySqlCommand(sql_insert_profit_file, connection);
                                    MySqlDataReader rdr_insert_profit_file = cmd_insert_profit_file.ExecuteReader();
                                    connection.Close();

                                    //get File_ID
                                    var sql_get_fileID = String.Format("select * from profit_files_log order by FILE_ID desc limit 1");
                                    connection.Open();
                                    var cmd_get_fileID = new MySqlCommand(sql_get_fileID, connection);
                                    MySqlDataAdapter MyAdapter = new MySqlDataAdapter();
                                    MyAdapter.SelectCommand = cmd_get_fileID;
                                    DataTable dTable_FileID = new DataTable();
                                    MyAdapter.Fill(dTable_FileID);
                                    connection.Close();

                                    log_fileid = dTable_FileID.Rows[0][0].ToString();
                                    using (StreamReader sr = new StreamReader(pathtg))
                                    {
                                        int line = 0;
                                        //insert data to table
                                        var sql_insert_data_supplier = String.Format(@"INSERT INTO `aeon_pop`.`supplier_temp`(`SUPPLIER_CODE`,`SUPPLIER_NAME_END`,`SUPPLIER_NAME_VNM`,`SUPPLIER_SHORTNAME_END`,`SUPPLIER_SHORTNAME_VNM`,`SUPPLIER_TYPE`,`DELETE_FLAG`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Split(',');
                                            line++;

                                            //get data
                                            string SUPPLIER_CODE = rows[0].ToString();
                                            string SUPPLIER_NAME_END = rows[2].ToString();
                                            string SUPPLIER_NAME_VNM = rows[3].ToString();
                                            string SUPPLIER_SHORTNAME_END = rows[4].ToString();
                                            string SUPPLIER_SHORTNAME_VNM = rows[5].ToString();
                                            string SUPPLIER_TYPE = rows[7].ToString();
                                            string DELETE_FLAG = rows[11].ToString();
                                            string FILE_ID = log_fileid;

                                            sql_insert_data_supplier += string.Format(@"(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}""),"
                                                                                        , SUPPLIER_CODE, SUPPLIER_NAME_END, SUPPLIER_NAME_VNM, SUPPLIER_SHORTNAME_END, SUPPLIER_SHORTNAME_VNM, SUPPLIER_TYPE, DELETE_FLAG, FILE_ID);

                                            if (line == 1000)
                                            {
                                                connection.Open();
                                                sql_insert_data_supplier = sql_insert_data_supplier.Substring(0, sql_insert_data_supplier.Length - 1);
                                                var cmd_insert_data = new MySqlCommand(sql_insert_data_supplier, connection);
                                                MySqlDataReader rdr_insert_data = cmd_insert_data.ExecuteReader();
                                                connection.Close();

                                                sql_insert_data_supplier = String.Format(@"INSERT INTO `aeon_pop`.`supplier_temp`(`SUPPLIER_CODE`,`SUPPLIER_NAME_END`,`SUPPLIER_NAME_VNM`,`SUPPLIER_SHORTNAME_END`,`SUPPLIER_SHORTNAME_VNM`,`SUPPLIER_TYPE`,`DELETE_FLAG`,`FILE_ID`)VALUES");
                                                line = 0;
                                            }
                                        }
                                        if (line > 0)
                                        {
                                            connection.Open();
                                            sql_insert_data_supplier = sql_insert_data_supplier.Substring(0, sql_insert_data_supplier.Length - 1);
                                            var cmd_insert_data = new MySqlCommand(sql_insert_data_supplier, connection);
                                            MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data.ExecuteReader();
                                            connection.Close();
                                        }
                                    }
                                    //move file to folder backup
                                    String dirBackup = @"C:\Profit_Receive\Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                        }
                    }
                }

                MessageBox.Show("SS");
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message + lineeeeee);
                MySqlConnection connection = new MySqlConnection(connectionString);
                //update info file to log_file
                //var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"{0}\" WHERE `FILE_ID` = '{1}';"
                //                                            , ex.Message
                //                                            , log_fileid);
                //connection.Open();
                //var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                //MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                //connection.Close();

                connection.Open();
                MySqlCommand comm_sql_update_profit_file = connection.CreateCommand();
                var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"{0}\" WHERE `FILE_ID` = '{1}';"
                                                            , ex.Message
                                                            , log_fileid);
                comm_sql_update_profit_file.CommandText = sql_update_profit_file;
                comm_sql_update_profit_file.ExecuteNonQuery();
                connection.Close();
                MessageBox.Show(ex.Message);
            }
        }

        public static DataTable ConvertCSVtoDataTable_ItemSellPrice(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("STORE");
                dt.Columns.Add("SKU");
                dt.Columns.Add("DESCRIPTION");
                dt.Columns.Add("CURRENT_PRICE");
                dt.Columns.Add("PROMOTION_FLAG");
                dt.Columns.Add("PROMOTION_RETAIL");
                dt.Columns.Add("MEMBER_RETAIL");
                dt.Columns.Add("MEMBER_PROMOTION_FLAG");
                dt.Columns.Add("MEMBER_PROMOTION_RETAIL");
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i <= 8; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            return dt;
        }

        public static DataTable ConvertCSVtoDataTable_Hamper(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("PACK_SKU");
                dt.Columns.Add("STORE");
                dt.Columns.Add("DESCRIPTION");
                dt.Columns.Add("PACK_TYPE");
                dt.Columns.Add("STATUS");
                dt.Columns.Add("SKU");
                dt.Columns.Add("QTY_PER_SKU");
                dt.Columns.Add("QTY_UOM");
                dt.Columns.Add("DECORATION_FLAG");
                dt.Columns.Add("MODIFIED_DATE");
                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i <= 9; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            return dt;
        }
        public static DataTable ConvertCSVtoDataTable_ItemPriceChange(string strFilePath)
        {
            DataSet odataSet = new DataSet();
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("PRICE_CHANGE_NO");
                dt.Columns.Add("CREATED_DATE");
                dt.Columns.Add("TRANS_TYPE");
                dt.Columns.Add("REASON");
                dt.Columns.Add("CREATED_BY");//
                dt.Columns.Add("DEPARTMENT");
                dt.Columns.Add("START_DATE");
                dt.Columns.Add("END_DATE");
                dt.Columns.Add("DAILY_START_TIME");
                dt.Columns.Add("DAILY_END_TIME");
                dt.Columns.Add("REFERENCE");//
                dt.Columns.Add("STATUS");
                dt.Columns.Add("PRICE_CHANGE_TYPE");
                dt.Columns.Add("EVENT_ID");
                dt.Columns.Add("MEMBER_DISC_CODE");//
                dt.Columns.Add("PROMOTION_TYPE");
                dt.Columns.Add("STORE");
                dt.Columns.Add("SKU");
                dt.Columns.Add("LAST_SELL_PRICE");
                dt.Columns.Add("LAST_SELL_UNIT");
                dt.Columns.Add("NEW_SELL_PRICE");
                dt.Columns.Add("PROMOTION_MARGIN");//
                dt.Columns.Add("SUPPLIER_BEARING");//
                dt.Columns.Add("EXPORT_DATE");//
                dt.Columns.Add("PRICE_CHANGE_TYPE_VALUE");
                dt.Columns.Add("MODIFIED_DATE");


                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    string store = rows[16].ToString();
                    if (odataSet.Tables.Contains(store))
                    {
                        DataRow dr = dt.NewRow();
                        for (int i = 0; i <= 25; i++)
                        {
                            dr[i] = rows[i];
                        }
                        //dt.Rows.Add(dr);
                        odataSet.Tables[store].Rows.Add(dr);
                    } 
                    else
                    {
                        dt.TableName = store;
                        odataSet.Tables.Add(dt);
                        DataRow dr = dt.NewRow();
                        for (int i = 0; i <= 25; i++)
                        {
                            dr[i] = rows[i];
                        }
                        //dt.Rows.Add(dr);
                        odataSet.Tables[store].Rows.Add(dr);
                    }    

                    //DataRow dr = dt.NewRow();
                    //int temp = 0;
                    //for (int i = 0; i <= 25; i++)
                    //{
                    //    //if (i != 4 && i != 10 && i != 14 && i != 21 && i != 22 && i != 23)
                    //    //{
                    //    //    dr[temp] = rows[i];
                    //    //    temp++;
                    //    //}
                    //    dr[i] = rows[i];
                    //}
                    //temp = 0;
                    //dt.Rows.Add(dr);
                }

            }
            return dt;
        }

        public static DataTable ConvertCSVtoDataTable_MixMatch(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("PROMO_NO");
                dt.Columns.Add("PROMO_TYPE");
                dt.Columns.Add("PROMO_DESC");
                dt.Columns.Add("START_DATE");
                dt.Columns.Add("END_DATE");
                dt.Columns.Add("START_TIME");
                dt.Columns.Add("END_TIME");
                dt.Columns.Add("STATUS");
                dt.Columns.Add("CREATE_BY");
                dt.Columns.Add("CONFIRM_BY");
                dt.Columns.Add("AUTH_BY");
                dt.Columns.Add("MAX_OR_PARTIAL");
                dt.Columns.Add("CANCEL_DATE");
                dt.Columns.Add("CANCEL_BY");
                dt.Columns.Add("EVENT_ID");
                dt.Columns.Add("STORE");
                dt.Columns.Add("SKU");
                dt.Columns.Add("SEQ");
                dt.Columns.Add("NORMAL_PRICE");
                dt.Columns.Add("SELL_UOM");
                dt.Columns.Add("PROMO_QTY");
                dt.Columns.Add("PROMO_PRICE");
                dt.Columns.Add("FOC_QTY");
                dt.Columns.Add("EXPORT_DATE");
                dt.Columns.Add("TTL_PROMO_QTY");
                dt.Columns.Add("TTL_PROMO_PRICE");
                dt.Columns.Add("PLU_COUNT");
                dt.Columns.Add("FOC_SKU");
                dt.Columns.Add("MODIFIED_DATE");

                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i <= 28; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            return dt;
        }

        public static DataTable ConvertCSVtoDataTable_GroupPriceChange(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("PRICE_CHANGE_NO");
                dt.Columns.Add("CREATED_DATE");
                dt.Columns.Add("TRANS_TYPE");
                dt.Columns.Add("REASON");
                dt.Columns.Add("CREATED_BY");
                dt.Columns.Add("START_DATE");
                dt.Columns.Add("END_DATE");
                dt.Columns.Add("START_TIME");
                dt.Columns.Add("END_TIME");
                dt.Columns.Add("EVENT_ID");
                dt.Columns.Add("EXCLUDE_SEASON_ID");
                dt.Columns.Add("STATUS");
                dt.Columns.Add("PRICE_CHANGE_TYPE");
                dt.Columns.Add("PRICE_CHANGE_TYPE_VALUE");
                dt.Columns.Add("PROMOTION_TYPE");
                dt.Columns.Add("STORE");
                dt.Columns.Add("CATEGORY");
                dt.Columns.Add("EXPORT_DATE");
                dt.Columns.Add("MODIFIED_DATE");

                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i <= 18; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            return dt;
        }

        public static DataTable ConvertCSVtoDataTable_SKU(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("SKU");
                dt.Columns.Add("DATE_CREATE");
                dt.Columns.Add("CREATED_BY");
                dt.Columns.Add("ITEM_DESC_ENG");
                dt.Columns.Add("ITEM_DESC_VNM");
                dt.Columns.Add("PLU_DESC_ENG");
                dt.Columns.Add("PLU_DESC_VNM");
                dt.Columns.Add("FOC_DESC_ENG");
                dt.Columns.Add("FOC_DESC_VNM");
                dt.Columns.Add("TICKET1_DESC_ENG");
                dt.Columns.Add("TICKET1_DESC_VNM");
                dt.Columns.Add("TICKET2_DESC_ENG");
                dt.Columns.Add("TICKET2_DESC_VNM");
                dt.Columns.Add("POP1_DESC_ENG");
                dt.Columns.Add("POP1_DESC_VNM");
                dt.Columns.Add("POP2_DESC_ENG");
                dt.Columns.Add("POP2_DESC_VNM");
                dt.Columns.Add("DEPT_ID");
                dt.Columns.Add("SUPPLIER_ID");
                dt.Columns.Add("CATEGORY_ID");
                dt.Columns.Add("SUPPLIER_CONTRACT");
                dt.Columns.Add("BRAND");
                dt.Columns.Add("DIVISION_ID");
                dt.Columns.Add("LINE_ID");
                dt.Columns.Add("GROUP_ID");
                dt.Columns.Add("STYLE");
                dt.Columns.Add("COLOUR_SIZE_GRID");
                dt.Columns.Add("COLOUR");
                dt.Columns.Add("SIZE_ID");
                dt.Columns.Add("PURCHASE_METHOD");
                dt.Columns.Add("ITEM_SOURCE");
                dt.Columns.Add("RETURNABLE");
                dt.Columns.Add("KADS1M_FLAG");
                dt.Columns.Add("ITEM_TYPE");
                dt.Columns.Add("INGREDIENT_TYPE");
                dt.Columns.Add("MERCHANDISE_PLAN");
                dt.Columns.Add("SEASON_ID");
                dt.Columns.Add("PACK_ITEM");
                dt.Columns.Add("PERISH_ITEM");
                dt.Columns.Add("NON_INVENTORY");
                dt.Columns.Add("NON_INVENTORY_CODE");
                dt.Columns.Add("NON_PLU");
                dt.Columns.Add("MOMMY_ITEM");
                dt.Columns.Add("FOOD_ITEM");
                dt.Columns.Add("MEMBER_DISC_ITEM");
                dt.Columns.Add("SUPER_SAVER_ITEM");
                dt.Columns.Add("ADD_AUTO_DISC_ITEM");
                dt.Columns.Add("AUTO_REPLENISH_ITEM");
                dt.Columns.Add("DAISO_DOC_SKU");
                dt.Columns.Add("ACTIVED");
                dt.Columns.Add("DATE_ACTIVED");
                dt.Columns.Add("HOLD_ORDER");
                dt.Columns.Add("HOLD_ORDER_START_DATE");
                dt.Columns.Add("HOLD_ORDER_END_DATE");
                dt.Columns.Add("DISCONTINUE");
                dt.Columns.Add("DATE_DISCONTINUE");
                dt.Columns.Add("DELETED");
                dt.Columns.Add("DATE_DELETED");
                dt.Columns.Add("SUB_CATEGORY");
                dt.Columns.Add("RETAIL_VAT_CODE");
                dt.Columns.Add("RETAIL_VAT_RATE");
                dt.Columns.Add("SUG_UNIT_RETAIL_WVAT");
                dt.Columns.Add("RETAIL_UOM");
                dt.Columns.Add("SUG_UNIT_RETAIL_WOVAT");
                dt.Columns.Add("SALES_TAX_RATE");
                dt.Columns.Add("COST_VAT_RATE");
                dt.Columns.Add("STD_COST_UOM");
                dt.Columns.Add("ORDER_UOM");
                dt.Columns.Add("PARENT_SKU");
                dt.Columns.Add("TICKET_SKU");
                dt.Columns.Add("TICKET_TYPE");
                dt.Columns.Add("AUTO_ORDER_START_DATE");
                dt.Columns.Add("AUTO_ORDER_END_DATE");
                dt.Columns.Add("HS_CODE");
                dt.Columns.Add("MSDS_CODE");
                dt.Columns.Add("NET_WEIGHT_KG");
                dt.Columns.Add("GROSS_WEIGHT_KG");
                dt.Columns.Add("CUBIC_METER_M3");
                dt.Columns.Add("NEIRE_PERC");
                dt.Columns.Add("WASTAGE");
                dt.Columns.Add("FOC_ITEM");
                dt.Columns.Add("PROMO_ITEM");
                dt.Columns.Add("STCK_POINT_ITEM");
                dt.Columns.Add("MODIFIED_DATE");
                dt.Columns.Add("POP3_DESC_VNM");
                dt.Columns.Add("SELLING_POINT1");
                dt.Columns.Add("SELLING_POINT2");
                dt.Columns.Add("SELLING_POINT3");
                dt.Columns.Add("SELLING_POINT4");
                dt.Columns.Add("SELLING_POINT5");

                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    for (int i = 0; i <= 89; i++)
                    {
                        dr[i] = rows[i];
                    }
                    dt.Rows.Add(dr);
                }

            }
            return dt;
        }

        public static DataTable ConvertCSVtoDataTable_BarCode(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("BUSINESS_UNIT");
                dt.Columns.Add("SKU");
                dt.Columns.Add("BARCODE");
                dt.Columns.Add("IN_HOUSE_FLAG");
                dt.Columns.Add("PRIMARY_FLAG");

                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    if (rows[0].ToString() != "")
                    {
                        for (int i = 0; i <= 4; i++)
                        {
                            dr[i] = rows[i];
                        }

                        dt.Rows.Add(dr);
                    }
                }

            }
            return dt;
        }

        public static DataTable ConvertCSVtoDataTable_Contract(string strFilePath)
        {
            DataTable dt = new DataTable();
            using (StreamReader sr = new StreamReader(strFilePath))
            {
                //string[] headers = sr.ReadLine().Split(',');
                dt.Columns.Add("SKU");
                dt.Columns.Add("SUPPLIER");
                dt.Columns.Add("CONTRACT_NO");
                dt.Columns.Add("STORE");
                dt.Columns.Add("DEFAULT_STORE");

                while (!sr.EndOfStream)
                {
                    string[] rows = sr.ReadLine().Split(',');
                    DataRow dr = dt.NewRow();
                    if (rows[0].ToString() != "")
                    {
                        for (int i = 0; i <= 4; i++)
                        {
                            dr[i] = rows[i];
                        }

                        dt.Rows.Add(dr);
                    }
                }
            }
            return dt;
        }

        private void button1_Click(object sender, EventArgs e)
        {
            //DownloadFileFromFTP();

            myWorker_ItemSellPrice.RunWorkerAsync();
        }

        private void DownloadFileFromFTP()
        {
            try
            {
                // Setup session options
                SessionOptions sessionOptions = new SessionOptions
                {
                    Protocol = Protocol.Ftp,
                    HostName = "127.0.0.1",
                    UserName = "test",
                    Password = "qs0123123",
                };

                using (Session session = new Session())
                {
                    // Connect
                    session.Open(sessionOptions);

                    // Download today's times
                    TransferOptions transferOptions = new TransferOptions();
                    transferOptions.FileMask = "ITEM_*.csv;MIXMATCH_*.csv;ITEMSELLPRICE_*.csv" + "|<1D";
                    session.GetFiles("/*", @"C:\Profit_Receive\", false, transferOptions).Check();
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            try
            {
                myWorker_PostDataToMobile.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
    }
}
