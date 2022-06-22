using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.Drawing;
using System.IO;
using System.Net;
using System.Collections;
using System.Timers;
using System.Threading;
using RestSharp;

namespace AEON_POP3rdParty_WindowsService
{
    public partial class Service1 : ServiceBase
    {
        private string connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3};old guids=true;", "139.180.214.252", "aeon_pop", "fpt", "fptpop@2021");
        //private string connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3};old guids=true;", "139.180.214.252", "aeon_pop_prd", "fpt", "fptpop@2021");
        //khai báo backgroundprocess
        private BackgroundWorker myWorker_ItemSellPrice = new BackgroundWorker();
        private BackgroundWorker myWorker_PostDataToMobile = new BackgroundWorker();

        public static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        // tạo 1 biến Timer private
        private System.Timers.Timer timer = null;

        private string Folder_in = System.Configuration.ConfigurationManager.AppSettings.Get("Folder_in");

        private bool check_backgroundworker_running = false;
        private bool check_backgroundworker_PostDataToMobile_running = false;

        public Service1()
        {
            InitializeComponent();
            //khai báo properties của background process ApproveFileInDMS
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

            //myWorker_ItemSellPrice.RunWorkerAsync();
        }

        protected override void OnStart(string[] args)
        {
            log.Info("AEON_POP3rdParty_Service has started!");
            // Tạo 1 timer từ libary System.Timers
            timer = new System.Timers.Timer();
            // Execute mỗi 1 phút
            timer.Interval = 60000;
            // Những gì xảy ra khi timer đó dc tick
            timer.Elapsed += timer_Tick;
            // Enable timer
            timer.Enabled = true;
        }

        protected override void OnStop()
        {
            // Ghi log lại khi Services đã được stop
            timer.Enabled = true;
            //Utilities.WriteLogError("1st WindowsService has been stop");
            log.Info("AEON_POP3rdParty_Service has stopped!");
        }

        private void timer_Tick(object sender, ElapsedEventArgs args)
        {
            //if (args.SignalTime.Minute % 3 == 0 && check_backgroundworker_running == false)
            log.Info("check_backgroundworker_running: " + check_backgroundworker_running);
            if (check_backgroundworker_running == false)
            {
                try
                {
                    myWorker_ItemSellPrice.RunWorkerAsync();
                }
                catch (Exception e)
                {
                    log.Error(String.Format("Can not run backgroud_worker: myWorker_ItemSellPrice!|{0}", e.Message));
                }
            }
            if (check_backgroundworker_PostDataToMobile_running == false)
            {
                try
                {
                    myWorker_PostDataToMobile.RunWorkerAsync();
                }
                catch (Exception e)
                {
                    log.Error(String.Format("Can not run backgroud_worker: myWorker_PostDataToMobile!|{0}", e.Message));
                }
            }    
        }

        private void myWorker_ItemSellPrice_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {

        }

        private void myWorker_ItemSellPrice_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_ItemSellPrice_RunWorkerCompleted!");
            //check_backgroundworker_running = false;
        }

        private void myWorker_ItemSellPrice_DoWork(object sender, DoWorkEventArgs e)
        {
            log.Info("myWorker_RunWorkerBeginning!");
            check_backgroundworker_running = true;
            string file_name = "";
            string log_fileid = "";

            try
            {
                MySqlConnection connection = new MySqlConnection(connectionString);

                DirectoryInfo info = new DirectoryInfo(Folder_in);
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_SKU = String.Format(@"INSERT INTO `AEON_POP`.`sku_temp`
                                                                                    (`SKU_CODE`,`ITEM_DESC_VNM`,`PACK_ITEM`,`PERISH_ITEM`,`NON_INVENTORY`,`NON_PLU`,`MOMMY_ITEM`
                                                                                    ,`FOOD_ITEM`,`MEMBER_DISC_ITEM`,`SUPER_SAVER_ITEM`,`AUTO_REPLENISH_ITEM`,`PURCHASE_METHOD`
                                                                                    ,`LINE_ID`,`DIVISION_ID`,`GROUP_ID`,`DEPT_ID`,`CATEGORY_ID`,`SUB_CATEGORY`
                                                                                    ,`COLOUR_SIZE_GRID`,`COLOUR`,`SIZE_ID`,`POP1_DESC_VNM`,`POP2_DESC_VNM`,`POP3_DESC_VNM`
                                                                                    ,`SELLING_POINT1`,`SELLING_POINT2`,`SELLING_POINT3`,`SELLING_POINT4`,`SELLING_POINT5`
                                                                                    ,`RETAIL_UOM`,`STATUS`,`ACTIVED`,`DELETED`,`DATE_CREATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

                                            //get data
                                            string SKU_CODE = rows[0].ToString();

                                            string ITEM_DESC_VNM = "";
                                            string temp_desc = rows[4].ToString();
                                            if (temp_desc.Contains("\""))
                                            {
                                                ITEM_DESC_VNM = temp_desc.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                ITEM_DESC_VNM = rows[4].ToString();
                                            }

                                            string PACK_ITEM = "";
                                            string temp_pack_item = rows[37].ToString();
                                            if (temp_pack_item.Contains("\""))
                                            {
                                                PACK_ITEM = temp_pack_item.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                PACK_ITEM = rows[37].ToString();
                                            }

                                            string PERISH_ITEM = "";
                                            string temp_perish_item = rows[38].ToString();
                                            if (temp_perish_item.Contains("\""))
                                            {
                                                PERISH_ITEM = temp_perish_item.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                PERISH_ITEM = rows[38].ToString();
                                            }

                                            string NON_INVENTORY = "";
                                            string temp_NON_INVENTORY = rows[39].ToString();
                                            if (temp_NON_INVENTORY.Contains("\""))
                                            {
                                                NON_INVENTORY = temp_NON_INVENTORY.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                NON_INVENTORY = rows[39].ToString();
                                            }
                                            //string NON_INVENTORY = result.NON_INVENTORY;

                                            string NON_PLU = "";
                                            string temp_NON_PLU = rows[41].ToString();
                                            if (temp_NON_PLU.Contains("\""))
                                            {
                                                NON_PLU = temp_NON_PLU.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                NON_PLU = rows[41].ToString();
                                            }
                                            //string NON_PLU = result.NON_PLU;

                                            string MOMMY_ITEM = "";
                                            string temp_MOMMY_ITEM = rows[42].ToString();
                                            if (temp_MOMMY_ITEM.Contains("\""))
                                            {
                                                MOMMY_ITEM = temp_MOMMY_ITEM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                MOMMY_ITEM = rows[42].ToString();
                                            }
                                            //string MOMMY_ITEM = result.MOMMY_ITEM;

                                            string FOOD_ITEM = "";
                                            string temp_FOOD_ITEM = rows[43].ToString();
                                            if (temp_FOOD_ITEM.Contains("\""))
                                            {
                                                FOOD_ITEM = temp_FOOD_ITEM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                FOOD_ITEM = rows[43].ToString();
                                            }
                                            //string FOOD_ITEM = result.FOOD_ITEM;

                                            string MEMBER_DISC_ITEM = "";
                                            string temp_MEMBER_DISC_ITEM = rows[44].ToString();
                                            if (temp_MEMBER_DISC_ITEM.Contains("\""))
                                            {
                                                MEMBER_DISC_ITEM = temp_MEMBER_DISC_ITEM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                MEMBER_DISC_ITEM = rows[44].ToString();
                                            }
                                            //string MEMBER_DISC_ITEM = result.MEMBER_DISC_ITEM;

                                            string SUPER_SAVER_ITEM = "";
                                            string temp_SUPER_SAVER_ITEM = rows[45].ToString();
                                            if (temp_SUPER_SAVER_ITEM.Contains("\""))
                                            {
                                                SUPER_SAVER_ITEM = temp_SUPER_SAVER_ITEM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SUPER_SAVER_ITEM = rows[45].ToString();
                                            }
                                            //string SUPER_SAVER_ITEM = result.SUPER_SAVER_ITEM;

                                            string AUTO_REPLENISH_ITEM = "";
                                            string temp_AUTO_REPLENISH_ITEM = rows[47].ToString();
                                            if (temp_AUTO_REPLENISH_ITEM.Contains("\""))
                                            {
                                                AUTO_REPLENISH_ITEM = temp_AUTO_REPLENISH_ITEM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                AUTO_REPLENISH_ITEM = rows[47].ToString();
                                            }
                                            //string AUTO_REPLENISH_ITEM = result.AUTO_REPLENISH_ITEM;

                                            string PURCHASE_METHOD = "";
                                            string temp_PURCHASE_METHOD = rows[29].ToString();
                                            if (temp_PURCHASE_METHOD.Contains("\""))
                                            {
                                                PURCHASE_METHOD = temp_PURCHASE_METHOD.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                PURCHASE_METHOD = rows[29].ToString();
                                            }
                                            //string PURCHASE_METHOD = rows[29].ToString();

                                            string LINE_ID = "";
                                            string temp_LINE_ID = rows[23].ToString();
                                            if (temp_LINE_ID.Contains("\""))
                                            {
                                                LINE_ID = temp_LINE_ID.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                LINE_ID = rows[23].ToString();
                                            }
                                            //string LINE_ID = rows[23].ToString();

                                            string DIVISION_ID = "";
                                            string temp_DIVISION_ID = rows[22].ToString();
                                            if (temp_DIVISION_ID.Contains("\""))
                                            {
                                                DIVISION_ID = temp_DIVISION_ID.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                DIVISION_ID = rows[22].ToString();
                                            }
                                            //string DIVISION_ID = rows[22].ToString();

                                            string GROUP_ID = "";
                                            string temp_GROUP_ID = rows[24].ToString();
                                            if (temp_GROUP_ID.Contains("\""))
                                            {
                                                GROUP_ID = temp_GROUP_ID.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                GROUP_ID = rows[24].ToString();
                                            }
                                            //string GROUP_ID = rows[24].ToString();

                                            string DEPT_ID = "";
                                            string temp_DEPT_ID = rows[17].ToString();
                                            if (temp_DEPT_ID.Contains("\""))
                                            {
                                                DEPT_ID = temp_DEPT_ID.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                DEPT_ID = rows[17].ToString();
                                            }
                                            //string DEPT_ID = rows[17].ToString();

                                            string CATEGORY_ID = "";
                                            string temp_CATEGORY_ID = rows[19].ToString();
                                            if (temp_CATEGORY_ID.Contains("\""))
                                            {
                                                CATEGORY_ID = temp_CATEGORY_ID.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                CATEGORY_ID = rows[19].ToString();
                                            }
                                            //string CATEGORY_ID = rows[19].ToString();

                                            string SUB_CATEGORY = "";
                                            string temp_SUB_CATEGORY = rows[58].ToString();
                                            if (temp_SUB_CATEGORY.Contains("\""))
                                            {
                                                SUB_CATEGORY = temp_SUB_CATEGORY.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SUB_CATEGORY = rows[58].ToString();
                                            }
                                            //string SUB_CATEGORY = rows[58].ToString();
                                            string COLOUR_SIZE_GRID = "";
                                            string temp_COLOUR_SIZE_GRID = rows[26].ToString();
                                            if (temp_COLOUR_SIZE_GRID.Contains("\""))
                                            {
                                                COLOUR_SIZE_GRID = temp_COLOUR_SIZE_GRID.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                COLOUR_SIZE_GRID = rows[26].ToString();
                                            }
                                            //string COLOUR_SIZE_GRID = rows[26].ToString();
                                            string COLOUR = "";
                                            string temp_COLOUR = rows[27].ToString();
                                            if (temp_COLOUR.Contains("\""))
                                            {
                                                COLOUR = temp_COLOUR.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                COLOUR = rows[27].ToString();
                                            }
                                            //string COLOUR = rows[27].ToString();
                                            string SIZE_ID = "";
                                            string temp_SIZE_ID = rows[28].ToString();
                                            if (temp_SIZE_ID.Contains("\""))
                                            {
                                                SIZE_ID = temp_SIZE_ID.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SIZE_ID = rows[28].ToString();
                                            }
                                            //string SIZE_ID = rows[28].ToString();

                                            string POP1_DESC_VNM = "";
                                            string temp_POP1_DESC_VNM = rows[14].ToString();
                                            if (temp_POP1_DESC_VNM.Contains("\""))
                                            {
                                                POP1_DESC_VNM = temp_POP1_DESC_VNM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                POP1_DESC_VNM = rows[14].ToString();
                                            }
                                            //string POP1_DESC_VNM = result.POP1_DESC_VNM;
                                            string POP2_DESC_VNM = "";
                                            string temp_POP2_DESC_VNM = rows[15].ToString();
                                            if (temp_POP2_DESC_VNM.Contains("\""))
                                            {
                                                POP2_DESC_VNM = temp_POP2_DESC_VNM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                POP2_DESC_VNM = rows[15].ToString();
                                            }
                                            //string POP2_DESC_VNM = result.POP2_DESC_VNM;
                                            string POP3_DESC_VNM = "";
                                            string temp_POP3_DESC_VNM = rows[84].ToString();
                                            if (temp_POP3_DESC_VNM.Contains("\""))
                                            {
                                                POP3_DESC_VNM = temp_POP3_DESC_VNM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                POP3_DESC_VNM = rows[84].ToString();
                                            }
                                            //string POP3_DESC_VNM = result.POP3_DESC_VNM;
                                            string SELLING_POINT1 = "";
                                            string temp_SELLING_POINT1 = rows[85].ToString();
                                            if (temp_SELLING_POINT1.Contains("\""))
                                            {
                                                SELLING_POINT1 = temp_SELLING_POINT1.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SELLING_POINT1 = rows[85].ToString();
                                            }
                                            if (temp_SELLING_POINT1.Contains(@"@@"))
                                            {
                                                SELLING_POINT1 = temp_SELLING_POINT1.Replace(@"@@", ",");
                                            }
                                            else
                                            {
                                                SELLING_POINT1 = rows[85].ToString();
                                            }
                                            //string SELLING_POINT1 = rows[85].ToString();
                                            string SELLING_POINT2 = "";
                                            string temp_SELLING_POINT2 = rows[86].ToString();
                                            if (temp_SELLING_POINT2.Contains("\""))
                                            {
                                                SELLING_POINT2 = temp_SELLING_POINT2.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SELLING_POINT2 = rows[86].ToString();
                                            }
                                            if (temp_SELLING_POINT2.Contains(@"@@"))
                                            {
                                                SELLING_POINT2 = temp_SELLING_POINT2.Replace(@"@@", ",");
                                            }
                                            else
                                            {
                                                SELLING_POINT2 = rows[86].ToString();
                                            }
                                            //string SELLING_POINT2 = rows[86].ToString();
                                            string SELLING_POINT3 = "";
                                            string temp_SELLING_POINT3 = rows[87].ToString();
                                            if (temp_SELLING_POINT3.Contains("\""))
                                            {
                                                SELLING_POINT3 = temp_SELLING_POINT3.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SELLING_POINT3 = rows[87].ToString();
                                            }
                                            if (temp_SELLING_POINT3.Contains(@"@@"))
                                            {
                                                SELLING_POINT3 = temp_SELLING_POINT3.Replace(@"@@", ",");
                                            }
                                            else
                                            {
                                                SELLING_POINT3 = rows[87].ToString();
                                            }
                                            //string SELLING_POINT3 = rows[87].ToString();
                                            string SELLING_POINT4 = "";
                                            string temp_SELLING_POINT4 = rows[88].ToString();
                                            if (temp_SELLING_POINT4.Contains("\""))
                                            {
                                                SELLING_POINT4 = temp_SELLING_POINT4.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SELLING_POINT4 = rows[88].ToString();
                                            }
                                            if (temp_SELLING_POINT4.Contains(@"@@"))
                                            {
                                                SELLING_POINT4 = temp_SELLING_POINT4.Replace(@"@@", ",");
                                            }
                                            else
                                            {
                                                SELLING_POINT4 = rows[88].ToString();
                                            }
                                            //string SELLING_POINT4 = rows[88].ToString();
                                            string SELLING_POINT5 = "";
                                            string temp_SELLING_POINT5 = rows[89].ToString();
                                            if (temp_SELLING_POINT5.Contains("\""))
                                            {
                                                SELLING_POINT5 = temp_SELLING_POINT5.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                SELLING_POINT5 = rows[89].ToString();
                                            }
                                            if (temp_SELLING_POINT5.Contains(@"@@"))
                                            {
                                                SELLING_POINT5 = temp_SELLING_POINT5.Replace(@"@@", ",");
                                            }
                                            else
                                            {
                                                SELLING_POINT5 = rows[89].ToString();
                                            }
                                            //string SELLING_POINT5 = rows[89].ToString();
                                            string RETAIL_UOM = "";
                                            string temp_RETAIL_UOM = rows[62].ToString();
                                            if (temp_RETAIL_UOM.Contains("\""))
                                            {
                                                RETAIL_UOM = temp_RETAIL_UOM.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                RETAIL_UOM = rows[62].ToString();
                                            }
                                            //string RETAIL_UOM = rows[62].ToString();

                                            string STATUS = "";
                                            string temp_STATUS = rows[56].ToString();
                                            if (temp_STATUS.Contains("\""))
                                            {
                                                STATUS = temp_STATUS.Replace("\"", "\"\"");
                                            }
                                            else
                                            {
                                                STATUS = rows[56].ToString();
                                            }
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

                                                sql_insert_data_SKU = String.Format(@"INSERT INTO `AEON_POP`.`sku_temp`
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                if (filename.Substring(0, 10) == "HAMPERMST_")
                                {
                                    #region Hamper new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_Hamper = String.Format(@"INSERT INTO `AEON_POP`.`hamper_temp`(`PACK_SKU`,`DESCRIPTION`,`PACK_TYPE`,`SKU`,`QTY_PER_SKU`,`QTY_UOM`,`STORE`
                                                                                            ,`DECORATION_FLAG`,`STATUS`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
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

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                sql_insert_data_Hamper = sql_insert_data_Hamper.Substring(0, sql_insert_data_Hamper.Length - 1);
                                                var cmd_insert_data_Hamper = new MySqlCommand(sql_insert_data_Hamper, connection);
                                                MySqlDataReader rdr_insert_data_Hamper = cmd_insert_data_Hamper.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_Hamper = String.Format(@"INSERT INTO `AEON_POP`.`hamper_temp`(`PACK_SKU`,`DESCRIPTION`,`PACK_TYPE`,`SKU`,`QTY_PER_SKU`,`QTY_UOM`,`STORE`
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 13)
                            {
                                if (filename.Substring(0, 13) == "ITEMPRICECHG_")
                                {
                                    #region ItemPriceChange new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_ItemPriceChange = String.Format(@"INSERT INTO `AEON_POP`.`pricechange_temp`(`PRICE_CHANGE_NO`,`DEPARTMENT`,`TRANS_TYPE`,`REASON`,`EVENT_ID`
                                                                                            ,`PRICE_CHANGE_TYPE`,`PRICE_CHANGE_TYPE_VALUE`,`PROMOTION_TYPE`,`START_DATE`,`DAILY_START_TIME`,`END_DATE`
                                                                                            ,`DAILY_END_TIME`,`STATUS`,`STORE`,`SKU`,`LAST_SELL_PRICE`,`LAST_SELL_UNIT`,`NEW_SELL_PRICE`,`CREATED_DATE`
                                                                                            ,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

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

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                sql_insert_data_ItemPriceChange = sql_insert_data_ItemPriceChange.Substring(0, sql_insert_data_ItemPriceChange.Length - 1);
                                                var cmd_insert_data_ItemPriceChange = new MySqlCommand(sql_insert_data_ItemPriceChange, connection);
                                                MySqlDataReader rdr_insert_data_ItemPriceChange = cmd_insert_data_ItemPriceChange.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_ItemPriceChange = String.Format(@"INSERT INTO `AEON_POP`.`pricechange_temp`(`PRICE_CHANGE_NO`,`DEPARTMENT`,`TRANS_TYPE`,`REASON`,`EVENT_ID`
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                if (filename.Substring(0, 9) == "MIXMATCH_")
                                {
                                    #region MIXMATCH_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_MixMatch = String.Format(@"INSERT INTO `AEON_POP`.`mix_match_temp`(`PROMO_NO`,`PROMO_TYPE`,`PROMO_DESC`,`STATUS`,`MAX_OR_PARTIAL`
                                                                                                ,`START_DATE`,`START_TIME`,`END_DATE`,`END_TIME`,`TTL_PROMO_QTY`,`TTL_PROMO_PRICE`,`PLU_COUNT`,`EVENT_ID`,`STORE`
                                                                                                ,`SKU`,`SEQ`,`NORMAL_PRICE`,`SELL_UOM`,`PROMO_QTY`,`FOC_QTY`,`PROMO_PRICE`,`FOC_SKU`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
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

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                sql_insert_data_MixMatch = sql_insert_data_MixMatch.Substring(0, sql_insert_data_MixMatch.Length - 1);
                                                var cmd_insert_data_MixMatch = new MySqlCommand(sql_insert_data_MixMatch, connection);
                                                MySqlDataReader rdr_insert_data_MixMatch = cmd_insert_data_MixMatch.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_MixMatch = String.Format(@"INSERT INTO `AEON_POP`.`mix_match_temp`(`PROMO_NO`,`PROMO_TYPE`,`PROMO_DESC`,`STATUS`,`MAX_OR_PARTIAL`
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 12)
                            {
                                if (filename.Substring(0, 12) == "GRPPRICECHG_")
                                {
                                    #region GRPPRICECHG_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_GroupPriceChange = String.Format(@"INSERT INTO `AEON_POP`.`group_pricechange_temp`(`PRICE_CHANGE_NO`,`TRANS_TYPE`,`START_DATE`,`START_TIME`
                                                                                            ,`END_DATE`,`END_TIME`,`CATEGORY`,`STORE`,`EVENT_ID`,`EXCLUDE_SEASON_ID`,`PRICE_CHANGE_TYPE`
                                                                                            ,`PRICE_CHANGE_TYPE_VALUE`,`REASON`,`PROMOTION_TYPE`,`STATUS`,`CREATED_DATE`,`MODIFIED_DATE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
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

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                sql_insert_data_GroupPriceChange = sql_insert_data_GroupPriceChange.Substring(0, sql_insert_data_GroupPriceChange.Length - 1);
                                                var cmd_insert_data_GroupPriceChange = new MySqlCommand(sql_insert_data_GroupPriceChange, connection);
                                                MySqlDataReader rdr_insert_data_GroupPriceChange = cmd_insert_data_GroupPriceChange.ExecuteReader();
                                                connection.Close();


                                                sql_insert_data_GroupPriceChange = String.Format(@"INSERT INTO `AEON_POP`.`group_pricechange_temp`(`PRICE_CHANGE_NO`,`TRANS_TYPE`,`START_DATE`,`START_TIME`
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 14)
                            {
                                if (filename.Substring(0, 14) == "ITEMSELLPRICE_")
                                {
                                    #region ITEMSELLPRICE_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_ITEMSELLPRICE = String.Format(@"INSERT INTO `AEON_POP`.`item_sell_price_temp`(`STORE`,`SKU`
                                                                                            ,`DESCRIPTION`,`CURRENT_PRICE`,`PROMOTION_FLAG`,`PROMOTION_RETAIL`,`MEMBER_RETAIL`
                                                                                            ,`MEMBER_PROMOTION_FLAG`,`MEMBER_PROMOTION_RETAIL`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
                                            line++;

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

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                sql_insert_data_ITEMSELLPRICE = sql_insert_data_ITEMSELLPRICE.Substring(0, sql_insert_data_ITEMSELLPRICE.Length - 1);
                                                var cmd_insert_data_ITEMSELLPRICE = new MySqlCommand(sql_insert_data_ITEMSELLPRICE, connection);
                                                MySqlDataReader rdr_insert_data_ITEMSELLPRICE = cmd_insert_data_ITEMSELLPRICE.ExecuteReader();
                                                connection.Close();

                                                sql_insert_data_ITEMSELLPRICE = String.Format(@"INSERT INTO `AEON_POP`.`item_sell_price_temp`(`STORE`,`SKU`
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                         , log_fileid);
                                    connection.Open();
                                    var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                    MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                    connection.Close();
                                    #endregion
                                }
                            }
                            if (filename.Length >= 12)
                            {
                                if (filename.Substring(0, 12) == "ITEMBARCODE_")
                                {
                                    #region ITEMBARCODE_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_BarCode = String.Format(@"INSERT INTO `AEON_POP`.`barcode_temp`(`BUSINESS_UNIT`,`SKU`
                                                                                            ,`BARCODE`,`IN_HOUSE_FLAG`,`PRIMARY_FLAG`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
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

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                sql_insert_data_BarCode = sql_insert_data_BarCode.Substring(0, sql_insert_data_BarCode.Length - 1);
                                                var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_BarCode, connection);
                                                MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                                connection.Close();

                                                sql_insert_data_BarCode = String.Format(@"INSERT INTO `AEON_POP`.`barcode_temp`(`BUSINESS_UNIT`,`SKU`
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                if (filename.Substring(0, 10) == "ITEMSUPPL_")//ITEMSUPPL_ 
                                {
                                    #region ITEMSUPPL_ new
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                        var sql_insert_data_item_supplier_contract = String.Format(@"INSERT INTO `AEON_POP`.`item_supplier_contract_temp`(`SKU`,`SUPPLIER`,`CONTRACT_NO`,`STORE`,`DEFAULT_STORE`,`FILE_ID`)VALUES");

                                        while (!sr.EndOfStream)
                                        {
                                            string[] rows = sr.ReadLine().Replace("\\", "").Split(',');
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

                                            if (line == 100)
                                            {
                                                connection.Open();
                                                sql_insert_data_item_supplier_contract = sql_insert_data_item_supplier_contract.Substring(0, sql_insert_data_item_supplier_contract.Length - 1);
                                                var cmd_insert_data_BarCode = new MySqlCommand(sql_insert_data_item_supplier_contract, connection);
                                                MySqlDataReader rdr_insert_data_BarCode = cmd_insert_data_BarCode.ExecuteReader();
                                                connection.Close();

                                                sql_insert_data_item_supplier_contract = String.Format(@"INSERT INTO `AEON_POP`.`item_supplier_contract_temp`(`SKU`,`SUPPLIER`,`CONTRACT_NO`,`STORE`,`DEFAULT_STORE`,`FILE_ID`)VALUES");
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                if (filename.Substring(0, 9) == "SUPPLIER_")
                                {
                                    #region SUPPLIER_
                                    var sql_insert_profit_file = String.Format("INSERT INTO `AEON_POP`.`profit_files_log` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    String dirBackup = Folder_in + @"Backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                    bool exist = Directory.Exists(dirBackup);
                                    if (!exist)
                                    {
                                        // Tạo thư mục.
                                        Directory.CreateDirectory(dirBackup);
                                    }
                                    string dirPathBackup = dirBackup + Path.GetFileName(pathtg);
                                    File.Move(pathtg, dirPathBackup);

                                    if (File.Exists(dirPathBackup))
                                    {
                                        log.InfoFormat("Read File Success! {0}", dirPathBackup);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("Read File Failed! {0}", dirPathBackup);
                                    }


                                    //update info file to log_file
                                    var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
            }
            catch (Exception ex)
            {
                log.Error(string.Format("myWorker_RunWorker Exception: FileName - {0}, FileID - {1}, Exception: {2}", file_name, log_fileid, ex.Message));
                MySqlConnection connection = new MySqlConnection(connectionString);
                //update info file to log_file
                var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"{0}\" WHERE `FILE_ID` = '{1}';"
                                                            , ex.Message
                                                            , log_fileid);
                connection.Open();
                var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                connection.Close();
                //MessageBox.Show(ex.Message);
            }
            finally
            {
                check_backgroundworker_running = false;
            }
        }

        private void myWorker_PostDataToMobile_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_PostDataToMobile_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            //throw new NotImplementedException();
            log.Info("myWorker_PostDataToMobile_RunWorkerCompleted!");
        }

        private void myWorker_PostDataToMobile_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_PostDataToMobile_DoWork!");
                check_backgroundworker_PostDataToMobile_running = true;

                MySqlConnection connection = new MySqlConnection(connectionString);
                var sql_get_SKU = String.Format("SELECT distinct * FROM aeon_pop.sku_code_temp limit 2000;");
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
                int dem = 0;
                for (int i = 0; i < dTable_SKUCode.Rows.Count; i++)
                {
                    dem++;
                    sku_code += "\"" + dTable_SKUCode.Rows[i][0].ToString() + "\",";

                    if (dem == 100)
                    {
                        sku_code = sku_code.Substring(0, sku_code.Length - 1);
                        body += sku_code + "]}";

                        request.AddParameter("application/json", body, ParameterType.RequestBody);
                        log.InfoFormat("PostDataToMobile: Json body: {0}", body);
                        IRestResponse response = client.Execute(request);
                        //MessageBox.Show(response.StatusCode + ": " + response.Content);
                        log.InfoFormat("PostDataToMobile: Result: {0} - {1}", response.StatusCode, response.Content);

                        if (response.IsSuccessful)
                        {
                            var sql_delete_sku_code = String.Format("DELETE FROM `AEON_POP`.`sku_code_temp` WHERE SKU_CODE IN ({0});", sku_code);
                            connection.Open();
                            MySqlCommand comm_sql_delete_sku_code = connection.CreateCommand();
                            comm_sql_delete_sku_code.CommandText = sql_delete_sku_code;
                            int kq = comm_sql_delete_sku_code.ExecuteNonQuery();
                            connection.Close();

                            //MessageBox.Show(sql_delete_sku_code + ": " + kq);
                            log.InfoFormat("PostDataToMobile: Clear sku: {0} - {1}", sql_delete_sku_code, kq);
                        }

                        body = "{\"skus\": [";
                        sku_code = "";

                        request.Parameters.Clear();
                        request.AddHeader("Authorization", "6d9bf625-7f54-452d-bc37-6e89f702c17a");
                        request.AddHeader("Content-Type", "application/json");

                        dem = 0;
                    }    
                }

                if (dem > 0)
                {
                    sku_code = sku_code.Substring(0, sku_code.Length - 1);
                    body += sku_code + "]}";

                    request.AddParameter("application/json", body, ParameterType.RequestBody);
                    log.InfoFormat("PostDataToMobile: Json body: {0}", body);
                    IRestResponse response = client.Execute(request);
                    //MessageBox.Show(response.StatusCode + ": " + response.Content);
                    log.InfoFormat("PostDataToMobile: Result: {0} - {1}", response.StatusCode, response.Content);

                    if (response.IsSuccessful)
                    {
                        var sql_delete_sku_code = String.Format("DELETE FROM `AEON_POP`.`sku_code_temp` WHERE SKU_CODE IN ({0});", sku_code);
                        connection.Open();
                        MySqlCommand comm_sql_delete_sku_code = connection.CreateCommand();
                        comm_sql_delete_sku_code.CommandText = sql_delete_sku_code;
                        int kq = comm_sql_delete_sku_code.ExecuteNonQuery();
                        connection.Close();

                        //MessageBox.Show(sql_delete_sku_code + ": " + kq);
                        log.InfoFormat("PostDataToMobile: Clear sku: {0} - {1}", sql_delete_sku_code, kq);
                    }
                }    
                
            }
            catch (Exception ex)
            {
                log.ErrorFormat("PostDataToMobile: Exception: {0}", ex.Message);
            }
            finally
            {
                check_backgroundworker_PostDataToMobile_running = false;
            }
        }
    }
}
