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
using System.Linq;
using System.Net;
using WinSCP;
using System.Collections;

namespace AEON_POP_WinForm
{
    public partial class Form1 : Form
    {
        private string connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3};old guids=true;", "lyngocluan.synology.me", "AEON_POP", "fpt", "w3gXanGvWS=rV>en");

        //khai báo backgroundprocess
        private BackgroundWorker myWorker_ItemSellPrice = new BackgroundWorker();

        public Form1()
        {
            InitializeComponent();
            //khai báo properties của background process ApproveFileInDMS
            myWorker_ItemSellPrice.DoWork += new DoWorkEventHandler(myWorker_ItemSellPrice_DoWork);
            myWorker_ItemSellPrice.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_ItemSellPrice_RunWorkerCompleted);
            myWorker_ItemSellPrice.ProgressChanged += new ProgressChangedEventHandler(myWorker_ItemSellPrice_ProgressChanged);
            myWorker_ItemSellPrice.WorkerReportsProgress = true;
            myWorker_ItemSellPrice.WorkerSupportsCancellation = true;
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

            try
            { 
                MySqlConnection connection = new MySqlConnection(connectionString);

                DirectoryInfo info = new DirectoryInfo(@"C:\Profit_Receive\");
                List<string> filesPath = info.GetFiles("*.csv").Where(x => x.CreationTime.Date == DateTime.Today.AddDays(0))
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
                            if (filename.Length >= 5)
                            {
                                if (filename.Substring(0, 5) == "ITEM_")
                                {
                                    #region SKU
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

                                    using (var reader = new StreamReader(pathtg))
                                    {
                                        while (!reader.EndOfStream)
                                        {
                                            var line = reader.ReadLine();
                                            var values = line.Split(',');

                                            if (!string.IsNullOrEmpty(values[0]))
                                            {
                                                //get data
                                                string SKU_CODE = values[0];
                                                string ITEM_DESC_VNM = values[4];
                                                string SKU_TYPE = values[33];
                                                string PURCHASE_METHOD = values[29];
                                                string BUSINESS_UNIT = "";
                                                string STORE = "";
                                                string LINE_ID = values[23];
                                                string DIVISION_ID = values[22];
                                                string GROUP_ID = values[24];
                                                string DEPT_ID = values[17];
                                                string CATEGORY_ID = values[19];
                                                string SUB_CATEGORY = values[58];
                                                string COLOUR_SIZE_GRID = values[26];
                                                string COLOUR = values[27];
                                                string SIZE_ID = values[28];
                                                string BARCODE = "";
                                                string POP1_DESC_VNM = values[14];
                                                string POP2_DESC_VNM = values[16];
                                                string SELLING_POINT1 = "";
                                                string SELLING_POINT2 = "";
                                                string SELLING_POINT3 = "";
                                                string SELLING_POINT4 = "";
                                                string SELLING_POINT5 = "";
                                                string CURRENT_PRICE = "";
                                                string RETAIL_UOM = values[62];
                                                string STATUS = "";
                                                string DATE_CREATE = values[1];
                                                string MODIFIED_DATE = "";
                                                string CLOSING_STOCK_QTY = "";
                                                string CLOSING_STOCK_RETAIL = "";
                                                string FILE_ID = log_fileid;

                                                //insert data to table SKU
                                                var sql_insert_data_SKU = String.Format(@"INSERT INTO `AEON_POP`.`sku`
                                                                                (`SKU_CODE`,`ITEM_DESC_VNM`,`SKU_TYPE`,`PURCHASE_METHOD`,`BUSINESS_UNIT`,`STORE`
                                                                                    ,`LINE_ID`,`DIVISION_ID`,`GROUP_ID`,`DEPT_ID`,`CATEGORY_ID`,`SUB_CATEGORY`
                                                                                    ,`COLOUR_SIZE_GRID`,`COLOUR`,`SIZE_ID`,`BARCODE`,`POP1_DESC_VNM`,`POP2_DESC_VNM`
                                                                                    ,`SELLING_POINT1`,`SELLING_POINT2`,`SELLING_POINT3`,`SELLING_POINT4`,`SELLING_POINT5`
                                                                                    ,`CURRENT_PRICE`,`RETAIL_UOM`,`STATUS`,`DATE_CREATE`,`MODIFIED_DATE`,`CLOSING_STOCK_QTY`
                                                                                    ,`CLOSING_STOCK_RETAIL`,`FILE_ID`)
                                                                                VALUES(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                        ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                        ,""{20}"",""{21}"",""{22}"",""{23}"",""{24}"",""{25}"",""{26}"",""{27}"",""{28}"",""{29}""
                                                                                        ,""{30}"");"
                                                                                            , SKU_CODE, ITEM_DESC_VNM, SKU_TYPE, PURCHASE_METHOD, BUSINESS_UNIT, STORE, LINE_ID, DIVISION_ID, GROUP_ID, DEPT_ID
                                                                                            , CATEGORY_ID, SUB_CATEGORY, COLOUR_SIZE_GRID, COLOUR, SIZE_ID, BARCODE, POP1_DESC_VNM, POP2_DESC_VNM, SELLING_POINT1, SELLING_POINT2
                                                                                            , SELLING_POINT3, SELLING_POINT4, SELLING_POINT5, CURRENT_PRICE, RETAIL_UOM, STATUS, DATE_CREATE, MODIFIED_DATE, CLOSING_STOCK_QTY, CLOSING_STOCK_RETAIL
                                                                                            , FILE_ID);
                                                connection.Open();
                                                var cmd_insert_data_SKU = new MySqlCommand(sql_insert_data_SKU, connection);
                                                MySqlDataReader rdr_insert_data_SKU = cmd_insert_data_SKU.ExecuteReader();
                                                connection.Close();
                                            }
                                        }

                                        reader.Close();

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
                                        var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                             , log_fileid);
                                        connection.Open();
                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                        connection.Close();
                                    }
                                    #endregion
                                }
                            }
                            if (filename.Length >= 10)
                            {
                                if (filename.Substring(0, 10) == "HAMPERMST_")
                                {
                                    #region Hamper
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

                                    using (var reader = new StreamReader(pathtg))
                                    {
                                        while (!reader.EndOfStream)
                                        {
                                            var line = reader.ReadLine();
                                            var values = line.Split(',');

                                            if (!string.IsNullOrEmpty(values[0]))
                                            {
                                                //get data
                                                string PACK_SKU = values[0];
                                                string DESCRIPTION = values[2];
                                                string PACK_TYPE = values[3];
                                                string SKU = values[5];
                                                string QTY_PER_SKU = values[6];
                                                string QTY_UOM = values[7];
                                                string STORE = values[1];
                                                string DECORATION_FLAG = values[8];
                                                string STATUS = values[4];
                                                string MODIFIED_DATE = "";
                                                string FILE_ID = log_fileid;

                                                //insert data to table SKU
                                                var sql_insert_data_Hamper = String.Format(@"INSERT INTO `AEON_POP`.`hamper`(`PACK_SKU`,`DESCRIPTION`,`PACK_TYPE`,`SKU`,`QTY_PER_SKU`,`QTY_UOM`,`STORE`
                                                                                            ,`DECORATION_FLAG`,`STATUS`,`MODIFIED_DATE`,`FILE_ID`)
                                                                                        VALUES(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}"",""{10}"");"
                                                                                        , PACK_SKU, DESCRIPTION, PACK_TYPE, SKU, QTY_PER_SKU, QTY_UOM, STORE, DECORATION_FLAG, STATUS, MODIFIED_DATE, FILE_ID);
                                                connection.Open();
                                                var cmd_insert_data_Hamper = new MySqlCommand(sql_insert_data_Hamper, connection);
                                                MySqlDataReader rdr_insert_data_Hamper = cmd_insert_data_Hamper.ExecuteReader();
                                                connection.Close();
                                            }
                                        }

                                        reader.Close();

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
                                        var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                             , log_fileid);
                                        connection.Open();
                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                        connection.Close();
                                    }
                                    #endregion
                                }
                            }
                            if (filename.Length >= 13)
                            {
                                if (filename.Substring(0, 13) == "ITEMPRICECHG_")
                                {
                                    #region ItemPriceChange
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

                                    using (var reader = new StreamReader(pathtg))
                                    {
                                        while (!reader.EndOfStream)
                                        {
                                            var line = reader.ReadLine();
                                            var values = line.Split(',');

                                            if (!string.IsNullOrEmpty(values[0]))
                                            {
                                                //get data
                                                string PRICE_CHANGE_NO = values[0];
                                                string DEPARTMENT = values[5];
                                                string TRANS_TYPE = values[2];
                                                string REASON = values[3];
                                                string EVENT_ID = values[13];
                                                string PRICE_CHANGE_TYPE = values[12];
                                                string PRICE_CHANGE_TYPE_VALUE = "";
                                                string PROMOTION_TYPE = values[15];
                                                string START_DATE = values[6];
                                                string DAILY_START_TIME = values[8];
                                                string END_DATE = values[7];
                                                string DAILY_END_TIME = values[9];
                                                string STATUS = values[11];
                                                string STORE = values[16];
                                                string SKU = values[17];
                                                string LAST_SELL_PRICE = values[18];
                                                string LAST_SELL_UNIT = values[19];
                                                string NEW_SELL_PRICE = values[20];
                                                string CREATED_DATE = values[1];
                                                string MODIFIED_DATE = "";
                                                string FILE_ID = log_fileid;

                                                //insert data to table SKU
                                                var sql_insert_data_ItemPriceChange = String.Format(@"INSERT INTO `AEON_POP`.`pricechange`(`PRICE_CHANGE_NO`,`DEPARTMENT`,`TRANS_TYPE`,`REASON`,`EVENT_ID`
                                                                                            ,`PRICE_CHANGE_TYPE`,`PRICE_CHANGE_TYPE_VALUE`,`PROMOTION_TYPE`,`START_DATE`,`DAILY_START_TIME`,`END_DATE`
                                                                                            ,`DAILY_END_TIME`,`STATUS`,`STORE`,`SKU`,`LAST_SELL_PRICE`,`LAST_SELL_UNIT`,`NEW_SELL_PRICE`,`CREATED_DATE`,`MODIFIED_DATE`,`FILE_ID`)
                                                                                        VALUES(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                                ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                                ,""{20}"");"
                                                                                        , PRICE_CHANGE_NO, DEPARTMENT, TRANS_TYPE, REASON, EVENT_ID, PRICE_CHANGE_TYPE, PRICE_CHANGE_TYPE_VALUE, PROMOTION_TYPE, START_DATE, DAILY_START_TIME
                                                                                        , END_DATE, DAILY_END_TIME, STATUS, STORE, SKU, LAST_SELL_PRICE, LAST_SELL_UNIT, NEW_SELL_PRICE, CREATED_DATE, MODIFIED_DATE, FILE_ID);
                                                connection.Open();
                                                var cmd_insert_data_ItemPriceChange = new MySqlCommand(sql_insert_data_ItemPriceChange, connection);
                                                MySqlDataReader rdr_insert_data_ItemPriceChange = cmd_insert_data_ItemPriceChange.ExecuteReader();
                                                connection.Close();
                                            }
                                        }

                                        reader.Close();

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
                                        var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                             , log_fileid);
                                        connection.Open();
                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                        connection.Close();
                                    }
                                    #endregion
                                }
                            }
                            if (filename.Length >= 9)
                            {
                                if (filename.Substring(0, 9) == "MIXMATCH_")
                                {
                                    #region MixMatch
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

                                    using (var reader = new StreamReader(pathtg))
                                    {
                                        while (!reader.EndOfStream)
                                        {
                                            var line = reader.ReadLine();
                                            var values = line.Split(',');

                                            if (!string.IsNullOrEmpty(values[0]))
                                            {
                                                //get data
                                                string PROMO_NO = values[0];
                                                string PROMO_TYPE = values[1];
                                                string PROMO_DESC = values[2];
                                                string STATUS = values[7];
                                                string MAX_OR_PARTIAL = values[11];
                                                string START_DATE = values[3];
                                                string START_TIME = values[5];
                                                string END_DATE = values[4];
                                                string END_TIME = values[6];
                                                string TTL_PROMO_QTY = "";
                                                string TTL_PROMO_PRICE = "";
                                                string PLU_COUNT = "";
                                                string EVENT_ID = values[14];
                                                string STORE = values[15];
                                                string SKU = values[16];
                                                string SEQ = values[17];
                                                string NORMAL_PRICE = values[18];
                                                string SELL_UOM = values[19];
                                                string PROMO_QTY = values[20];
                                                string FOC_QTY = values[22];
                                                string PROMO_PRICE = values[21];
                                                string FOC_SKU = "";
                                                string MODIFIED_DATE = "";
                                                string FILE_ID = log_fileid;

                                                //insert data to table SKU
                                                var sql_insert_data_MixMatch = String.Format(@"INSERT INTO `AEON_POP`.`mix_match`(`PROMO_NO`,`PROMO_TYPE`,`PROMO_DESC`,`STATUS`,`MAX_OR_PARTIAL`
                                                                                                ,`START_DATE`,`START_TIME`,`END_DATE`,`END_TIME`,`TTL_PROMO_QTY`,`TTL_PROMO_PRICE`,`PLU_COUNT`,`EVENT_ID`,`STORE`
                                                                                                ,`SKU`,`SEQ`,`NORMAL_PRICE`,`SELL_UOM`,`PROMO_QTY`,`FOC_QTY`,`PROMO_PRICE`,`FOC_SKU`,`MODIFIED_DATE`,`FILE_ID`)
                                                                                            VALUES(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                                ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                                ,""{20}"",""{21}"",""{22}"",""{23}"");"
                                                                                        , PROMO_NO, PROMO_TYPE, PROMO_DESC, STATUS, MAX_OR_PARTIAL, START_DATE, START_TIME, END_DATE, END_TIME, TTL_PROMO_QTY
                                                                                        , TTL_PROMO_PRICE, PLU_COUNT, EVENT_ID, STORE, SKU, SEQ, NORMAL_PRICE, SELL_UOM, PROMO_QTY, FOC_QTY, PROMO_PRICE, FOC_SKU
                                                                                        , MODIFIED_DATE, FILE_ID);
                                                connection.Open();
                                                var cmd_insert_data_MixMatch = new MySqlCommand(sql_insert_data_MixMatch, connection);
                                                MySqlDataReader rdr_insert_data_MixMatch = cmd_insert_data_MixMatch.ExecuteReader();
                                                connection.Close();
                                            }
                                        }

                                        reader.Close();

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
                                        var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                             , log_fileid);
                                        connection.Open();
                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                        connection.Close();
                                    }
                                    #endregion
                                }
                            }
                            if (filename.Length >= 12)
                            {
                                if (filename.Substring(0, 12) == "GRPPRICECHG_")
                                {
                                    #region Group_PriceChange
                                    /*
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

                                    using (var reader = new StreamReader(pathtg))
                                    {
                                        while (!reader.EndOfStream)
                                        {
                                            var line = reader.ReadLine();
                                            var values = line.Split(',');

                                            if (!string.IsNullOrEmpty(values[0]))
                                            {
                                                //get data
                                                string PROMO_NO = values[0];
                                                string PROMO_TYPE = values[1];
                                                string PROMO_DESC = values[2];
                                                string STATUS = values[7];
                                                string MAX_OR_PARTIAL = values[11];
                                                string START_DATE = values[3];
                                                string START_TIME = values[5];
                                                string END_DATE = values[4];
                                                string END_TIME = values[6];
                                                string TTL_PROMO_QTY = "";
                                                string TTL_PROMO_PRICE = "";
                                                string PLU_COUNT = "";
                                                string EVENT_ID = values[14];
                                                string STORE = values[15];
                                                string SKU = values[16];
                                                string SEQ = values[17];
                                                string NORMAL_PRICE = values[18];
                                                string SELL_UOM = values[19];
                                                string PROMO_QTY = values[20];
                                                string FOC_QTY = values[22];
                                                string PROMO_PRICE = values[21];
                                                string FOC_SKU = "";
                                                string MODIFIED_DATE = "";
                                                string FILE_ID = log_fileid;

                                                //insert data to table SKU
                                                var sql_insert_data_MixMatch = String.Format(@"INSERT INTO `AEON_POP`.`mix_match`(`PROMO_NO`,`PROMO_TYPE`,`PROMO_DESC`,`STATUS`,`MAX_OR_PARTIAL`
                                                                                                ,`START_DATE`,`START_TIME`,`END_DATE`,`END_TIME`,`TTL_PROMO_QTY`,`TTL_PROMO_PRICE`,`PLU_COUNT`,`EVENT_ID`,`STORE`
                                                                                                ,`SKU`,`SEQ`,`NORMAL_PRICE`,`SELL_UOM`,`PROMO_QTY`,`FOC_QTY`,`PROMO_PRICE`,`FOC_SKU`,`MODIFIED_DATE`,`FILE_ID`)
                                                                                            VALUES(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                                ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                                ,""{20}"",""{21}"",""{22}"",""{23}"");"
                                                                                        , PROMO_NO, PROMO_TYPE, PROMO_DESC, STATUS, MAX_OR_PARTIAL, START_DATE, START_TIME, END_DATE, END_TIME, TTL_PROMO_QTY
                                                                                        , TTL_PROMO_PRICE, PLU_COUNT, EVENT_ID, STORE, SKU, SEQ, NORMAL_PRICE, SELL_UOM, PROMO_QTY, FOC_QTY, PROMO_PRICE, FOC_SKU
                                                                                        , MODIFIED_DATE, FILE_ID);
                                                connection.Open();
                                                var cmd_insert_data_MixMatch = new MySqlCommand(sql_insert_data_MixMatch, connection);
                                                MySqlDataReader rdr_insert_data_MixMatch = cmd_insert_data_MixMatch.ExecuteReader();
                                                connection.Close();
                                            }
                                        }

                                        reader.Close();

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
                                        var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                             , log_fileid);
                                        connection.Open();
                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                        connection.Close();
                                    }
                                    */
                                    #endregion
                                }
                            }
                            if (filename.Length >= 14)
                            {
                                if (filename.Substring(0, 14) == "ITEMSELLPRICE_")
                                {
                                    #region ITEMSELLPRICE
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
                                    DataTable dTable_ItemSellPrice_New = ConvertCSVtoDataTable(pathtg);

                                    //linq xử lý, lọc dữ liệu cần lấy
                                    var result_table = from table1 in dTable_ItemSellPrice_Cur.AsEnumerable()
                                                       join table2 in dTable_ItemSellPrice_New.AsEnumerable()
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
                                                       where ((table1["CURRENT_PRICE"].ToString()) != (table2["CURRENT_PRICE"].ToString())) 
                                                       select new {
                                                           STORE = table2 == null || table2["STORE"] == null ? string.Empty : table2["STORE"].ToString(),
                                                           SKU = table2 == null || table2["SKU"] == null ? string.Empty : table2["SKU"].ToString(),
                                                           DESCRIPTION = table2 == null || table2["DESCRIPTION"] == null ? string.Empty : table2["DESCRIPTION"].ToString(),
                                                           CURRENT_PRICE = table2 == null || table2["CURRENT_PRICE"] == null ? string.Empty : table2["CURRENT_PRICE"].ToString(),
                                                           PROMOTION_FLAG = table2 == null || table2["PROMOTION_FLAG"] == null ? string.Empty : table2["PROMOTION_FLAG"].ToString(),
                                                           PROMOTION_RETAIL = table2 == null || table2["PROMOTION_RETAIL"] == null ? string.Empty : table2["PROMOTION_RETAIL"].ToString(),
                                                           MEMBER_RETAIL = table2 == null || table2["MEMBER_RETAIL"] == null ? string.Empty : table2["MEMBER_RETAIL"].ToString(),
                                                           MEMBER_PROMOTION_FLAG = table2 == null || table2["MEMBER_PROMOTION_FLAG"] == null ? string.Empty : table2["MEMBER_PROMOTION_FLAG"].ToString(),
                                                           MEMBER_PROMOTION_RETAIL = table2 == null || table2["MEMBER_PROMOTION_RETAIL"] == null ? string.Empty : table2["MEMBER_PROMOTION_RETAIL"].ToString(),
                                                       };
                                    #endregion

                                    //insert data to table ITEMSELLPRICE
                                    var sql_insert_data_ITEMSELLPRICE = String.Format(@"INSERT INTO `AEON_POP`.`item_sell_price`(`STORE`,`SKU`
                                                                                            ,`DESCRIPTION`,`CURRENT_PRICE`,`PROMOTION_FLAG`,`PROMOTION_RETAIL`,`MEMBER_RETAIL`
                                                                                            ,`MEMBER_PROMOTION_FLAG`,`MEMBER_PROMOTION_RETAIL`,`FILE_ID`)VALUES");
                                    foreach (var result in result_table)
                                    {
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
                                    }

                                    connection.Open();
                                    sql_insert_data_ITEMSELLPRICE = sql_insert_data_ITEMSELLPRICE.Substring(0, sql_insert_data_ITEMSELLPRICE.Length - 1);
                                    var cmd_insert_data_ITEMSELLPRICE = new MySqlCommand(sql_insert_data_ITEMSELLPRICE, connection);
                                    MySqlDataReader rdr_insert_data_ITEMSELLPRICE = cmd_insert_data_ITEMSELLPRICE.ExecuteReader();
                                    connection.Close();

                                    
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

                MessageBox.Show("SS");
            }
            catch (Exception ex)
            {
                MySqlConnection connection = new MySqlConnection(connectionString);
                //update info file to log_file
                var sql_update_profit_file = String.Format("UPDATE `AEON_POP`.`profit_files_log` SET `MESSAGE` = \"{0}\" WHERE `FILE_ID` = '{1}';"
                                                            , ex.Message
                                                            , log_fileid);
                connection.Open();
                var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                connection.Close();
                MessageBox.Show(ex.Message);
            }
        }

        public static DataTable ConvertCSVtoDataTable(string strFilePath)
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

        private void button1_Click(object sender, EventArgs e)
        {
            DownloadFileFromFTP();

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
    }
}
