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

namespace AEON_POP_WinForm
{
    public partial class Form1 : Form
    {
        private string connectionString = String.Format("SERVER={0};DATABASE={1};UID={2};PASSWORD={3};old guids=true;", "127.0.0.1", "AEON_POP", "root", "qs0123123");
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            string log_fileid = "";
            
            try
            {
                MySqlConnection connection = new MySqlConnection(connectionString);
                

                string[] filePaths = Directory.GetFiles(@"C:\Profit_Receive\", "*.*");
                foreach (string pathtg in filePaths)
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
                                if (filename.Substring(0,5) == "ITEM_")
                                {
                                    #region SKU
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`PROFIT_FILES_LOG` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    var sql_get_fileID = String.Format("select * from PROFIT_FILES_LOG order by FILE_ID desc limit 1");
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
                                                var sql_insert_data_SKU = String.Format(@"INSERT INTO `aeon_pop`.`sku`
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
                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`PROFIT_FILES_LOG` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    var sql_get_fileID = String.Format("select * from PROFIT_FILES_LOG order by FILE_ID desc limit 1");
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
                                                var sql_insert_data_SKU = String.Format(@"INSERT INTO `aeon_pop`.`hamper`(`PACK_SKU`,`DESCRIPTION`,`PACK_TYPE`,`SKU`,`QTY_PER_SKU`,`QTY_UOM`,`STORE`
                                                                                            ,`DECORATION_FLAG`,`STATUS`,`MODIFIED_DATE`,`FILE_ID`)
                                                                                        VALUES(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}"",""{10}"");"
                                                                                        , PACK_SKU, DESCRIPTION, PACK_TYPE, SKU, QTY_PER_SKU, QTY_UOM, STORE, DECORATION_FLAG, STATUS, MODIFIED_DATE, FILE_ID);
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
                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
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
                                    #region Hamper
                                    var sql_insert_profit_file = String.Format("INSERT INTO `aeon_pop`.`PROFIT_FILES_LOG` (`FILE_DATE`,`FILE_NAME`,`SYS_DATE`,`SYS_TIME`,`MESSAGE`) VALUES('{0}','{1}','{2}','{3}','{4}'); "
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
                                    var sql_get_fileID = String.Format("select * from PROFIT_FILES_LOG order by FILE_ID desc limit 1");
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
                                                var sql_insert_data_SKU = String.Format(@"INSERT INTO `aeon_pop`.`pricechange`(`PRICE_CHANGE_NO`,`DEPARTMENT`,`TRANS_TYPE`,`REASON`,`EVENT_ID`
                                                                                            ,`PRICE_CHANGE_TYPE`,`PRICE_CHANGE_TYPE_VALUE`,`PROMOTION_TYPE`,`START_DATE`,`DAILY_START_TIME`,`END_DATE`
                                                                                            ,`DAILY_END_TIME`,`STATUS`,`STORE`,`SKU`,`LAST_SELL_PRICE`,`LAST_SELL_UNIT`,`NEW_SELL_PRICE`,`CREATED_DATE`,`MODIFIED_DATE`,`FILE_ID`)
                                                                                        VALUES(""{0}"",""{1}"",""{2}"",""{3}"",""{4}"",""{5}"",""{6}"",""{7}"",""{8}"",""{9}""
                                                                                                ,""{10}"",""{11}"",""{12}"",""{13}"",""{14}"",""{15}"",""{16}"",""{17}"",""{18}"",""{19}""
                                                                                                ,""{20}"");"
                                                                                        , PRICE_CHANGE_NO, DEPARTMENT, TRANS_TYPE, REASON, EVENT_ID, PRICE_CHANGE_TYPE, PRICE_CHANGE_TYPE_VALUE, PROMOTION_TYPE, START_DATE, DAILY_START_TIME
                                                                                        , END_DATE, DAILY_END_TIME, STATUS, STORE, SKU, LAST_SELL_PRICE, LAST_SELL_UNIT, NEW_SELL_PRICE, CREATED_DATE, MODIFIED_DATE, FILE_ID);
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
                                        var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"Successfully\" WHERE `FILE_ID` = '{0}';"
                                                             , log_fileid);
                                        connection.Open();
                                        var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                                        MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                                        connection.Close();
                                    }
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
                var sql_update_profit_file = String.Format("UPDATE `aeon_pop`.`profit_files_log` SET `MESSAGE` = \"{0}\" WHERE `FILE_ID` = '{1}';"
                                                            , ex.Message
                                                            , log_fileid);
                connection.Open();
                var cmd_update_profit_file = new MySqlCommand(sql_update_profit_file, connection);
                MySqlDataReader rdr_update_profit_file = cmd_update_profit_file.ExecuteReader();
                connection.Close();
                MessageBox.Show(ex.Message);
            }
        }
    }
}
