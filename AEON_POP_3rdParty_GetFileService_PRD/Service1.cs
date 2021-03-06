using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using System.IO;
using Renci.SshNet;

namespace AEON_POP_3rdParty_GetFileService
{
    public partial class Service1 : ServiceBase
    {
        public static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        // tạo 1 biến Timer private
        private System.Timers.Timer timer = null;

        private BackgroundWorker myWorker_GetFile3rdParty_PRD = new BackgroundWorker();

        private bool check_backgroundworker_running = false;

        private string FileConfig_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig_PRD");
        private string DirectoryFrom_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom_PRD");
        private string FileConfig2_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig2_PRD");
        private string DirectoryFrom2_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom2_PRD");
        private string AzureHost = System.Configuration.ConfigurationManager.AppSettings.Get("AzureHost");
        private string AzurePort = System.Configuration.ConfigurationManager.AppSettings.Get("AzurePort");
        private string AzureUser = System.Configuration.ConfigurationManager.AppSettings.Get("AzureUser");
        private string AzurePwd = System.Configuration.ConfigurationManager.AppSettings.Get("AzurePwd");

        public Service1()
        {
            InitializeComponent();

            myWorker_GetFile3rdParty_PRD.DoWork += new DoWorkEventHandler(myWorker_GetFile3rdParty_PRD_DoWork);
            myWorker_GetFile3rdParty_PRD.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFile3rdParty_PRD_RunWorkerCompleted);
            myWorker_GetFile3rdParty_PRD.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFile3rdParty_PRD_ProgressChanged);
            myWorker_GetFile3rdParty_PRD.WorkerReportsProgress = true;
            myWorker_GetFile3rdParty_PRD.WorkerSupportsCancellation = true;
        }

        protected override void OnStart(string[] args)
        {
            log.Info("AEON_POP_3rdParty_GetFileService has started!");
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
            log.Info("AEON_POP_3rdParty_GetFileService has stopped!");
        }

        private void timer_Tick(object sender, ElapsedEventArgs args)
        {
            if (check_backgroundworker_running == false)
            {
                try
                {
                    myWorker_GetFile3rdParty_PRD.RunWorkerAsync();
                }
                catch (Exception e)
                {
                    log.Error(String.Format("Can not run backgroud_worker: myWorker_GetFile3rdParty!|{0}", e.Message));
                }
            }
        }

        private void myWorker_GetFile3rdParty_PRD_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFile3rdParty_PRD_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_GetFile3rdParty_PRD_RunWorkerCompleted");
            check_backgroundworker_running = false;
        }

        private void myWorker_GetFile3rdParty_PRD_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFile3rdParty_PRD_DoWork");
                check_backgroundworker_running = true;

                DateTime max_time_pop = DateTime.MinValue;
                string last_filename_pop = "";
                if (File.Exists(FileConfig_PRD))
                {
                    using (var reader = new StreamReader(FileConfig_PRD))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

                            last_filename_pop = values[0].ToString();
                            string max_time = values[1].ToString();
                            log.InfoFormat("MaxTime_Pop_PRD: " + max_time);
                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out max_time_pop);
                        }
                    }
                }
                if (max_time_pop == DateTime.MinValue)
                {
                    log.Info("MaxTime_Pop_PRD: " + max_time_pop.ToString());
                    return;
                }
                log.Info("MaxTime_Pop_PRD: " + max_time_pop.ToString());
                TimeSpan duration = new TimeSpan(0, 0, 0, 0, 1);


                DirectoryInfo info = new DirectoryInfo(DirectoryFrom_PRD);
                List<string> filesPath = info.GetFiles("*.csv")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                //.Where(x => x.LastWriteTime >= max_time_pop.Add(duration))
                                                .Where(x => x.LastWriteTime >= max_time_pop)
                                                .OrderByDescending(x => x.LastWriteTime)
                                              .Select(x => x.FullName)
                                              .ToList();
                if (filesPath.Count > 0)
                {
                    if (filesPath.Count == 1 && Path.GetFileName(filesPath[0]).ToString() == last_filename_pop)
                    {
                        log.Info("UploadFile_POP3rdParty_PRD get file old!");
                    }
                    else
                    {
                        //var host = "139.180.214.252";
                        //var port = 22;
                        //var username = "fptsftpuser";
                        //var password = "Fptsftp*2021";
                        var host = AzureHost;
                        var port = Convert.ToInt32(AzurePort);
                        var username = AzureUser;
                        var password = AzurePwd;

                        using (var client = new SftpClient(host, port, username, password))
                        {
                            client.Connect();
                            if (client.IsConnected)
                            {
                                log.Info("UploadFile_POP3rdParty_PRD Connected to AEON Azure");

                                int maxtime_pos = 0;
                                foreach (string pathtg in filesPath)
                                {
                                    string lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                    if (maxtime_pos == 0)
                                    {
                                        log.Info(String.Format("last time pos PRD: {0}", lastwritetime));
                                        string filename = string.Format("MaxTime_Pop_PRD.csv");
                                        //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                        //{
                                        //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                        //}
                                        StreamWriter sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false, Encoding.Unicode);
                                        sw.Write(pathtg + ",");
                                        sw.Write(lastwritetime);
                                        sw.WriteLine();
                                        sw.Close();
                                    }
                                    maxtime_pos++;
                                    using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                    {
                                        try
                                        {
                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                            client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                            client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                            log.Info(string.Format("GetFilePOP3rdParty_PRD: UploadFile successfully: {0}", pathtg));
                                        }
                                        catch (Exception ex)
                                        {
                                            log.Error(string.Format("GetFilePOP3rdParty_PRD: UploadFile Exception: {0}", ex.Message));
                                        }
                                    }
                                }
                            }
                            else
                            {
                                log.Error("UploadFile_POP3rdParty_PRD can not connected to AEON Azure");
                            }
                        }
                        log.Info("UploadFile_POP3rdParty_PRD done!");
                    }
                }
                else
                {
                    log.Info(string.Format("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom_PRD));
                }

                DateTime max_time_pop_master = DateTime.MinValue;
                if (File.Exists(FileConfig2_PRD))
                {
                    using (var reader = new StreamReader(FileConfig2_PRD))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

                            string max_time = values[1].ToString();
                            log.Info("MaxTime_Pop_PRD_Master: " + max_time);
                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out max_time_pop_master);
                        }
                    }
                }
                if (max_time_pop_master == DateTime.MinValue)
                {
                    log.Info("MaxTime_Pop_PRD_Master: " + max_time_pop_master.ToString());
                    return;
                }
                log.Info(max_time_pop_master.ToString());
                TimeSpan duration_master = new TimeSpan(0, 0, 0, 1);

                //check day before
                {
                    DirectoryInfo info_master = new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.AddDays(-1).ToString("ddMMyyyy")));
                    List<string> filesPath_master = info_master.GetFiles("ITEMBARCODE_*.csv")
                                                    .Union(info_master.GetFiles("ITEMSUPPL_*.csv"))
                                                    .Union(info_master.GetFiles("SUPPLIER_*.csv"))
                                                    .Union(info_master.GetFiles("STORE_*.csv"))
                                                    .Union(info_master.GetFiles("LINE_*.csv"))
                                                    .Union(info_master.GetFiles("DIVISION_*.csv"))
                                                    .Union(info_master.GetFiles("GROUP_*.csv"))
                                                    .Union(info_master.GetFiles("DEPT_*.csv"))
                                                    .Union(info_master.GetFiles("CATEGORY_*.csv"))
                                                    .Union(info_master.GetFiles("SCATEGORY_*.csv"))
                                                    //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                    .Where(x => x.LastWriteTime >= max_time_pop_master.Add(duration_master))
                                                    //.Where(x => x.LastWriteTime >= max_time_pop_master)
                                                    .OrderByDescending(x => x.LastWriteTime)
                                                  .Select(x => x.FullName)
                                                  .ToList();
                    if (filesPath_master.Count > 0)
                    {
                        var host = AzureHost;
                        var port = Convert.ToInt32(AzurePort);
                        var username = AzureUser;
                        var password = AzurePwd;

                        using (var client = new SftpClient(host, port, username, password))
                        {
                            client.Connect();
                            if (client.IsConnected)
                            {
                                log.Info("UploadFile_POP3rdParty_master Connected to AEON Azure");

                                int maxtime_pos = 0;
                                foreach (string pathtg in filesPath_master)
                                {
                                    string lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                    if (maxtime_pos == 0)
                                    {
                                        log.Info(String.Format("last time pop PRD: {0}", lastwritetime));
                                        //string filename = string.Format("MaxTime_Pop.csv");
                                        //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                        //{
                                        //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                        //}
                                        StreamWriter sw = new StreamWriter(FileConfig2_PRD, false, Encoding.Unicode);
                                        sw.Write(pathtg + ",");
                                        sw.Write(lastwritetime);
                                        sw.WriteLine();
                                        sw.Close();
                                    }
                                    maxtime_pos++;
                                    using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                    {
                                        try
                                        {
                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                            client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                            client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                            log.Info(string.Format("GetFilePOP3rdParty: UploadFile_master_PRD successfully: {0}", pathtg));
                                        }
                                        catch (Exception ex)
                                        {
                                            log.Error(string.Format("GetFilePOP3rdParty: UploadFile_master_PRD Exception: {0}", ex.Message));
                                        }
                                    }
                                }
                            }
                            else
                            {
                                log.Error("UploadFile_POP3rdParty_master_PRD can not connected to AEON Azure");
                            }
                        }
                    }
                    else
                    {
                        log.Info(string.Format("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2_PRD));
                    }
                }

                //check current day
                {
                    DirectoryInfo info_master = new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.ToString("ddMMyyyy")));
                    List<string> filesPath_master = info_master.GetFiles("ITEMBARCODE_*.csv")
                                                    .Union(info_master.GetFiles("ITEMSUPPL_*.csv"))
                                                    .Union(info_master.GetFiles("SUPPLIER_*.csv"))
                                                    .Union(info_master.GetFiles("STORE_*.csv"))
                                                    .Union(info_master.GetFiles("LINE_*.csv"))
                                                    .Union(info_master.GetFiles("DIVISION_*.csv"))
                                                    .Union(info_master.GetFiles("GROUP_*.csv"))
                                                    .Union(info_master.GetFiles("DEPT_*.csv"))
                                                    .Union(info_master.GetFiles("CATEGORY_*.csv"))
                                                    .Union(info_master.GetFiles("SCATEGORY_*.csv"))
                                                    //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                    .Where(x => x.LastWriteTime >= max_time_pop_master.Add(duration_master))
                                                    //.Where(x => x.LastWriteTime >= max_time_pop_master)
                                                    .OrderByDescending(x => x.LastWriteTime)
                                                  .Select(x => x.FullName)
                                                  .ToList();
                    if (filesPath_master.Count > 0)
                    {
                        var host = AzureHost;
                        var port = Convert.ToInt32(AzurePort);
                        var username = AzureUser;
                        var password = AzurePwd;

                        using (var client = new SftpClient(host, port, username, password))
                        {
                            client.Connect();
                            if (client.IsConnected)
                            {
                                log.Info("UploadFile_POP3rdParty_master Connected to AEON Azure");

                                int maxtime_pos = 0;
                                foreach (string pathtg in filesPath_master)
                                {
                                    string lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                    if (maxtime_pos == 0)
                                    {
                                        log.Info(String.Format("last time pop PRD: {0}", lastwritetime));
                                        //string filename = string.Format("MaxTime_Pop.csv");
                                        //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                        //{
                                        //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                        //}
                                        StreamWriter sw = new StreamWriter(FileConfig2_PRD, false, Encoding.Unicode);
                                        sw.Write(pathtg + ",");
                                        sw.Write(lastwritetime);
                                        sw.WriteLine();
                                        sw.Close();
                                    }
                                    maxtime_pos++;
                                    using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                    {
                                        try
                                        {
                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                            client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                            client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                            log.Info(string.Format("GetFilePOP3rdParty_PRD: UploadFile_master successfully: {0}", pathtg));
                                        }
                                        catch (Exception ex)
                                        {
                                            log.Error(string.Format("GetFilePOP3rdParty_PRD: UploadFile_master Exception: {0}", ex.Message));
                                        }
                                    }
                                }
                            }
                            else
                            {
                                log.Error("UploadFile_POP3rdParty_master_PRD can not connected to AEON Azure");
                            }
                        }
                    }
                    else
                    {
                        log.Info(string.Format("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2_PRD));
                    }
                }

                log.Info("UploadFile_POP3rdParty_master_PRD done!");
            }
            catch (Exception ex)
            {
                log.Error(string.Format("myWorker_GetFile3rdParty_PRD_DoWork - Exception: {0}", ex.Message));
            }
        }

    }
}
