﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using System.IO;
using System.Threading;
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
        class MyCounter_backgroundworker
        {
            public static int count = 0;
            public static Mutex MuTexLock = new Mutex();
        }

        private string FileConfig_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig_PRD");
        private string DirectoryFrom_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom_PRD");
        private string FileConfig2_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig2_PRD");
        private string DirectoryFrom2_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom2_PRD");
        private string FileConfigTransation = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfigTransation");
        private string DirectoryFromTransation = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFromTransation");
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
            if (MyCounter_backgroundworker.count == 0 && (args.SignalTime.Minute % 5 == 0))
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
        }

        private void myWorker_GetFile3rdParty_PRD_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFile3rdParty_PRD_DoWork");
                //check_backgroundworker_running = true;

                MyCounter_backgroundworker.MuTexLock.WaitOne();
                MyCounter_backgroundworker.count++;
                MyCounter_backgroundworker.MuTexLock.ReleaseMutex();

                //if (GetMaxTimePop(out var lastFileName, out var maxTimePop)) return;

                //DownloadFilePop(GetFilesPath(maxTimePop), lastFileName);
                DownloadFilePop_New();

                if (GetMaxTimePopMaster(out var maxTimePopMaster))
                {
                    //check day before
                    DownloadFilePopMaster_DayBefore(GetFilesPathMaster_DayBefore(maxTimePopMaster));

                    //check current day
                    DownloadFilePopMaster_DayCurrent(GetFilesPathMaster_DayCurrent(maxTimePopMaster));
                }

                if (GetMaxTimePopTransaction(out var maxTimePopTransaction))
                {
                    //check day before
                    DownloadFilePopTransaction_DayBefore(GetFilesPathTransation_DayBefore(maxTimePopTransaction));
                    //check day current
                    DownloadFilePopTransaction_DayCurrent(GetFilesPathTransation_DayCurrent(maxTimePopTransaction));
                }


                #region old

                /*
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
                TimeSpan duration = new TimeSpan(0, 0, -1, 0);


                DirectoryInfo info = new DirectoryInfo(DirectoryFrom_PRD);
                List<string> filesPath = info.GetFiles("*.csv")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                .Where(x => x.LastWriteTime >= max_time_pop.Add(duration))
                                                //.Where(x => x.LastWriteTime >= max_time_pop)
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
                TimeSpan duration_master = new TimeSpan(0, 0, -1, 0);

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
                */

                #endregion

                log.Info("UploadFile_POP3rdParty_master_PRD done!");
            }
            catch (Exception ex)
            {
                log.Error(string.Format("myWorker_GetFile3rdParty_PRD_DoWork - Exception: {0}", ex.Message));
            }
            finally
            {
                //check_backgroundworker_running = false;
                MyCounter_backgroundworker.MuTexLock.WaitOne();
                MyCounter_backgroundworker.count--;
                MyCounter_backgroundworker.MuTexLock.ReleaseMutex();
            }
        }

        private void DownloadFilePop_New()
        {
            try
            {
                //string pop_path = @"\\10.121.2.207\NFS\production\vnm\download\fpt_bi\pop_system";
                string pop_path = DirectoryFrom_PRD;
                DirectoryInfo info = new DirectoryInfo(pop_path);
                List<string> filesPath = info.GetFiles("*.csv")
                    .Select(x => x.FullName)
                    .ToList();
                log.InfoFormat("UploadFile_POP3rdParty_PRD pop_path count: {0}", filesPath.Count);
                if (filesPath.Count > 0)
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
                            log.Info("UploadFile_POP3rdParty_PRD Connected to AEON Azure");
                            var task = Task.Run(() =>
                            {
                                try
                                {
                                    foreach (var pathtg in filesPath)
                                    {
                                        using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                        {
                                            try
                                            {
                                                client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                                client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                            }
                                            catch (Exception ex)
                                            {
                                                log.ErrorFormat("GetFilePOP3rdParty_PRD: UploadFile Exception: {0}", ex.Message);
                                            }
                                        }
                                        if (client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/" + Path.GetFileName(pathtg)))
                                        {
                                            //move file to folder backup
                                            String dirBackup = DirectoryFrom_PRD + @"\fpt_backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
                                            //String dirBackup = @"C:\profit\vnm\download\fpt_bi\pop_system\fpt_backup\" + DateTime.Now.ToString("yyyyMMdd") + @"\";
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
                                                log.InfoFormat("GetFilePOP3rdParty_PRD: UploadFile successfully: {0}", dirPathBackup);
                                            }
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    log.ErrorFormat("UploadFile_POP3rdParty_PRD DownloadFilePop_New Exception: {0}", ex.Message);
                                }
                            });

                            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                            if (isCompletedSuccessfully)
                            {
                                log.InfoFormat("UploadFile_POP3rdParty_PRD DownloadFilePop_New successfully!");
                            }
                            else
                            {
                                log.ErrorFormat("UploadFile_POP3rdParty_PRD DownloadFilePop_New: The function has taken longer than the maximum time allowed.");
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_PRD can not connected to FPT Cloud");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                log.ErrorFormat("UploadFile_POP3rdParty_PRD Exception: {0}", ex.Message);
            }
        }

        private void DownloadFilePop(List<string> filesPath, string lastFileName)
        {
            if (filesPath.Count > 0)
            {
                if (filesPath.Count == 1 && Path.GetFileName(filesPath[0]).ToString() == lastFileName)
                {
                    log.Info("UploadFile_POP3rdParty_PRD get file old!");
                }
                else
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
                            log.Info("UploadFile_POP3rdParty_PRD Connected to AEON Azure");

                            var maxtimePos = 0;
                            foreach (var pathtg in filesPath)
                            {
                                var lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                if (maxtimePos == 0)
                                {
                                    log.Info(String.Format("last time pos PRD: {0}", lastwritetime));
                                    var filename = string.Format("MaxTime_Pop_PRD.csv");
                                    //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                    //{
                                    //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                    //}
                                    StreamWriter sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename),
                                        false,
                                        Encoding.Unicode);
                                    sw.Write(pathtg + ",");
                                    sw.Write(lastwritetime);
                                    sw.WriteLine();
                                    sw.Close();
                                }

                                maxtimePos++;
                                using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                {
                                    try
                                    {
                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                        client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                        client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                        log.InfoFormat("GetFilePOP3rdParty_PRD: UploadFile successfully: {0}", pathtg);
                                    }
                                    catch (Exception ex)
                                    {
                                        log.ErrorFormat("GetFilePOP3rdParty_PRD: UploadFile Exception: {0}", ex.Message);
                                    }
                                }
                            }
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_PRD can not connected to FPT Cloud");
                        }
                    }

                    log.Info("UploadFile_POP3rdParty_PRD done!");
                }
            }
            else
            {
                log.InfoFormat("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom_PRD);
            }
        }

        private void DownloadFilePopMaster_DayCurrent(IReadOnlyCollection<string> filesPathMaster)
        {
            try
            {
                if (filesPathMaster.Count > 0)
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
                            var task = Task.Run(() =>
                            {
                                try
                                {
                                    var maxtimePos = 0;
                                    foreach (var path in filesPathMaster)
                                    {
                                        var lastwritetime = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                                        if (maxtimePos == 0)
                                        {
                                            log.InfoFormat("last time pop PRD: {0}", lastwritetime);

                                            StreamWriter sw = new StreamWriter(FileConfig2_PRD, false, Encoding.Unicode);
                                            sw.Write(path + ",");
                                            sw.Write(lastwritetime);
                                            sw.WriteLine();
                                            sw.Close();
                                        }

                                        maxtimePos++;
                                        string filename = Path.GetFileName(path);
                                        if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/" + filename))
                                        {
                                            if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + filename))
                                            {
                                                if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.ToString("yyyyMMdd") + @"/" + filename))
                                                {
                                                    using (var fileStream = new FileStream(path, FileMode.Open))
                                                    {
                                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                        client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                                        log.InfoFormat("UploadFile_POP3rdParty_PRD: UploadFile_master successfully: {0}", path);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    log.InfoFormat("UploadFile_POP3rdParty_PRD: Excption {0}!", e.Message);
                                }
                            });

                            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                            if (isCompletedSuccessfully)
                            {
                                log.InfoFormat("GetFilePOP3rdParty_PRD UploadFile_master successfully!");
                            }
                            else
                            {
                                log.ErrorFormat("GetFilePOP3rdParty_PRD UploadFile_master: The function has taken longer than the maximum time allowed.");
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_master_PRD can not connected to AEON Azure");
                        }
                    }
                }
                else
                {
                    log.InfoFormat("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2_PRD);
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("DownloadFilePopMaster_DayCurrent Exception: {0}", e.Message);
            }
        }

        private List<string> GetFilesPathMaster_DayCurrent(DateTime maxTimePopMaster)
        {
            List<string> filesPathMaster = new List<string>();
            try
            {
                var task = Task.Run(() =>
                {

                    var durationMaster = new TimeSpan(0, 0, -1, 0);
                    var infoMaster =
                        new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.ToString("ddMMyyyy")));
                    filesPathMaster = infoMaster.GetFiles("ITEMBARCODE_*.csv")
                        .Union(infoMaster.GetFiles("ITEMSUPPL_*.csv"))
                        .Union(infoMaster.GetFiles("SUPPLIER_*.csv"))
                        .Union(infoMaster.GetFiles("STORE_*.csv"))
                        .Union(infoMaster.GetFiles("LINE_*.csv"))
                        .Union(infoMaster.GetFiles("DIVISION_*.csv"))
                        .Union(infoMaster.GetFiles("GROUP_*.csv"))
                        .Union(infoMaster.GetFiles("DEPT_*.csv"))
                        .Union(infoMaster.GetFiles("CATEGORY_*.csv"))
                        .Union(infoMaster.GetFiles("SCATEGORY_*.csv"))
                        .Union(infoMaster.GetFiles("SUPPLCONTRACT_*.csv"))
                        .Where(x => x.LastWriteTime >= maxTimePopMaster.Add(durationMaster))
                        .OrderByDescending(x => x.LastWriteTime)
                        .Select(x => x.FullName)
                        .ToList();

                    return filesPathMaster;
                });
                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                if (isCompletedSuccessfully)
                {
                    log.InfoFormat("GetFilesPathMaster_DayCurrent successfully! File count: {0}", task.Result.Count);
                }
                else
                {
                    log.ErrorFormat("GetFilesPathMaster_DayCurrent: The function has taken longer than the maximum time allowed.");
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("GetFilesPathMaster_DayCurrent Exception: {0}", e.Message);
            }
            return filesPathMaster;
        }

        private void DownloadFilePopMaster_DayBefore(IReadOnlyCollection<string> filesPathMaster)
        {
            try
            {
                if (filesPathMaster.Count > 0)
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
                            var task = Task.Run(() =>
                            {
                                try
                                {
                                    var maxtimePos = 0;
                                    foreach (var pathtg in filesPathMaster)
                                    {
                                        var lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                        if (maxtimePos == 0)
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

                                        maxtimePos++;
                                        string filename = Path.GetFileName(pathtg);
                                        if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/" + filename))
                                        {
                                            if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + filename))
                                            {
                                                if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.ToString("yyyyMMdd") + @"/" + filename))
                                                {
                                                    using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                                    {
                                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                        client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                                        client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                                        log.InfoFormat("UploadFile_POP3rdParty_PRD: UploadFile_master_PRD successfully: {0}", pathtg);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    log.ErrorFormat("UploadFile_POP3rdParty_PRD: Exception {0}!", e.Message);
                                }
                            });

                            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                            if (isCompletedSuccessfully)
                            {
                                log.InfoFormat("UploadFile_POP3rdParty_PRD successfully!");
                            }
                            else
                            {
                                log.ErrorFormat("UploadFile_POP3rdParty_PRD UploadFile_master_PRD: The function has taken longer than the maximum time allowed.");
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_master_PRD can not connected to AEON Azure");
                        }
                    }
                }
                else
                {
                    log.InfoFormat("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2_PRD);
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("UploadFile_POP3rdParty_PRD: Exception {0}!", e.Message);
            }
        }

        private List<string> GetFilesPathMaster_DayBefore(DateTime maxTimePopMaster)
        {
            List<string> filesPathMaster = new List<string>();
            try
            {
                var task = Task.Run(() =>
                {
                    var durationMaster = new TimeSpan(0, 0, -1, 0);
                    var infoMaster =
                        new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.AddDays(-1).ToString("ddMMyyyy")));
                    filesPathMaster = infoMaster.GetFiles("ITEMBARCODE_*.csv")
                        .Union(infoMaster.GetFiles("ITEMSUPPL_*.csv"))
                        .Union(infoMaster.GetFiles("SUPPLIER_*.csv"))
                        .Union(infoMaster.GetFiles("STORE_*.csv"))
                        .Union(infoMaster.GetFiles("LINE_*.csv"))
                        .Union(infoMaster.GetFiles("DIVISION_*.csv"))
                        .Union(infoMaster.GetFiles("GROUP_*.csv"))
                        .Union(infoMaster.GetFiles("DEPT_*.csv"))
                        .Union(infoMaster.GetFiles("CATEGORY_*.csv"))
                        .Union(infoMaster.GetFiles("SCATEGORY_*.csv"))
                        .Union(infoMaster.GetFiles("SUPPLCONTRACT_*.csv"))
                        .Where(x => x.LastWriteTime >= maxTimePopMaster.Add(durationMaster))
                        .OrderByDescending(x => x.LastWriteTime)
                        .Select(x => x.FullName)
                        .ToList();
                    return filesPathMaster;
                });
                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(300000));
                if (isCompletedSuccessfully)
                {
                    log.InfoFormat("GetFilesPathMaster_DayBefore successfully! File count: {0}", task.Result.Count);
                }
                else
                {
                    log.ErrorFormat("GetFilesPathMaster_DayBefore: The function has taken longer than the maximum time allowed.");
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("GetFilesPathMaster_DayBefore Exception: {0}", e.Message);
            }

            return filesPathMaster;
        }

        private bool GetMaxTimePopMaster(out DateTime maxTimePopMaster)
        {
            bool rs = false;
            maxTimePopMaster = DateTime.MinValue;
            try
            {
                if (File.Exists(FileConfig2_PRD))
                {
                    using (var reader = new StreamReader(FileConfig2_PRD))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (line != null)
                            {
                                var values = line.Split(',');
                                var maxTime = values[1].ToString();
                                log.Info(maxTime);
                                DateTime.TryParseExact(maxTime, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture,
                                    System.Globalization.DateTimeStyles.None, out maxTimePopMaster);
                            }
                        }
                    }
                    if (maxTimePopMaster == DateTime.MinValue)
                    {
                        log.Info(maxTimePopMaster.ToString(CultureInfo.InvariantCulture));
                        rs = false;
                    }
                    else
                    {
                        log.Info(maxTimePopMaster.ToString(CultureInfo.InvariantCulture));
                        rs = true;
                    }
                }
                else
                {
                    rs = false;
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("GetMaxTimePopMaster exception: {0}", e.Message);
            }

            return rs;
        }

        private List<string> GetFilesPath(DateTime maxTimePop)
        {
            List<string> filesPathUpload_temp = new List<string>();
            var task = Task.Run(() =>
            {
                try
                {
                    var info = new DirectoryInfo(DirectoryFrom_PRD);
                    var duration = new TimeSpan(0, 0, -1, 0);
                    filesPathUpload_temp = info.GetFiles("*.csv")
                        //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                        .Where(x => x.LastWriteTime >= maxTimePop.Add(duration))
                        .OrderByDescending(x => x.LastWriteTime)
                        .Select(x => x.FullName)
                        .ToList();
                }
                catch (Exception e)
                {
                    log.ErrorFormat("GetFilesPath-POP Exception: {0}", e.Message);
                }
                return filesPathUpload_temp;
            });
            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(300000));
            if (isCompletedSuccessfully)
            {
                log.InfoFormat("GetFilesPath-POP successfully! File count: {0}", task.Result.Count);
            }
            else
            {
                log.ErrorFormat("GetFilesPath-POP: The function has taken longer than the maximum time allowed.");
            }
            return filesPathUpload_temp;
        }

        private bool GetMaxTimePop(out string lastFileName, out DateTime maxTimePop)
        {
            maxTimePop = DateTime.MinValue;
            lastFileName = "";
            if (File.Exists(FileConfig_PRD))
            {
                using (var reader = new StreamReader(FileConfig_PRD))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        if (line == null) continue;
                        var values = line.Split(',');

                        lastFileName = values[0].ToString();
                        var maxTime = values[1].ToString();
                        log.InfoFormat("MaxTime_Pop_PRD: " + maxTime);
                        DateTime.TryParseExact(maxTime, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture,
                            System.Globalization.DateTimeStyles.None, out maxTimePop);
                    }
                }
            }

            if (maxTimePop == DateTime.MinValue)
            {
                log.Info("MaxTime_Pop_PRD: " + maxTimePop.ToString(CultureInfo.InvariantCulture));
                return true;
            }

            log.Info("MaxTime_Pop_PRD: " + maxTimePop.ToString(CultureInfo.InvariantCulture));
            return false;
        }
        private bool GetMaxTimePopTransaction(out DateTime maxTimePopTransaction)
        {
            maxTimePopTransaction = DateTime.MinValue;
            bool rs = false;
            try
            {
                if (File.Exists(FileConfigTransation))
                {
                    using (var reader = new StreamReader(FileConfigTransation))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (line != null)
                            {
                                var values = line.Split(',');
                                var maxTime = values[1].ToString();
                                log.Info(maxTime);
                                DateTime.TryParseExact(maxTime, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture,
                                    System.Globalization.DateTimeStyles.None, out maxTimePopTransaction);
                            }
                        }
                    }
                    if (maxTimePopTransaction == DateTime.MinValue)
                    {
                        log.Info(maxTimePopTransaction.ToString(CultureInfo.InvariantCulture));
                        rs = false;
                    }
                    else
                    {
                        log.Info(maxTimePopTransaction.ToString(CultureInfo.InvariantCulture));
                        rs = true;
                    }
                }
                else
                {
                    rs = false;
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("GetMaxTimePopTransaction Exception: {0}", e.Message);
            }

            return rs;
        }
        private List<string> GetFilesPathTransation_DayCurrent(DateTime maxTimePopTransaction)
        {
            List<string> filesPathTransation = new List<string>();
            try
            {
                var task = Task.Run(() =>
                {
                    var durationTransation = new TimeSpan(0, 0, 0, 0);
                    var infoTransaction =
                        new DirectoryInfo(string.Format(DirectoryFromTransation + @"\" + DateTime.Now.ToString("ddMMyyyy")));
                    filesPathTransation = infoTransaction.GetFiles("PO_*.csv")
                        .Where(x => x.LastWriteTime >= maxTimePopTransaction.Add(durationTransation))
                        .OrderByDescending(x => x.LastWriteTime)
                        .Select(x => x.FullName)
                        .ToList();

                    return filesPathTransation;
                });
                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                if (isCompletedSuccessfully)
                {
                    log.InfoFormat("GetFilesPathTransation_DayCurrent successfully! File count: {0}", task.Result.Count);
                }
                else
                {
                    log.ErrorFormat("GetFilesPathTransation_DayCurrent: The function has taken longer than the maximum time allowed.");
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("GetFilesPathTransation_DayCurrent Exception: {0}", e.Message);
            }
            return filesPathTransation;
        }
        private void DownloadFilePopTransaction_DayCurrent(IReadOnlyCollection<string> filesPathTransation)
        {
            try
            {
                if (filesPathTransation.Count > 0)
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
                            log.Info("UploadFile_POP3rdParty_Transaction Connected to AVN Azure");
                            var task = Task.Run(() =>
                            {
                                try
                                {
                                    var maxtimePos = 0;
                                    foreach (var path in filesPathTransation)
                                    {
                                        var lastwritetime = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                                        if (maxtimePos == 0)
                                        {
                                            log.InfoFormat("last time transation PRD: {0}", lastwritetime);

                                            StreamWriter sw = new StreamWriter(FileConfigTransation, false, Encoding.Unicode);
                                            sw.Write(path + ",");
                                            sw.Write(lastwritetime);
                                            sw.WriteLine();
                                            sw.Close();
                                        }

                                        maxtimePos++;
                                        string filename = Path.GetFileName(path);
                                        if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/" + filename))
                                        {
                                            if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + filename))
                                            {
                                                if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.ToString("yyyyMMdd") + @"/" + filename))
                                                {
                                                    using (var fileStream = new FileStream(path, FileMode.Open))
                                                    {
                                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                        client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                                        log.InfoFormat("UploadFile_POP3rdParty_PRD: UploadFile_transaction successfully: {0}", path);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    log.InfoFormat("UploadFile_POP3rdParty_transaction_PRD: Exception {0}!", e.Message);
                                }
                            });

                            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                            if (isCompletedSuccessfully)
                            {
                                log.InfoFormat("UploadFile_POP3rdParty_PRD UploadFile_transaction successfully!");
                            }
                            else
                            {
                                log.ErrorFormat("UploadFile_POP3rdParty_PRD UploadFile_transaction: The function has taken longer than the maximum time allowed.");
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_transaction_PRD can not connected to AVN Azure");
                        }
                    }
                }
                else
                {
                    log.InfoFormat("UploadFile_POP3rdParty_transaction_PRD: service get no file from {0}!", DirectoryFromTransation);
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("DownloadFilePopTransaction_DayCurrent Exception: {0}", e.Message);
            }
        }
        private List<string> GetFilesPathTransation_DayBefore(DateTime maxTimePopTransaction)
        {
            List<string> filesPathTransation = new List<string>();
            try
            {
                var task = Task.Run(() =>
                {

                    var durationTransation = new TimeSpan(0, 0, 0, 0);
                    var infoTransaction =
                        new DirectoryInfo(string.Format(DirectoryFromTransation + @"\" + DateTime.Now.AddDays(-1).ToString("ddMMyyyy")));
                    filesPathTransation = infoTransaction.GetFiles("PO_*.csv")
                        .Where(x => x.LastWriteTime >= maxTimePopTransaction.Add(durationTransation))
                        .OrderByDescending(x => x.LastWriteTime)
                        .Select(x => x.FullName)
                        .ToList();

                    return filesPathTransation;
                });
                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                if (isCompletedSuccessfully)
                {
                    log.InfoFormat("GetFilesPathTransation_DayBefore successfully! File count: {0}", task.Result.Count);
                }
                else
                {
                    log.ErrorFormat("GetFilesPathTransation_DayBefore: The function has taken longer than the maximum time allowed.");
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("GetFilesPathTransation_DayBefore Exception: {0}", e.Message);
            }
            return filesPathTransation;
        }
        private void DownloadFilePopTransaction_DayBefore(IReadOnlyCollection<string> filesPathTransation)
        {
            try
            {
                if (filesPathTransation.Count > 0)
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
                            log.Info("UploadFile_POP3rdParty_Transaction Connected to AVN Azure");
                            var task = Task.Run(() =>
                            {
                                try
                                {
                                    var maxtimePos = 0;
                                    foreach (var path in filesPathTransation)
                                    {
                                        var lastwritetime = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                                        if (maxtimePos == 0)
                                        {
                                            log.InfoFormat("last time transation PRD: {0}", lastwritetime);

                                            StreamWriter sw = new StreamWriter(FileConfigTransation, false, Encoding.Unicode);
                                            sw.Write(path + ",");
                                            sw.Write(lastwritetime);
                                            sw.WriteLine();
                                            sw.Close();
                                        }

                                        maxtimePos++;
                                        string filename = Path.GetFileName(path);
                                        if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/" + filename))
                                        {
                                            if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + filename))
                                            {
                                                if (!client.Exists(@"/datadrive/SFTP/POP_3rdParty_PRD/Backup/" + DateTime.Now.ToString("yyyyMMdd") + @"/" + filename))
                                                {
                                                    using (var fileStream = new FileStream(path, FileMode.Open))
                                                    {
                                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                        client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                                        log.InfoFormat("UploadFile_POP3rdParty_PRD: UploadFile_transaction successfully: {0}", path);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                catch (Exception e)
                                {
                                    log.InfoFormat("UploadFile_POP3rdParty_transaction_PRD: Exception {0}!", e.Message);
                                }
                            });

                            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                            if (isCompletedSuccessfully)
                            {
                                log.InfoFormat("GetFilePOP3rdParty_PRD UploadFile_transaction successfully!");
                            }
                            else
                            {
                                log.ErrorFormat("GetFilePOP3rdParty_PRD UploadFile_transaction: The function has taken longer than the maximum time allowed.");
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_transaction_PRD can not connected to AVN Azure");
                        }
                    }
                }
                else
                {
                    log.InfoFormat("UploadFile_POP3rdParty_transaction_PRD: service get no file from {0}!", DirectoryFromTransation);
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("DownloadFilePopTransaction_DayBefore Exception: {0}", e.Message);
            }
        }
    }
}
