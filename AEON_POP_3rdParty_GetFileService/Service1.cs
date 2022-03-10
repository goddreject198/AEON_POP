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

        private BackgroundWorker myWorker_GetFile3rdParty = new BackgroundWorker();

        private bool check_backgroundworker_running = false;

        private string FileConfig = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig");
        private string DirectoryFrom = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom");
        private string FileConfig2 = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig2");
        private string DirectoryFrom2 = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom2");
        private string FPTHost = System.Configuration.ConfigurationManager.AppSettings.Get("FPTHost");
        private string FPTPort = System.Configuration.ConfigurationManager.AppSettings.Get("FPTPort");
        private string FPTUser = System.Configuration.ConfigurationManager.AppSettings.Get("FPTUser");
        private string FPTPwd = System.Configuration.ConfigurationManager.AppSettings.Get("FPTPwd");

        public Service1()
        {
            InitializeComponent();

            myWorker_GetFile3rdParty.DoWork += new DoWorkEventHandler(myWorker_GetFile3rdParty_DoWork);
            myWorker_GetFile3rdParty.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFile3rdParty_RunWorkerCompleted);
            myWorker_GetFile3rdParty.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFile3rdParty_ProgressChanged);
            myWorker_GetFile3rdParty.WorkerReportsProgress = true;
            myWorker_GetFile3rdParty.WorkerSupportsCancellation = true;
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
                    myWorker_GetFile3rdParty.RunWorkerAsync();
                }
                catch (Exception e)
                {
                    log.Error(String.Format("Can not run backgroud_worker: myWorker_GetFile3rdParty!|{0}", e.Message));
                }
            }
        }

        private void myWorker_GetFile3rdParty_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFile3rdParty_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_GetFile3rdParty_RunWorkerCompleted");
            check_backgroundworker_running = false;
        }

        private void myWorker_GetFile3rdParty_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFile3rdParty_RunWorkerBegin");
                check_backgroundworker_running = true;

                int minute_now = DateTime.Now.Minute;
                DateTime max_time_pop = DateTime.MinValue;
                if (File.Exists(FileConfig))
                {
                    using (var reader = new StreamReader(FileConfig))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

                            string max_time = values[1].ToString();
                            log.Info(max_time);
                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out max_time_pop);
                        }
                    }
                }
                if (max_time_pop == DateTime.MinValue)
                {
                    log.Info(max_time_pop.ToString());
                    return;
                }
                log.Info(max_time_pop.ToString());
                TimeSpan duration = new TimeSpan(0, 0, 0, 1);

                DirectoryInfo info = new DirectoryInfo(string.Format(DirectoryFrom));
                List<string> filesPath = info.GetFiles("*.csv")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                .Where(x => x.LastWriteTime >= max_time_pop.Add(duration))
                                                .OrderByDescending(x => x.LastWriteTime)
                                              .Select(x => x.FullName)
                                              .ToList();
                if (filesPath.Count > 0)
                {
                    var host = FPTHost;
                    var port = Convert.ToInt32(FPTPort);
                    var username = FPTUser;
                    var password = FPTPwd;

                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_POP3rdParty_popsystem Connected to FPT Cloud");

                            int maxtime_pos = 0;
                            foreach (string pathtg in filesPath)
                            {
                                string lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                if (maxtime_pos == 0)
                                {
                                    log.Info(String.Format("last time pos: {0}", lastwritetime));
                                    //string filename = string.Format("MaxTime_Pop.csv");
                                    //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                    //{
                                    //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                    //}
                                    StreamWriter sw = new StreamWriter(FileConfig, false, Encoding.Unicode);
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
                                        client.ChangeDirectory("/POP_3rdParty");
                                        client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                        log.Info(string.Format("GetFilePOP3rdParty: UploadFile successfully: {0}", pathtg));
                                    }
                                    catch (Exception ex)
                                    {
                                        log.Error(string.Format("GetFilePOP3rdParty: UploadFile Exception: {0}", ex.Message));
                                    }
                                }
                            }
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty can not connected to FPT Cloud");
                        }
                    }
                    log.Info("UploadFile_POP3rdParty done!");
                }
                else
                {
                    log.Info(string.Format("UploadFile_POP3rdParty: service get no file from {0}!", DirectoryFrom));
                }

                DateTime max_time_pop_master = DateTime.MinValue;
                if (File.Exists(FileConfig2))
                {
                    using (var reader = new StreamReader(FileConfig2))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

                            string max_time = values[1].ToString();
                            log.Info(max_time);
                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out max_time_pop_master);
                        }
                    }
                }
                if (max_time_pop_master == DateTime.MinValue)
                {
                    log.Info(max_time_pop_master.ToString());
                    return;
                }
                log.Info(max_time_pop_master.ToString());
                TimeSpan duration_master = new TimeSpan(0, 0, 0, 1);

                DirectoryInfo info_master = new DirectoryInfo(string.Format(DirectoryFrom2));
                List<string> filesPath_master = info_master.GetFiles("ITEMBARCODE_*.csv").Union(info_master.GetFiles("ITEMSUPPL_*.csv"))
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                .Where(x => x.LastWriteTime >= max_time_pop_master.Add(duration_master))
                                                .OrderByDescending(x => x.LastWriteTime)
                                              .Select(x => x.FullName)
                                              .ToList();

                if (filesPath_master.Count > 0)
                {
                    var host = FPTHost;
                    var port = Convert.ToInt32(FPTPort);
                    var username = FPTUser;
                    var password = FPTPwd;

                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_POP3rdParty_master Connected to FPT Cloud");

                            int maxtime_pos = 0;
                            foreach (string pathtg in filesPath_master)
                            {
                                string lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                if (maxtime_pos == 0)
                                {
                                    log.Info(String.Format("last time pos: {0}", lastwritetime));
                                    //string filename = string.Format("MaxTime_Pop.csv");
                                    //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                    //{
                                    //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                    //}
                                    StreamWriter sw = new StreamWriter(FileConfig2, false, Encoding.Unicode);
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
                                        client.ChangeDirectory("/POP_3rdParty");
                                        client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                        log.Info(string.Format("GetFilePOP3rdParty: UploadFile_master successfully: {0}", pathtg));
                                    }
                                    catch (Exception ex)
                                    {
                                        log.Error(string.Format("GetFilePOP3rdParty: UploadFile_master Exception: {0}", ex.Message));
                                    }
                                }
                            }
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_master can not connected to FPT Cloud");
                        }
                    }
                    log.Info("UploadFile_POP3rdParty_master done!");
                }
                else
                {
                    log.Info(string.Format("UploadFile_POP3rdParty: service get no file from {0}!", DirectoryFrom2));
                }
            }
            catch (Exception ex)
            {
                log.Error(string.Format("myWorker_GetFile3rdParty_DoWork - Exception: {0}", ex.Message));
            }
        }

    }
}
