using System;
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
        private string FileConfigTransation = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfigTransation");
        private string DirectoryFromTransation = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFromTransation");
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
            if (check_backgroundworker_running == false && args.SignalTime.Minute % 5 == 0)
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
            
        }

        private void myWorker_GetFile3rdParty_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFile3rdParty_RunWorkerBegin");
                check_backgroundworker_running = true;

                if (GetMaxTimePop(out var lastFileName, out var maxTimePop)) return;

                DownloadFilePop(GetFilesPath(maxTimePop), lastFileName);

                if (GetMaxTimePopMaster(out var maxTimePopMaster)) return;
                //check day before
                //DownloadFilePopMaster_DayBefore(GetFilesPathMaster_DayBefore(maxTimePopMaster));

                //check current day
                DownloadFilePopMaster_DayCurrent(GetFilesPathMaster_DayCurrent(maxTimePopMaster));

                //get file transaction (PO)
                if (GetMaxTimePopTransaction(out var maxTimePopTransaction)) return;
                //check day before
                DownloadFilePopTransaction_DayCurrent(GetFilesPathTransation_DayCurrent(maxTimePopTransaction));


                log.Info("UploadFile_POP3rdParty done!");

                #region old

                /*
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
                TimeSpan duration = new TimeSpan(0, 0, -1, 0);

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
                TimeSpan duration_master = new TimeSpan(0, 0, -1, 0);

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
                */

                #endregion

            }
            catch (Exception ex)
            {
                log.Error(string.Format("myWorker_GetFile3rdParty_DoWork - Exception: {0}", ex.Message));
            }
            finally
            {
                check_backgroundworker_running = false;
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
                    var host = FPTHost;
                    var port = Convert.ToInt32(FPTPort);
                    var username = FPTUser;
                    var password = FPTPwd;

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
                log.InfoFormat("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom);
            }
        }

        private void DownloadFilePopMaster_DayCurrent(IReadOnlyCollection<string> filesPathMaster)
        {
            if (filesPathMaster.Count > 0)
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

                        var maxtimePos = 0;
                        foreach (var path in filesPathMaster)
                        {
                            var lastwritetime = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                            if (maxtimePos == 0)
                            {
                                log.InfoFormat("last time pop UAT: {0}", lastwritetime);

                                StreamWriter sw = new StreamWriter(FileConfig2, false, Encoding.Unicode);
                                sw.Write(path + ",");
                                sw.Write(lastwritetime);
                                sw.WriteLine();
                                sw.Close();
                            }

                            maxtimePos++;
                            using (var fileStream = new FileStream(path, FileMode.Open))
                            {
                                try
                                {
                                    client.BufferSize = 4 * 1024; // bypass Payload error large files
                                    client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                    client.UploadFile(fileStream, Path.GetFileName(path));
                                    log.InfoFormat("GetFilePOP3rdParty_UAT: UploadFile_master successfully: {0}", path);
                                }
                                catch (Exception ex)
                                {
                                    log.ErrorFormat("GetFilePOP3rdParty_UAT: UploadFile_master Exception: {0}", ex.Message);
                                }
                            }
                        }
                    }
                    else
                    {
                        log.Error("UploadFile_POP3rdParty_master_UAT can not connected to AEON Azure");
                    }
                }
            }
            else
            {
                log.InfoFormat("UploadFile_POP3rdParty_UAT: service get no file from {0}!", DirectoryFrom2);
            }
        }

        private List<string> GetFilesPathMaster_DayCurrent(DateTime maxTimePopMaster)
        {
            List<string> filesPathMaster = new List<string>();
            var task = Task.Run(() =>
            {
                try
                {
                    var durationMaster = new TimeSpan(0, 0, -1, 0);
                    var infoMaster =
                        new DirectoryInfo(string.Format(DirectoryFrom2 + @"\" + DateTime.Now.ToString("ddMMyyyy")));
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
                }
                catch (Exception e)
                {
                    log.ErrorFormat("GetFilesPathMaster_DayCurrent Exception: {0}", e.Message);
                }
                return filesPathMaster;
            });
            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(300000));
            if (isCompletedSuccessfully)
            {
                log.InfoFormat("GetFilesPathMaster_DayCurrent successfully! File count: {0}", task.Result.Count);
            }
            else
            {
                log.ErrorFormat("GetFilesPathMaster_DayCurrent: The function has taken longer than the maximum time allowed.");
            }
            return filesPathMaster;
        }

        private void DownloadFilePopMaster_DayBefore(IReadOnlyCollection<string> filesPathMaster)
        {
            if (filesPathMaster.Count > 0)
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
                        log.Info("UploadFile_POP3rdParty_master Connected to AEON Azure");

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
                                StreamWriter sw = new StreamWriter(FileConfig2, false, Encoding.Unicode);
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
                                    log.InfoFormat("GetFilePOP3rdParty: UploadFile_master_PRD successfully: {0}", pathtg);
                                }
                                catch (Exception ex)
                                {
                                    log.ErrorFormat("GetFilePOP3rdParty: UploadFile_master_PRD Exception: {0}", ex.Message);
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
                log.InfoFormat("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2);
            }
        }

        private List<string> GetFilesPathMaster_DayBefore(DateTime maxTimePopMaster)
        {
            List<string> filesPathMaster = new List<string>();
            var task = Task.Run(() =>
            {
                try
                {
                    var durationMaster = new TimeSpan(0, 0, -1, 0);
                    var infoMaster =
                        new DirectoryInfo(string.Format(DirectoryFrom2 + @"\" + DateTime.Now.AddDays(-1).ToString("ddMMyyyy")));
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
                }
                catch (Exception e)
                {
                    log.ErrorFormat("GetFilesPathMaster_DayBefore Exception: {0}", e.Message);
                }
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
            return filesPathMaster;
        }

        private bool GetMaxTimePopMaster(out DateTime maxTimePopMaster)
        {
            maxTimePopMaster = DateTime.MinValue;
            if (File.Exists(FileConfig2))
            {
                using (var reader = new StreamReader(FileConfig2))
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
            }

            if (maxTimePopMaster == DateTime.MinValue)
            {
                log.Info(maxTimePopMaster.ToString(CultureInfo.InvariantCulture));
                return true;
            }

            log.Info(maxTimePopMaster.ToString(CultureInfo.InvariantCulture));
            return false;
        }

        private List<string> GetFilesPath(DateTime maxTimePop)
        {
            List<string> filesPathUpload_temp = new List<string>();
            var task = Task.Run(() =>
            {
                try
                {
                    var info = new DirectoryInfo(DirectoryFrom);
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
            if (File.Exists(FileConfig))
            {
                using (var reader = new StreamReader(FileConfig))
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
            }

            if (maxTimePopTransaction == DateTime.MinValue)
            {
                log.Info(maxTimePopTransaction.ToString(CultureInfo.InvariantCulture));
                return true;
            }

            log.Info(maxTimePopTransaction.ToString(CultureInfo.InvariantCulture));
            return false;
        }

        private List<string> GetFilesPathTransation_DayCurrent(DateTime maxTimePopTransaction)
        {
            List<string> filesPathTransation = new List<string>();
            var task = Task.Run(() =>
            {
                try
                {
                    var durationTransation = new TimeSpan(0, 0, 0, 0);
                    var infoMaster =
                        new DirectoryInfo(string.Format(DirectoryFromTransation));
                    filesPathTransation = infoMaster.GetFiles("PO_*.csv")
                        .Where(x => x.LastWriteTime >= maxTimePopTransaction.Add(durationTransation))
                        .OrderByDescending(x => x.LastWriteTime)
                        .Select(x => x.FullName)
                        .ToList();
                }
                catch (Exception e)
                {
                    log.ErrorFormat("GetFilesPathTransation_DayBefore Exception: {0}", e.Message);
                }
                return filesPathTransation;
            });
            bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(300000));
            if (isCompletedSuccessfully)
            {
                log.InfoFormat("GetFilesPathTransation_DayBefore successfully! File count: {0}", task.Result.Count);
            }
            else
            {
                log.ErrorFormat("GetFilesPathTransation_DayBefore: The function has taken longer than the maximum time allowed.");
            }
            return filesPathTransation;
        }
        private void DownloadFilePopTransaction_DayCurrent(IReadOnlyCollection<string> filesPathTransation)
        {
            if (filesPathTransation.Count > 0)
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
                        log.Info("UploadFile_POP3rdParty_Transation Connected to FPT Cloud");

                        var maxtimePos = 0;
                        foreach (var path in filesPathTransation)
                        {
                            var lastwritetime = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                            if (maxtimePos == 0)
                            {
                                log.InfoFormat("last time transation UAT: {0}", lastwritetime);

                                StreamWriter sw = new StreamWriter(FileConfigTransation, false, Encoding.Unicode);
                                sw.Write(path + ",");
                                sw.Write(lastwritetime);
                                sw.WriteLine();
                                sw.Close();
                            }

                            maxtimePos++;
                            using (var fileStream = new FileStream(path, FileMode.Open))
                            {
                                try
                                {
                                    client.BufferSize = 4 * 1024; // bypass Payload error large files
                                    client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                    client.UploadFile(fileStream, Path.GetFileName(path));
                                    log.InfoFormat("GetFilePOP3rdParty_PRD: UploadFile_transaction successfully: {0}", path);
                                }
                                catch (Exception ex)
                                {
                                    log.ErrorFormat("GetFilePOP3rdParty_PRD: UploadFile_transaction Exception: {0}", ex.Message);
                                }
                            }
                        }
                    }
                    else
                    {
                        log.Error("UploadFile_POP3rdParty_transaction_UAT can not connected to FPT Cloud");
                    }
                }
            }
            else
            {
                log.InfoFormat("UploadFile_POP3rdParty_transaction_UAT: service get no file from {0}!", DirectoryFromTransation);
            }
        }
    }
}
