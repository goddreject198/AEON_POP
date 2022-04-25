using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace AEON_GetFile_WinForm
{
    public partial class Form1 : Form
    {
        public static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        //khai báo backgroundprocess
        private BackgroundWorker myWorker_GetFileCx = new BackgroundWorker();
        private BackgroundWorker myWorker_GetFile3rdParty = new BackgroundWorker();
        private BackgroundWorker myWorker_GetFile3rdParty_PRD = new BackgroundWorker();

        private string FileConfig = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig");
        private string DirectoryFrom = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom");
        private string FileConfig2 = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig2");
        private string DirectoryFrom2 = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom2");
        private string FileConfig_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig_PRD");
        private string DirectoryFrom_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom_PRD");
        private string FileConfig2_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig2_PRD");
        private string DirectoryFrom2_PRD = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom2_PRD");

        private string FPTHost = System.Configuration.ConfigurationManager.AppSettings.Get("FPTHost");
        private string FPTPort = System.Configuration.ConfigurationManager.AppSettings.Get("FPTPort");
        private string FPTUser = System.Configuration.ConfigurationManager.AppSettings.Get("FPTUser");
        private string FPTPwd = System.Configuration.ConfigurationManager.AppSettings.Get("FPTPwd");

        public Form1()
        {
            InitializeComponent();

            //khai báo properties của background process
            myWorker_GetFileCx.DoWork += new DoWorkEventHandler(myWorker_GetFileCx_DoWork);
            myWorker_GetFileCx.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFileCx_RunWorkerCompleted);
            myWorker_GetFileCx.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFileCx_ProgressChanged);
            myWorker_GetFileCx.WorkerReportsProgress = true;
            myWorker_GetFileCx.WorkerSupportsCancellation = true;

            myWorker_GetFile3rdParty.DoWork += new DoWorkEventHandler(myWorker_GetFile3rdParty_DoWork);
            myWorker_GetFile3rdParty.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFile3rdParty_RunWorkerCompleted);
            myWorker_GetFile3rdParty.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFile3rdParty_ProgressChanged);
            myWorker_GetFile3rdParty.WorkerReportsProgress = true;
            myWorker_GetFile3rdParty.WorkerSupportsCancellation = true;

            myWorker_GetFile3rdParty_PRD.DoWork += new DoWorkEventHandler(myWorker_GetFile3rdParty_PRD_DoWork);
            myWorker_GetFile3rdParty_PRD.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFile3rdParty_PRD_RunWorkerCompleted);
            myWorker_GetFile3rdParty_PRD.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFile3rdParty_PRD_ProgressChanged);
            myWorker_GetFile3rdParty_PRD.WorkerReportsProgress = true;
            myWorker_GetFile3rdParty_PRD.WorkerSupportsCancellation = true;
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

                int minute_now = DateTime.Now.Minute;
                //DirectoryInfo info = new DirectoryInfo(string.Format(@"C:\NFS\production\vnm\download\fpt_bi\pop_system\"));
                //DirectoryInfo info = new DirectoryInfo(string.Format(@"\\10.121.2.207\NFSUAT\vnmuat\download\fpt_bi\pop_system\"));
                //List<string> filesPath = info.GetFiles("*.csv").Where(x => x.LastWriteTime.Date == DateTime.Today.AddDays(0)
                //                                                                        && x.LastWriteTime.Hour == DateTime.Now.Hour
                //                                                                        && (x.LastWriteTime.Minute == minute_now
                //                                                                         || x.LastWriteTime.Minute == minute_now - 1
                //                                                                         || x.LastWriteTime.Minute == minute_now - 2
                //                                                                         || x.LastWriteTime.Minute == minute_now - 3
                //                                                                         || x.LastWriteTime.Minute == minute_now - 4))
                //                              .Select(x => x.FullName)
                //                              .ToList();
                DateTime max_time_pop = DateTime.MinValue;
                if (File.Exists(FileConfig_PRD))
                {
                    using (var reader = new StreamReader(FileConfig_PRD))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

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
                TimeSpan duration = new TimeSpan(0, 0, 0, 1);

                
                DirectoryInfo info = new DirectoryInfo(DirectoryFrom_PRD);
                List<string> filesPath = info.GetFiles("*.csv")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                .Where(x => x.LastWriteTime >= max_time_pop.Add(duration))
                                                .OrderByDescending(x => x.LastWriteTime)
                                              .Select(x => x.FullName)
                                              .ToList();
                if (filesPath.Count > 0)
                {
                    var host = "139.180.214.252";
                    var port = 22;
                    var username = "fptsftpuser";
                    var password = "Fptsftp*2021";

                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_POP3rdParty_PRD Connected to FPT Cloud");

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
                                        client.ChangeDirectory("/POP_3rdParty_PRD");
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
                            log.Error("UploadFile_POP3rdParty_PRD can not connected to FPT Cloud");
                        }
                    }
                    log.Info("UploadFile_POP3rdParty_PRD done!");
                }
                else
                {
                    log.Info(string.Format("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom));
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

                //check day before
                {
                    DirectoryInfo info_master = new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + max_time_pop_master.AddDays(-1).ToString("ddMMyyyy")));
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
                                            client.ChangeDirectory("/POP_3rdParty_PRD");
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
                                log.Error("UploadFile_POP3rdParty_master_PRD can not connected to FPT Cloud");
                            }
                        }

                        
                    }
                    else
                    {
                        log.Info(string.Format("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2));
                    }
                }

                //check current day
                {
                    DirectoryInfo info_master = new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + max_time_pop_master.ToString("ddMMyyyy")));
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
                                            client.ChangeDirectory("/POP_3rdParty_PRD");
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
                                log.Error("UploadFile_POP3rdParty_master_PRD can not connected to FPT Cloud");
                            }
                        }
                    }
                    else
                    {
                        log.Info(string.Format("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2));
                    }
                }

                log.Info("UploadFile_POP3rdParty_master_PRD done!");
            }
            catch (Exception ex)
            {
                log.Error(string.Format("myWorker_GetFile3rdParty_PRD_DoWork - Exception: {0}", ex.Message));
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

                int minute_now = DateTime.Now.Minute;
                //DirectoryInfo info = new DirectoryInfo(string.Format(@"C:\NFS\production\vnm\download\fpt_bi\pop_system\"));
                //DirectoryInfo info = new DirectoryInfo(string.Format(@"\\10.121.2.207\NFSUAT\vnmuat\download\fpt_bi\pop_system\"));
                //List<string> filesPath = info.GetFiles("*.csv").Where(x => x.LastWriteTime.Date == DateTime.Today.AddDays(0)
                //                                                                        && x.LastWriteTime.Hour == DateTime.Now.Hour
                //                                                                        && (x.LastWriteTime.Minute == minute_now
                //                                                                         || x.LastWriteTime.Minute == minute_now - 1
                //                                                                         || x.LastWriteTime.Minute == minute_now - 2
                //                                                                         || x.LastWriteTime.Minute == minute_now - 3
                //                                                                         || x.LastWriteTime.Minute == minute_now - 4))
                //                              .Select(x => x.FullName)
                //                              .ToList();
                DateTime max_time_pop = DateTime.MinValue;
                if (File.Exists(@"C:\FPTGetFile\Config\MaxTime_Pop.csv"))
                {
                    using (var reader = new StreamReader(@"C:\FPTGetFile\Config\MaxTime_Pop.csv"))
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

                DirectoryInfo info = new DirectoryInfo(string.Format(@"\\10.121.2.207\NFSUAT\vnmuat\download\fpt_bi\pop_system\"));
                List<string> filesPath = info.GetFiles("*.csv")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                .Where(x => x.LastWriteTime >= max_time_pop.Add(duration))
                                                .OrderByDescending(x => x.LastWriteTime)
                                              .Select(x => x.FullName)
                                              .ToList();
                if (filesPath.Count > 0)
                {
                    var host = "139.180.214.252";
                    var port = 22;
                    var username = "fptsftpuser";
                    var password = "Fptsftp*2021";

                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_POP3rdParty Connected to FPT Cloud");

                            int maxtime_pos = 0;
                            foreach (string pathtg in filesPath)
                            {
                                string lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                if (maxtime_pos == 0)
                                {
                                    log.Info(String.Format("last time pos: {0}", lastwritetime));
                                    string filename = string.Format("MaxTime_Pop.csv");
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

        private void myWorker_GetFileCx_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFileCx_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_GetFileCx_RunWorkerCompleted");
        }

        private void myWorker_GetFileCx_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFileCx_RunWorkerBegin");

                string[] store = new string[] { "1001", "1002", "1003", "1004", "1005", "1006", "1099", "3001", "3002", "3003", "3008", "3011", "3013", "3014", "3015", "3099"
                    , "5101", "5102", "5103", "5104", "5105", "5106", "5107", "5108", "5109", "5171", "5172", "5173", "5174", "5175", "5176", "5199", "5201", "5202", "5401"
                    , "5501", "5502", "5503", "5599", "5701", "5701_1", "5702", "5703", "5704", "5801", "5802", "5803", "5804", "5805", "5871", "5872", "5873", "5874"
                    , "5875", "5876", "5899", "5901", "5902", "5999" };
                foreach (var i in store)
                {
                    Thread t = new Thread(() => {
                        UploadFile_Cx(i);
                    });
                    t.Start();
                }
            }
            catch (Exception ex)
            {
                log.Error(string.Format("myWorker_GetFileCx_DoWork - Exception: {0}", ex.Message));
            }
        }

        private void UploadFile_Cx(string i)
        {
            try
            {
                int minute_now = DateTime.Now.Minute;
                
                DateTime max_time_cx_download = DateTime.MinValue;
                string directory_download = string.Format(@"C:\FPTGetFile\Config\MaxTime_Cx_Download_{0}.csv", i);
                if (File.Exists(directory_download))
                {
                    using (var reader = new StreamReader(directory_download))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

                            string max_time = values[1].ToString();

                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out max_time_cx_download);  
                        }
                    }
                }
                if (max_time_cx_download != DateTime.MinValue)
                {
                    log.Info(string.Format("max time download store: {0}, {1}", i, max_time_cx_download.ToString()));
                    DirectoryInfo info_download = new DirectoryInfo(string.Format(@"\\10.121.2.207\NFS\production\vnm\download\pos\{0}\backup\{1}\{2}\{3}\", i, DateTime.Now.ToString("yyyy"), DateTime.Now.ToString("MM"), DateTime.Now.ToString("dd")));
                    //List<string> filesPath_download = info_download.GetFiles("*.*").Where(x => x.LastWriteTime.Hour == DateTime.Now.Hour
                    //                                                                        && (x.LastWriteTime.Minute == minute_now
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 1
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 2
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 3
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 4))
                    //                              .Select(x => x.FullName)
                    //                              .ToList();
                    TimeSpan duration = new TimeSpan(0, 0, 0, 1);
                    List<string> filesPath_download = info_download.GetFiles("*.*")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                .Where(x => x.LastWriteTime >= max_time_cx_download.Add(duration))
                                                .OrderByDescending(x => x.LastWriteTime)
                                                  .Select(x => x.FullName)
                                                  .ToList();

                    var host = "139.180.214.252";
                    var port = 22;
                    var username = "fptsftpuser";
                    var password = "Fptsftp*2021";

                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_Cx Connected to FPT Cloud, Store: " + i);

                            int maxtime_download = 0;
                            foreach (string pathtg in filesPath_download)
                            {
                                if (maxtime_download == 0)
                                {
                                    log.Info(String.Format("last time download store {0}: {1}", i, File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss")));
                                    string filename = string.Format("MaxTime_Cx_Download_{0}.csv", i);
                                    //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                    //{
                                    //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                    //}
                                    StreamWriter sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false, Encoding.Unicode);
                                    sw.Write(pathtg + ",");
                                    sw.Write(File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss"));
                                    sw.WriteLine();
                                    sw.Close();
                                }
                                maxtime_download++;
                                using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                {
                                    try
                                    {
                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                        client.ChangeDirectory("/SAP_Cx/" + i);
                                        client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                        log.Info(string.Format("GetFileCx: UploadFile successfully store: {0}, {1}", i, pathtg));
                                    }
                                    catch (Exception ex)
                                    {
                                        log.Error(string.Format("GetFileCx: UploadFile Exception store: {0}, {1}", i, ex.Message));
                                    }
                                }

                            }
                        }
                        else
                        {
                            log.Error("UploadFile_Cx can not connected to FPT Cloud, Store: " + i);
                        }
                    }
                }
                
                
                DateTime max_time_cx_upload = DateTime.MinValue;
                string directory_upload = string.Format(@"C:\FPTGetFile\Config\MaxTime_Cx_Upload_{0}.csv", i);
                if (File.Exists(directory_upload))
                {
                    using (var reader = new StreamReader(directory_upload))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

                            string max_time = values[1].ToString();

                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out max_time_cx_upload);
                        }
                    }
                }
                if (max_time_cx_upload != DateTime.MinValue)
                {
                    log.Info(string.Format("max time upload store: {0}, {1}", i, max_time_cx_upload.ToString()));
                    DirectoryInfo info_upload = new DirectoryInfo(string.Format(@"\\10.121.2.207\NFS\production\vnm\upload\pos\{0}\backup\", i));
                    //List<string> filesPath_upload = info_upload.GetFiles("*.*").Where(x => x.LastWriteTime.Date == DateTime.Today.AddDays(0) 
                    //                                                                        && x.LastWriteTime.Hour == DateTime.Now.Hour 
                    //                                                                        && (x.LastWriteTime.Minute == minute_now
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 1
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 2
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 3
                    //                                                                         || x.LastWriteTime.Minute == minute_now - 4))
                    //                              .Select(x => x.FullName)
                    //                              .ToList();
                    TimeSpan duration = new TimeSpan(0, 0, 0, 1);
                    List<string> filesPath_upload = info_upload.GetFiles("*.*")
                                                    //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                    .Where(x => x.LastWriteTime >= max_time_cx_upload.Add(duration))
                                                    .OrderByDescending(x => x.LastWriteTime)
                                                    .Select(x => x.FullName)
                                                  .ToList();

                    var host = "139.180.214.252";
                    var port = 22;
                    var username = "fptsftpuser";
                    var password = "Fptsftp*2021";

                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_Cx Connected to FPT Cloud, Store: " + i);

                            int maxtime_upload = 0;
                            foreach (string pathtg in filesPath_upload)
                            {
                                if (maxtime_upload == 0)
                                {
                                    log.Info(String.Format("last time upload store {0}: {1}", i, File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss")));
                                    string filename = string.Format("MaxTime_Cx_Upload_{0}.csv", i);
                                    //check file, nếu có file cũ thì xóa
                                    //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                    //{
                                    //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                    //}    
                                    StreamWriter sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false, Encoding.Unicode);
                                    sw.Write(pathtg + ",");
                                    sw.Write(File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss"));
                                    sw.WriteLine();
                                    sw.Close();
                                }
                                maxtime_upload++;
                                using (var fileStream = new FileStream(pathtg, FileMode.Open))
                                {
                                    try
                                    {
                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                        client.ChangeDirectory("/SAP_Cx/" + i);
                                        client.UploadFile(fileStream, Path.GetFileName(pathtg));
                                        log.Info(string.Format("GetFileCx: UploadFile successfully: {0}", pathtg));
                                    }
                                    catch (Exception ex)
                                    {
                                        log.Error(string.Format("GetFileCx: UploadFile Exception: {0}", ex.Message));
                                    }
                                }
                            }
                        }
                        else
                        {
                            log.Error("UploadFile_Cx can not connected to FPT Cloud, Store: " + i);
                        }
                    }
                }
                
                log.Info("UploadFile_Cx done! Store: " + i);
            }
            catch (Exception ex)
            {
                log.Error(string.Format("UploadFile_Cx Thread Store: {0}, Exception: {1}", i, ex.Message));
            }
        }

        private void button1_Click(object sender, EventArgs e)
        {
            try
            {

                myWorker_GetFile3rdParty.RunWorkerAsync();

                //myWorker_GetFileCx.RunWorkerAsync();
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
                myWorker_GetFile3rdParty_PRD.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
    }
}
