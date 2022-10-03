using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
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
        private BackgroundWorker myWorker_GetFile3rdParty_Azure = new BackgroundWorker();
        private BackgroundWorker myWorker_PutFileCx_Pos = new BackgroundWorker();
        private BackgroundWorker myWorker_PutFileCx_BI = new BackgroundWorker();

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
        private string AzureHost = System.Configuration.ConfigurationManager.AppSettings.Get("AzureHost");
        private string AzurePort = System.Configuration.ConfigurationManager.AppSettings.Get("AzurePort");
        private string AzureUser = System.Configuration.ConfigurationManager.AppSettings.Get("AzureUser");
        private string AzurePwd = System.Configuration.ConfigurationManager.AppSettings.Get("AzurePwd");


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

            myWorker_GetFile3rdParty_Azure.DoWork += new DoWorkEventHandler(myWorker_GetFile3rdParty_Azure_DoWork);
            myWorker_GetFile3rdParty_Azure.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFile3rdParty_Azure_RunWorkerCompleted);
            myWorker_GetFile3rdParty_Azure.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFile3rdParty_Azure_ProgressChanged);
            myWorker_GetFile3rdParty_Azure.WorkerReportsProgress = true;
            myWorker_GetFile3rdParty_Azure.WorkerSupportsCancellation = true;

            myWorker_PutFileCx_Pos.DoWork += new DoWorkEventHandler(myWorker_PutFileCx_Pos_DoWork);
            myWorker_PutFileCx_Pos.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_PutFileCx_Pos_RunWorkerCompleted);
            myWorker_PutFileCx_Pos.ProgressChanged += new ProgressChangedEventHandler(myWorker_PutFileCx_Pos_ProgressChanged);
            myWorker_PutFileCx_Pos.WorkerReportsProgress = true;
            myWorker_PutFileCx_Pos.WorkerSupportsCancellation = true;

            myWorker_PutFileCx_BI.DoWork += new DoWorkEventHandler(myWorker_PutFileCx_BI_DoWork);
            myWorker_PutFileCx_BI.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_PutFileCx_BI_RunWorkerCompleted);
            myWorker_PutFileCx_BI.ProgressChanged += new ProgressChangedEventHandler(myWorker_PutFileCx_BI_ProgressChanged);
            myWorker_PutFileCx_BI.WorkerReportsProgress = true;
            myWorker_PutFileCx_BI.WorkerSupportsCancellation = true;
        }

        private void myWorker_PutFileCx_BI_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_PutFileCx_BI_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_PutFileCx_BI_RunWorkerCompleted");
        }

        private void myWorker_PutFileCx_BI_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_PutFileCx_Pos_DoWork");

                var host = FPTHost;
                var port = Convert.ToInt32(FPTPort);
                var username = FPTUser;
                var password = FPTPwd;

                PutFileCx_BI(host, port, username, password);

                log.Info("myWorker_PutFileCx_Pos_DoWork done!");
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_PutFileCx_Pos_DoWork - Exception: {0}", ex.Message);
            }
        }

        private static void PutFileCx_BI(string host, int port, string username, string password)
        {
            using (var client = new SftpClient(host, port, username, password))
            {
                client.Connect();
                if (client.IsConnected)
                {
                    log.Info("PutFileCx_BI Connected to FPT Cloud");

                    var filesList = client.ListDirectory("/SAP_CX_UAT/Cx_Out/BI");
                    foreach (var file in filesList)
                    {
                        var remoteFileName = file.Name;
                        if (!remoteFileName.StartsWith("backup"))
                        {
                            //download file
                            using (Stream file1 = File.OpenWrite(@"E:\SAP\BO\Prod\" + remoteFileName))
                            {
                                client.DownloadFile($"/SAP_CX_UAT/Cx_Out/BI/{remoteFileName}", file1);
                            }

                            if (File.Exists(@"E:\SAP\BO\Prod\" + remoteFileName))
                            {
                                log.InfoFormat("PutFileCx_BI: download file successfully: {0}", @"E:\SAP\BO\Prod\" + remoteFileName);

                                //move file backup on server
                                var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                if (!client.Exists($"/SAP_CX_UAT/Cx_Out/BI/backup/{dateNow}"))
                                {
                                    client.CreateDirectory($"/SAP_CX_UAT/Cx_Out/BI/backup/{dateNow}");
                                }
                                client.RenameFile($"/SAP_CX_UAT/Cx_Out/BI/{remoteFileName}", $"/SAP_CX_UAT/Cx_Out/BI/backup/{dateNow}/{remoteFileName}");
                            }
                            else
                                log.ErrorFormat("PutFileCx_BI: download file failed: {0}", @"E:\SAP\BO\Prod\" + remoteFileName);
                        }
                    }
                }
                else
                {
                    log.Error("PutFileCx_BI can not connected to FPT Cloud");
                }
            }
        }

        private void myWorker_PutFileCx_Pos_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_PutFileCx_Pos_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_PutFileCx_Pos_RunWorkerCompleted");
        }

        private void myWorker_PutFileCx_Pos_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_PutFileCx_Pos_DoWork");

                var host = FPTHost;
                var port = Convert.ToInt32(FPTPort);
                var username = FPTUser;
                var password = FPTPwd;

                PutFileCx_Pos(host, port, username, password);

                log.Info("myWorker_PutFileCx_Pos_DoWork done!");
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_PutFileCx_Pos_DoWork - Exception: {0}", ex.Message);
            }
        }

        private static void PutFileCx_Pos(string host, int port, string username, string password)
        {
            using (var client = new SftpClient(host, port, username, password))
            {
                client.Connect();
                if (client.IsConnected)
                {
                    log.Info("PutFileCx_Pos Connected to FPT Cloud");

                    var filesList = client.ListDirectory("/SAP_CX_UAT/Cx_Out/POS");
                    foreach (var file in filesList)
                    {
                        var remoteFileName = file.Name;
                        if (!remoteFileName.StartsWith("backup") && remoteFileName.StartsWith("M"))
                        {
                            //download file
                            using (Stream file1 = File.OpenWrite(@"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName))
                            {
                                client.DownloadFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}", file1);
                            }

                            if (File.Exists(@"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName))
                            {
                                log.InfoFormat("PutFileCx_Pos: download file successfully: {0}", @"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName);

                                //move file backup on server
                                var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                if (!client.Exists($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}"))
                                {
                                    client.CreateDirectory($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}");
                                }
                                client.RenameFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}", $"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}/{remoteFileName}");
                            }
                            else
                                log.ErrorFormat("PutFileCx_Pos: download file failed: {0}", @"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName);
                        }
                        else if (!remoteFileName.StartsWith("backup") && !remoteFileName.StartsWith("M"))
                        {
                            var storeFolder = remoteFileName.Substring(remoteFileName.Length - 5, 1) + Path.GetExtension(remoteFileName).Substring(1,3); 
                            //download file
                            using (Stream file1 = File.OpenWrite($@"\\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}"))
                            {
                                client.DownloadFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}", file1);
                            }

                            if (File.Exists($@"\\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}"))
                            {
                                log.InfoFormat($@"PutFileCx_Pos: download file successfully: \\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}");

                                //move file backup on server
                                var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                if (!client.Exists($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}"))
                                {
                                    client.CreateDirectory($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}");
                                }
                                client.RenameFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}", $"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}/{remoteFileName}");
                            }
                            else
                                log.ErrorFormat($@"PutFileCx_Pos: download file failed: \\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}");
                        }
                    }
                }
                else
                {
                    log.Error("PutFileCx_Pos can not connected to FPT Cloud");
                }
            }
        }

        private void myWorker_GetFile3rdParty_Azure_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFile3rdParty_Azure_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_GetFile3rdParty_Azure_RunWorkerCompleted");
        }

        private void myWorker_GetFile3rdParty_Azure_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFile3rdParty_PRD_DoWork");

                if (GetMaxTimePop(out var lastFileName, out var maxTimePop)) return;

                DownloadFilePop(GetFilesPath(maxTimePop), lastFileName);

                if (GetMaxTimePopMaster(out var maxTimePopMaster)) return;
                //check day before
                DownloadFilePopMaster_DayBefore(GetFilesPathMaster_DayBefore(maxTimePopMaster));

                //check current day
                DownloadFilePopMaster_DayCurrent(GetFilesPathMaster_DayCurrent(maxTimePopMaster));
                
                log.Info("UploadFile_POP3rdParty_master_PRD done!");
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_GetFile3rdParty_PRD_DoWork - Exception: {0}", ex.Message);
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
                            using (var fileStream = new FileStream(path, FileMode.Open))
                            {
                                try
                                {
                                    client.BufferSize = 4 * 1024; // bypass Payload error large files
                                    client.ChangeDirectory("/datadrive/SFTP/POP_3rdParty_PRD");
                                    client.UploadFile(fileStream, Path.GetFileName(path));
                                    log.InfoFormat("GetFilePOP3rdParty_PRD: UploadFile_master successfully: {0}",path);
                                }
                                catch (Exception ex)
                                {
                                    log.ErrorFormat("GetFilePOP3rdParty_PRD: UploadFile_master Exception: {0}", ex.Message);
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
                log.InfoFormat("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2_PRD);
            }
        }

        private List<string> GetFilesPathMaster_DayCurrent(DateTime maxTimePopMaster)
        {
            var durationMaster = new TimeSpan(0, 0, 0, 1);
            var infoMaster = new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.ToString("ddMMyyyy")));
            var filesPathMaster = infoMaster.GetFiles("ITEMBARCODE_*.csv")
                .Union(infoMaster.GetFiles("ITEMSUPPL_*.csv"))
                .Union(infoMaster.GetFiles("SUPPLIER_*.csv"))
                .Union(infoMaster.GetFiles("STORE_*.csv"))
                .Union(infoMaster.GetFiles("LINE_*.csv"))
                .Union(infoMaster.GetFiles("DIVISION_*.csv"))
                .Union(infoMaster.GetFiles("GROUP_*.csv"))
                .Union(infoMaster.GetFiles("DEPT_*.csv"))
                .Union(infoMaster.GetFiles("CATEGORY_*.csv"))
                .Union(infoMaster.GetFiles("SCATEGORY_*.csv"))
                .Where(x => x.LastWriteTime >= maxTimePopMaster.Add(durationMaster))
                .OrderByDescending(x => x.LastWriteTime)
                .Select(x => x.FullName)
                .ToList();
            return filesPathMaster;
        }

        private void DownloadFilePopMaster_DayBefore(IReadOnlyCollection<string> filesPathMaster)
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
                log.InfoFormat("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2_PRD);
            }
        }

        private List<string> GetFilesPathMaster_DayBefore(DateTime maxTimePopMaster)
        {
            var durationMaster = new TimeSpan(0, 0, 0, 1);
            var infoMaster =
                new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.AddDays(-1).ToString("ddMMyyyy")));
            var filesPathMaster = infoMaster.GetFiles("ITEMBARCODE_*.csv")
                .Union(infoMaster.GetFiles("ITEMSUPPL_*.csv"))
                .Union(infoMaster.GetFiles("SUPPLIER_*.csv"))
                .Union(infoMaster.GetFiles("STORE_*.csv"))
                .Union(infoMaster.GetFiles("LINE_*.csv"))
                .Union(infoMaster.GetFiles("DIVISION_*.csv"))
                .Union(infoMaster.GetFiles("GROUP_*.csv"))
                .Union(infoMaster.GetFiles("DEPT_*.csv"))
                .Union(infoMaster.GetFiles("CATEGORY_*.csv"))
                .Union(infoMaster.GetFiles("SCATEGORY_*.csv"))
                .Where(x => x.LastWriteTime >= maxTimePopMaster.Add(durationMaster))
                .OrderByDescending(x => x.LastWriteTime)
                .Select(x => x.FullName)
                .ToList();
            return filesPathMaster;
        }

        private bool GetMaxTimePopMaster(out DateTime maxTimePopMaster)
        {
            maxTimePopMaster = DateTime.MinValue;
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
            var duration = new TimeSpan(0, 0, 0, 1);
            var info = new DirectoryInfo(DirectoryFrom_PRD);
            var filesPath = info.GetFiles("*.csv")
                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                .Where(x => x.LastWriteTime >= maxTimePop)
                .OrderByDescending(x => x.LastWriteTime)
                .Select(x => x.FullName)
                .ToList();
            return filesPath;
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

                var maxTimePop = DateTime.MinValue;
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
                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out maxTimePop);
                        }
                    }
                }
                if (maxTimePop == DateTime.MinValue)
                {
                    log.Info("MaxTime_Pop_PRD: " + maxTimePop.ToString());
                    return;
                }
                log.Info("MaxTime_Pop_PRD: " + maxTimePop.ToString());
                TimeSpan duration = new TimeSpan(0, 0, 0, 1);

                
                DirectoryInfo info = new DirectoryInfo(DirectoryFrom_PRD);
                List<string> filesPath = info.GetFiles("*.csv")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                //.Where(x => x.LastWriteTime >= maxTimePop.Add(duration))
                                                .Where(x => x.LastWriteTime >= maxTimePop)
                                                .OrderByDescending(x => x.LastWriteTime)
                                              .Select(x => x.FullName)
                                              .ToList();
                if (filesPath.Count > 0)
                {
                    if (filesPath.Count == 1 && Path.GetFileName(filesPath[0]).ToString() == last_filename_pop)
                    {

                    }
                    else
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
                    DirectoryInfo info_master = new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.AddDays(-1).ToString("ddMMyyyy")));
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
                        log.Info(string.Format("UploadFile_POP3rdParty_PRD: service get no file from {0}!", DirectoryFrom2_PRD));
                    }
                }

                //check current day
                {
                    DirectoryInfo info_master = new DirectoryInfo(string.Format(DirectoryFrom2_PRD + @"\" + DateTime.Now.ToString("ddMMyyyy")));
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

                var store = new string[] {
                    "1001", "1002", "1003", "1004", "1005", "1006", "1008", "3002", "3003", "3005"
                    , "3008", "3011", "3013", "3014", "3015", "3016", "3018", "3099", "5101", "5102"
                    , "5103", "5104", "5105", "5106", "5107", "5108", "5109", "5171", "5172", "5173"
                    , "5174", "5175", "5176", "5199", "5201", "5202", "5401", "5501", "5502", "5503"
                    , "5599", "5701", "5702", "5703", "5704", "5801", "5802", "5803", "5804", "5805"
                    , "5871", "5872", "5873", "5874", "5875", "5876", "5899", "5901", "5902", "5999" };
                //var store = new string[] { "1001"};
                foreach (var i in store)
                {
                    var t = new Thread(() => {
                        UploadFile_Cx(i);
                    });
                    t.Start();
                }
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_GetFileCx_DoWork - Exception: {0}", ex.Message);
            }
        }

        private void UploadFile_Cx(string i)
        {
            try
            {
                MyCounter_GetFilePos2Cx.MuTexLock.WaitOne();
                MyCounter_GetFilePos2Cx.count++;
                log.InfoFormat("UploadFile_Cx Store: {0}, MuTexLock++: {1}", i, MyCounter_GetFilePos2Cx.count);
                MyCounter_GetFilePos2Cx.MuTexLock.ReleaseMutex();

                var host = FPTHost;
                var port = Convert.ToInt32(FPTPort);
                var username = FPTUser;
                var password = FPTPwd;

                var maxTimeCxDownload = GetMaxTimeCxDownload(i);
                if (maxTimeCxDownload != DateTime.MinValue)
                {
                    log.InfoFormat("max time download store: {0}, {1}", i, maxTimeCxDownload.ToString(CultureInfo.InvariantCulture));

                    DownloadFileCx_DayBefore(i, maxTimeCxDownload, host, port, username, password);

                    DownloadFileCx_DayCurrent(i, maxTimeCxDownload, host, port, username, password);
                }
                
                //get file from dir upload
                var maxTimeCxUpload = GetMaxTimeCxUpload(i);
                if (maxTimeCxUpload != DateTime.MinValue)
                {
                    //download file from folder Upload in Middle Server
                    DownloadFileCx_Upload(i, maxTimeCxUpload, host, port, username, password);
                }
                
                log.Info("UploadFile_Cx done! Store: " + i);
            }
            catch (Exception ex)
            {
                log.ErrorFormat("UploadFile_Cx Thread Store: {0}, Exception: {1}", i, ex.Message);
            }
            finally
            {
                MyCounter_GetFilePos2Cx.MuTexLock.WaitOne();
                MyCounter_GetFilePos2Cx.count--;
                log.InfoFormat("UploadFile_Cx Store: {0}, MuTexLock--: {1}", i, MyCounter_GetFilePos2Cx.count);
                MyCounter_GetFilePos2Cx.MuTexLock.ReleaseMutex();
            }
        }

        private static void DownloadFileCx_Upload(string i, DateTime maxTimeCxUpload, string host, int port, string username, string password)
        {
            try
            {
                log.InfoFormat("max time upload store: {0}, {1}", i, maxTimeCxUpload.ToString(CultureInfo.InvariantCulture));
                var infoUpload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\upload\pos\{i}\backup\");
                //var infoUpload = new DirectoryInfo($@"C:\NFS\production\vnm\upload\pos\{i}\backup\");
                var duration = new TimeSpan(0, 0, 0, 1);
                var filesPathUpload = infoUpload.GetFiles("*.*")
                    //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                    .Where(x => (x.LastWriteTime >= maxTimeCxUpload.Add(duration)) &&
                                (Path.GetFileName(x.FullName).Substring(0, 1) == "S" ||
                                 Path.GetFileName(x.FullName).Substring(0, 1) == "W"))
                    .OrderByDescending(x => x.LastWriteTime)
                    .Select(x => x.FullName)
                    .ToList();

                if (filesPathUpload.Count > 0)
                {
                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_Cx - Upload: Connected to FPT Cloud, Store: " + i);

                            var maxtimeUpload = 0;
                            foreach (var path in filesPathUpload)
                            {
                                if (maxtimeUpload == 0)
                                {
                                    log.InfoFormat("last time upload store {0}: {1:yyyyMMddHHmmss}", i, File.GetLastWriteTime(path));
                                    var filename = $"MaxTime_Cx_Upload_{i}.csv";

                                    var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                        Encoding.Unicode);
                                    sw.Write(path + ",");
                                    sw.Write(File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss"));
                                    sw.WriteLine();
                                    sw.Close();
                                }

                                maxtimeUpload++;
                                using (var fileStream = new FileStream(path, FileMode.Open))
                                {
                                    try
                                    {
                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                        client.ChangeDirectory("/SAP_CX_UAT/" + i);
                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                        log.InfoFormat("UploadFile_Cx - Upload: UploadFile successfully: {0}", path);
                                    }
                                    catch (Exception ex)
                                    {
                                        log.ErrorFormat("UploadFile_Cx - Upload: UploadFile Exception: {0}", ex.Message);
                                    }
                                }
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_Cx - Upload: Can not connected to FPT Cloud, Store: " + i);
                        }
                    }
                }
                else
                {
                    log.InfoFormat("UploadFile_Cx - Upload: Store: {0}, File count: {1}", i, filesPathUpload.Count);
                }
            }
            catch (Exception ex)
            {
                log.InfoFormat("UploadFile_Cx - Upload - Store {0}: Exception: {1}", i, ex.Message);
            }
        }

        private static DateTime GetMaxTimeCxUpload(string i)
        {
            var maxTimeCxUpload = DateTime.MinValue;
            var directoryUpload = $@"C:\FPTGetFile\Config\MaxTime_Cx_Upload_{i}.csv";
            if (File.Exists(directoryUpload))
            {
                using (var reader = new StreamReader(directoryUpload))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        if (line != null)
                        {
                            var values = line.Split(',');

                            var maxTime = values[1].ToString();

                            DateTime.TryParseExact(maxTime, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture,
                                System.Globalization.DateTimeStyles.None, out maxTimeCxUpload);
                        }
                    }
                }
            }

            return maxTimeCxUpload;
        }

        private static void DownloadFileCx_DayCurrent(string i, DateTime maxTimeCxDownload, string host, int port, string username, string password)
        {
            try
            {
                var infoDownload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\download\pos\{i}\backup\{DateTime.Now:yyyy}\{DateTime.Now:MM}\{DateTime.Now:dd}\");
                //var infoDownload = new DirectoryInfo($@"C:\NFS\production\vnm\download\pos\{i}\backup\{DateTime.Now:yyyy}\{DateTime.Now:MM}\{DateTime.Now:dd}\");

                var duration = new TimeSpan(0, 0, 0, 1);
                var filesPathDownload = infoDownload.GetFiles("*.*")
                    //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                    .Where(x => (x.LastWriteTime >= maxTimeCxDownload.Add(duration)) &&
                                (Path.GetFileName(x.FullName).Substring(0, 1) == "D"
                                 || Path.GetFileName(x.FullName).Substring(0, 1) == "T" ||
                                 Path.GetFileName(x.FullName).Substring(0, 1) == "A"
                                 || Path.GetFileName(x.FullName).Substring(0, 1) == "C" ||
                                 Path.GetFileName(x.FullName).Substring(0, 1) == "I"))
                    .OrderByDescending(x => x.LastWriteTime)
                    .Select(x => x.FullName)
                    .ToList();

                if (filesPathDownload.Count > 0)
                {
                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_Cx - Download_Day_Current: Connected to FPT Cloud, Store: " + i);

                            var maxtimeDownload = 0;
                            foreach (var path in filesPathDownload)
                            {
                                if (maxtimeDownload == 0)
                                {
                                    log.InfoFormat("last time download store {0}: {1:yyyyMMddHHmmss}", i, File.GetLastWriteTime(path));
                                    var filename = $"MaxTime_Cx_Download_{i}.csv";
                                    var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                        Encoding.Unicode);
                                    sw.Write(path + ",");
                                    sw.Write(File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss"));
                                    sw.WriteLine();
                                    sw.Close();
                                }

                                maxtimeDownload++;
                                using (var fileStream = new FileStream(path, FileMode.Open))
                                {
                                    try
                                    {
                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                        client.ChangeDirectory("/SAP_CX_UAT/" + i);
                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                        log.InfoFormat("UploadFile_Cx - Download_Day_Current: UploadFile successfully store: {0}, {1}", i, path);
                                    }
                                    catch (Exception ex)
                                    {
                                        log.ErrorFormat("UploadFile_Cx - Download_Day_Current: UploadFile Exception store: {0}, {1}", i, ex.Message);
                                    }
                                }
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_Cx - Download_Day_Current: Can not connected to AEON Azure, Store: " + i);
                        }
                    }
                }
                else
                {
                    log.InfoFormat("UploadFile_Cx - Download_Day_Current: Store: {0}, File count: {1}", i, filesPathDownload.Count);
                }
            }
            catch (Exception ex)
            {
                log.InfoFormat("UploadFile_Cx - Download_Day_Current - Store {0}: Exception: {1}", i, ex.Message);
            }
            
        }

        private static void DownloadFileCx_DayBefore(string i, DateTime maxTimeCxDownload, string host, int port, string username, string password)
        {
            try
            {
                string day = "";
                string month = "";
                string year = "";
                if (DateTime.Now.Day == 1)
                {
                    if (DateTime.Now.Month == 1)
                    {
                        year = DateTime.Now.AddYears(-1).ToString("yyyy");
                        month = DateTime.Now.AddMonths(-1).ToString("MM");
                        day = DateTime.Now.AddDays(-1).ToString("dd");
                    }
                    else
                    {
                        year = DateTime.Now.ToString("yyyy");
                        month = DateTime.Now.AddMonths(-1).ToString("MM");
                        day = DateTime.Now.AddDays(-1).ToString("dd");
                    }
                }
                else
                {
                    year = DateTime.Now.ToString("yyyy");
                    month = DateTime.Now.ToString("MM");
                    day = DateTime.Now.ToString("dd");
                }

                var infoDownload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\download\pos\{i}\backup\{year}\{month}\{day}\");
                //var infoDownload = new DirectoryInfo($@"C:\NFS\production\vnm\download\pos\{i}\backup\{year}\{month}\{day}\");
                var duration = new TimeSpan(0, 0, 0, 1);
                var filesPathDownload = infoDownload.GetFiles("*.*")
                    //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                    .Where(x => (x.LastWriteTime >= maxTimeCxDownload.Add(duration)) &&
                                (Path.GetFileName(x.FullName).Substring(0, 1) == "D"
                                 || Path.GetFileName(x.FullName).Substring(0, 1) == "T" ||
                                 Path.GetFileName(x.FullName).Substring(0, 1) == "A"
                                 || Path.GetFileName(x.FullName).Substring(0, 1) == "C" ||
                                 Path.GetFileName(x.FullName).Substring(0, 1) == "I"))
                    .OrderByDescending(x => x.LastWriteTime)
                    .Select(x => x.FullName)
                    .ToList();


                if (filesPathDownload.Count > 0)
                {
                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("UploadFile_Cx - Download_Day_Before: Connected to AEON Azure, Store: " + i);

                            var maxtimeDownload = 0;
                            foreach (var path in filesPathDownload)
                            {
                                if (maxtimeDownload == 0)
                                {
                                    log.InfoFormat("last time download store {0}: {1:yyyyMMddHHmmss}", i, File.GetLastWriteTime(path));
                                    var filename = $"MaxTime_Cx_Download_{i}.csv";

                                    var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                        Encoding.Unicode);
                                    sw.Write(path + ",");
                                    sw.Write(File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss"));
                                    sw.WriteLine();
                                    sw.Close();
                                }

                                maxtimeDownload++;
                                using (var fileStream = new FileStream(path, FileMode.Open))
                                {
                                    try
                                    {
                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                        client.ChangeDirectory("/SAP_CX_UAT/" + i);
                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                        log.InfoFormat("UploadFile_Cx - Download_Day_Before: UploadFile successfully store: {0}, {1}", i, path);
                                    }
                                    catch (Exception ex)
                                    {
                                        log.ErrorFormat("UploadFile_Cx - Download_Day_Before: UploadFile Exception store: {0}, {1}", i, ex.Message);
                                    }
                                }
                            }
                            client.Disconnect();
                        }
                        else
                        {
                            log.Error("UploadFile_Cx - Download_Day_Before: Can not connected to AEON Azure, Store: " + i);
                        }
                    }
                }
                else
                {
                    log.InfoFormat("UploadFile_Cx - Download_Day_Before: Store: {0}, File count: {1}", i, filesPathDownload.Count);
                }
            }
            catch (Exception ex)
            {
                log.InfoFormat("UploadFile_Cx - Download_Day_Before - Store {0}: Exception: {1}", i, ex.Message);
            }
            
        }

        private static DateTime GetMaxTimeCxDownload(string i)
        {
            var maxTimeCxDownload = DateTime.MinValue;
            var directoryDownload = $@"C:\FPTGetFile\Config\MaxTime_Cx_Download_{i}.csv";
            if (File.Exists(directoryDownload))
            {
                using (var reader = new StreamReader(directoryDownload))
                {
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        var values = line.Split(',');

                        var maxTime = values[1].ToString();

                        DateTime.TryParseExact(maxTime, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture,
                            System.Globalization.DateTimeStyles.None, out maxTimeCxDownload);
                    }
                }
            }

            return maxTimeCxDownload;
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

        private System.Timers.Timer timer = null;
        private void button3_Click(object sender, EventArgs e)
        {
            try
            {
                // Tạo 1 timer từ libary System.Timers
                timer = new System.Timers.Timer();
                // Execute mỗi 1 phút
                timer.Interval = 60000;
                // Những gì xảy ra khi timer đó dc tick
                timer.Elapsed += timer_Tick;
                // Enable timer
                timer.Enabled = true;

                //myWorker_GetFileCx.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }

        }
        class MyCounter_GetFilePos2Cx
        {
            public static int count = 0;
            public static Mutex MuTexLock = new Mutex();
        }
        private void timer_Tick(object sender, ElapsedEventArgs args)
        {
            if (args.SignalTime.Minute % 5 == 0)
            {
                log.InfoFormat("MyCounter_GetFilePos2Cx.count: {0}", MyCounter_GetFilePos2Cx.count);
                if (MyCounter_GetFilePos2Cx.count == 0)
                {
                    try
                    {
                        myWorker_GetFileCx.RunWorkerAsync();
                    }
                    catch (Exception e)
                    {
                        log.Error(String.Format("Can not run backgroud_worker: myWorker_GetFileCx!|{0}", e.Message));
                    }
                }
            }
        }

        private void button4_Click(object sender, EventArgs e)
        {
            try
            {
                myWorker_GetFile3rdParty_Azure.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void button5_Click(object sender, EventArgs e)
        {
            try
            {
                myWorker_PutFileCx_Pos.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void button6_Click(object sender, EventArgs e)
        {
            try
            {
                myWorker_PutFileCx_BI.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

    }
}
