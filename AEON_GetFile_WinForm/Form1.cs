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
        private BackgroundWorker myWorker_GetFileCxPRD = new BackgroundWorker();
        private BackgroundWorker myWorker_GetFile3rdParty = new BackgroundWorker();
        private BackgroundWorker myWorker_GetFile3rdParty_PRD = new BackgroundWorker();
        private BackgroundWorker myWorker_GetFile3rdParty_Azure = new BackgroundWorker();
        private BackgroundWorker myWorker_GetFile3rdParty_Azure_New = new BackgroundWorker();
        private BackgroundWorker myWorker_PutFileCx_Pos = new BackgroundWorker();
        private BackgroundWorker myWorker_PutFileCx_BI = new BackgroundWorker();

        private BackgroundWorker myWorker_GetFileCxPRD_new = new BackgroundWorker();
        private BackgroundWorker myWorker_PutFileCx_Pos_new = new BackgroundWorker();

        private string FileConfig = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig");
        private string DirectoryFrom = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom");
        private string FileConfig2 = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfig2");
        private string DirectoryFrom2 = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFrom2");
        private string FileConfigTransation = System.Configuration.ConfigurationManager.AppSettings.Get("FileConfigTransation");
        private string DirectoryFromTransation = System.Configuration.ConfigurationManager.AppSettings.Get("DirectoryFromTransation");
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
        private string AVNAzureHost = System.Configuration.ConfigurationManager.AppSettings.Get("AVNAzureHost");
        private string AVNAzurePort = System.Configuration.ConfigurationManager.AppSettings.Get("AVNAzurePort");
        private string AVNAzureUser = System.Configuration.ConfigurationManager.AppSettings.Get("AVNAzureUser");
        private string AVNAzurePwd = System.Configuration.ConfigurationManager.AppSettings.Get("AVNAzurePwd");
        private string Store_Directory = System.Configuration.ConfigurationManager.AppSettings.Get("Store_Directory");
        private string Store_Directory_Download = System.Configuration.ConfigurationManager.AppSettings.Get("Store_Directory_Download");


        public Form1()
        {
            InitializeComponent();

            //khai báo properties của background process
            myWorker_GetFileCx.DoWork += new DoWorkEventHandler(myWorker_GetFileCx_DoWork);
            myWorker_GetFileCx.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFileCx_RunWorkerCompleted);
            myWorker_GetFileCx.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFileCx_ProgressChanged);
            myWorker_GetFileCx.WorkerReportsProgress = true;
            myWorker_GetFileCx.WorkerSupportsCancellation = true;

            myWorker_GetFileCxPRD.DoWork += new DoWorkEventHandler(myWorker_GetFileCxPRD_DoWork);
            myWorker_GetFileCxPRD.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFileCxPRD_RunWorkerCompleted);
            myWorker_GetFileCxPRD.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFileCxPRD_ProgressChanged);
            myWorker_GetFileCxPRD.WorkerReportsProgress = true;
            myWorker_GetFileCxPRD.WorkerSupportsCancellation = true;

            myWorker_GetFileCxPRD_new.DoWork += new DoWorkEventHandler(myWorker_GetFileCxPRD_new_DoWork);
            myWorker_GetFileCxPRD_new.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFileCxPRD_new_RunWorkerCompleted);
            myWorker_GetFileCxPRD_new.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFileCxPRD_new_ProgressChanged);
            myWorker_GetFileCxPRD_new.WorkerReportsProgress = true;
            myWorker_GetFileCxPRD_new.WorkerSupportsCancellation = true;

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

            myWorker_GetFile3rdParty_Azure_New.DoWork += new DoWorkEventHandler(myWorker_GetFile3rdParty_Azure_New_DoWork);
            myWorker_GetFile3rdParty_Azure_New.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_GetFile3rdParty_Azure_New_RunWorkerCompleted);
            myWorker_GetFile3rdParty_Azure_New.ProgressChanged += new ProgressChangedEventHandler(myWorker_GetFile3rdParty_Azure_New_ProgressChanged);
            myWorker_GetFile3rdParty_Azure_New.WorkerReportsProgress = true;
            myWorker_GetFile3rdParty_Azure_New.WorkerSupportsCancellation = true;

            myWorker_PutFileCx_Pos.DoWork += new DoWorkEventHandler(myWorker_PutFileCx_Pos_DoWork);
            myWorker_PutFileCx_Pos.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_PutFileCx_Pos_RunWorkerCompleted);
            myWorker_PutFileCx_Pos.ProgressChanged += new ProgressChangedEventHandler(myWorker_PutFileCx_Pos_ProgressChanged);
            myWorker_PutFileCx_Pos.WorkerReportsProgress = true;
            myWorker_PutFileCx_Pos.WorkerSupportsCancellation = true;

            myWorker_PutFileCx_Pos_new.DoWork += new DoWorkEventHandler(myWorker_PutFileCx_Pos_new_DoWork);
            myWorker_PutFileCx_Pos_new.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_PutFileCx_Pos_new_RunWorkerCompleted);
            myWorker_PutFileCx_Pos_new.ProgressChanged += new ProgressChangedEventHandler(myWorker_PutFileCx_Pos_new_ProgressChanged);
            myWorker_PutFileCx_Pos_new.WorkerReportsProgress = true;
            myWorker_PutFileCx_Pos_new.WorkerSupportsCancellation = true;

            myWorker_PutFileCx_BI.DoWork += new DoWorkEventHandler(myWorker_PutFileCx_BI_DoWork);
            myWorker_PutFileCx_BI.RunWorkerCompleted += new RunWorkerCompletedEventHandler(myWorker_PutFileCx_BI_RunWorkerCompleted);
            myWorker_PutFileCx_BI.ProgressChanged += new ProgressChangedEventHandler(myWorker_PutFileCx_BI_ProgressChanged);
            myWorker_PutFileCx_BI.WorkerReportsProgress = true;
            myWorker_PutFileCx_BI.WorkerSupportsCancellation = true;
        }

        private void myWorker_GetFileCxPRD_new_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFileCxPRD_new_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFileCxPRD_new_DoWork(object sender, DoWorkEventArgs e)
        {
            //throw new NotImplementedException();
            try
            {
                List<string> store_list = new List<string>();
                //store_list.Add("1001");
                var directory = Store_Directory;
                if (File.Exists(directory))
                {
                    using (var reader = new StreamReader(directory))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (!string.IsNullOrEmpty(line))
                            {
                                store_list.Add(line);
                            }
                        }
                    }
                }
                else
                {
                    log.ErrorFormat("myWorker_GetFileCxPRD_DoWork - Can not find config file: {0}", directory);
                    return;
                }

                foreach (var store in store_list)
                {
                    var t = new Thread(() =>
                    {
                        GetFileVoucher(store);
                    });
                    t.Start();
                }
            }
            catch (Exception ex)
            {
                log.ErrorFormat("GetFile W Exception: {0}", ex.Message);
            }
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
                            using (Stream file1 = File.Create(@"E:\SAP\BO\Prod\" + remoteFileName))
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
                MyCounter_PutFileCx2Pos.MuTexLock.WaitOne();
                MyCounter_PutFileCx2Pos.count++;
                MyCounter_PutFileCx2Pos.MuTexLock.ReleaseMutex();

                var host = FPTHost;
                var port = Convert.ToInt32(FPTPort);
                var username = FPTUser;
                var password = FPTPwd;

                PutFileCx_Pos_PRD(host, port, username, password);

                log.Info("myWorker_PutFileCx_Pos_DoWork done!");
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_PutFileCx_Pos_DoWork - Exception: {0}", ex.Message);
            }
            finally
            {
                MyCounter_PutFileCx2Pos.MuTexLock.WaitOne();
                MyCounter_PutFileCx2Pos.count--;
                MyCounter_PutFileCx2Pos.MuTexLock.ReleaseMutex();
            }
        }

        private static void PutFileCx_Pos_PRD(string host, int port, string username, string password)
        {
            try
            {
                var task_Cx2Pos = Task.Run(() =>
                {
                    using (var client = new SftpClient(host, port, username, password))
                    {
                        client.Connect();
                        if (client.IsConnected)
                        {
                            log.Info("PutFileCx_Pos Connected to FPT Cloud");

                            var filesList = client.ListDirectory("/SAP_CX_PRD/Cx_Out/POS").OrderBy(file => file.Name);
                            foreach (var file in filesList)
                            {
                                var remoteFileName = file.Name;
                                if (!remoteFileName.StartsWith("backup") && remoteFileName.StartsWith("M"))
                                {
                                    var task = Task.Run(() =>
                                    {
                                        bool result = false;
                                        try
                                        {
                                            //download file
                                            using (Stream file1 = File.Create(@"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName))
                                            {
                                                client.DownloadFile($"/SAP_CX_PRD/Cx_Out/POS/{remoteFileName}", file1);
                                            }

                                            if (File.Exists(@"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName))
                                            {
                                                log.InfoFormat("PutFileCx_Pos: download file successfully: {0}",
                                                    @"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName);

                                                //move file backup on server
                                                var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                                if (!client.Exists($"/SAP_CX_PRD/Cx_Out/POS/backup/{dateNow}"))
                                                {
                                                    client.CreateDirectory($"/SAP_CX_PRD/Cx_Out/POS/backup/{dateNow}");
                                                }

                                                client.RenameFile($"/SAP_CX_PRD/Cx_Out/POS/{remoteFileName}",
                                                    $"/SAP_CX_PRD/Cx_Out/POS/backup/{dateNow}/{remoteFileName}");
                                                result = true;
                                            }
                                            else
                                            {
                                                log.ErrorFormat("PutFileCx_Pos: download file failed: {0}",
                                                    @"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName);
                                                result = false;
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            log.ErrorFormat("PutFileCx_Pos - M Exception: {0}", e.Message);
                                        }
                                        return result;
                                    });
                                    bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(300000));

                                    if (isCompletedSuccessfully)
                                    {
                                        log.InfoFormat("PutFileCx_Pos - M: Put file Cx to Pos susscessfully! result: {0}", task.Result);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("PutFileCx_Pos - M: The function has taken longer than the maximum time allowed.");
                                    }
                                }
                                else if (!remoteFileName.StartsWith("backup") && !remoteFileName.StartsWith("M"))
                                {
                                    var task = Task.Run(() =>
                                    {
                                        bool result = false;
                                        try
                                        {
                                            var storeFolder = remoteFileName.Substring(remoteFileName.Length - 5, 1) +
                                                              Path.GetExtension(remoteFileName).Substring(1, 3);
                                            //download file
                                            using (Stream file1 = File.Create($@"\\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}"))
                                            {
                                                client.DownloadFile($"/SAP_CX_PRD/Cx_Out/POS/{remoteFileName}", file1);
                                            }

                                            if (File.Exists($@"\\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}"))
                                            {
                                                log.InfoFormat($@"PutFileCx_Pos: download file successfully: \\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}");

                                                //move file backup on server
                                                var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                                if (!client.Exists($"/SAP_CX_PRD/Cx_Out/POS/backup/{dateNow}"))
                                                {
                                                    client.CreateDirectory($"/SAP_CX_PRD/Cx_Out/POS/backup/{dateNow}");
                                                }

                                                client.RenameFile($"/SAP_CX_PRD/Cx_Out/POS/{remoteFileName}",
                                                    $"/SAP_CX_PRD/Cx_Out/POS/backup/{dateNow}/{remoteFileName}");
                                                result = true;
                                            }
                                            else
                                            {
                                                log.ErrorFormat($@"PutFileCx_Pos: download file failed: \\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}");
                                                result = false;
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            log.ErrorFormat("PutFileCx_Pos - Other Exception: {0} - {1}", remoteFileName, e.Message);
                                        }
                                        return result;
                                    });
                                    bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));

                                    if (isCompletedSuccessfully)
                                    {
                                        log.InfoFormat("PutFileCx_Pos - Other: Put file Cx to Pos susscessfully! result: {0}", task.Result);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("PutFileCx_Pos - Other: The function has taken longer than the maximum time allowed.");
                                    }
                                }
                            }
                        }
                        else
                        {
                            log.Error("PutFileCx_Pos can not connected to FPT Cloud");
                        }
                    }
                });

                bool isCompletedSuccessfully_Cx2Pos = task_Cx2Pos.Wait(TimeSpan.FromMilliseconds(300000));
                if (!isCompletedSuccessfully_Cx2Pos)
                {
                    log.ErrorFormat("PutFileCx_Pos: The function has taken longer than the maximum time allowed.");
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("PutFileCx_Pos Exception: {0}", e.Message);
            }
        }

        private static void PutFileCx_Pos_UAT(string host, int port, string username, string password)
        {
            try
            {
                MyCounter_PutFileCx2Pos.MuTexLock.WaitOne();
                MyCounter_PutFileCx2Pos.count++;
                MyCounter_PutFileCx2Pos.MuTexLock.ReleaseMutex();

                using (var client = new SftpClient(host, port, username, password))
                {
                    client.Connect();
                    if (client.IsConnected)
                    {
                        log.Info("PutFileCx_Pos Connected to FPT Cloud");

                        var filesList = client.ListDirectory("/SAP_CX_PRD/Cx_Out/POS");
                        foreach (var file in filesList)
                        {
                            var remoteFileName = file.Name;
                            if (!remoteFileName.StartsWith("backup") && remoteFileName.StartsWith("M"))
                            {
                                var task = Task.Run(() =>
                                {
                                    bool result = false;
                                    try
                                    {
                                        //download file
                                        using (Stream file1 = File.Create(@"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName))
                                        {
                                            client.DownloadFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}", file1);
                                        }

                                        if (File.Exists(@"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName))
                                        {
                                            log.InfoFormat("PutFileCx_Pos: download file successfully: {0}",
                                                @"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName);

                                            //move file backup on server
                                            var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                            if (!client.Exists($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}"))
                                            {
                                                client.CreateDirectory($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}");
                                            }

                                            client.RenameFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}",
                                                $"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}/{remoteFileName}");
                                            result = true;
                                        }
                                        else
                                        {
                                            log.ErrorFormat("PutFileCx_Pos: download file failed: {0}",
                                                @"\\10.121.2.207\NFSUAT\vnmuat\download\oro2\" + remoteFileName);
                                            result = false;
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        log.ErrorFormat("", e.Message);
                                    }
                                    return result;
                                });
                                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(300000));

                                if (isCompletedSuccessfully)
                                {
                                    log.InfoFormat("PutFileCx_Pos - M: Put file Cx to Pos susscessfully! result: {0}", task.Result);
                                }
                                else
                                {
                                    log.ErrorFormat("PutFileCx_Pos - M: The function has taken longer than the maximum time allowed.");
                                }
                            }
                            else if (!remoteFileName.StartsWith("backup") && !remoteFileName.StartsWith("M"))
                            {
                                var task = Task.Run(() =>
                                {
                                    bool result = false;
                                    try
                                    {
                                        var storeFolder = remoteFileName.Substring(remoteFileName.Length - 5, 1) +
                                                          Path.GetExtension(remoteFileName).Substring(1, 3);
                                        //download file
                                        using (Stream file1 = File.Create(
                                            $@"\\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}"))
                                        {
                                            client.DownloadFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}", file1);
                                        }

                                        if (File.Exists(
                                            $@"\\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}"))
                                        {
                                            log.InfoFormat(
                                                $@"PutFileCx_Pos: download file successfully: \\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}");

                                            //move file backup on server
                                            var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                            if (!client.Exists($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}"))
                                            {
                                                client.CreateDirectory($"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}");
                                            }

                                            client.RenameFile($"/SAP_CX_UAT/Cx_Out/POS/{remoteFileName}",
                                                $"/SAP_CX_UAT/Cx_Out/POS/backup/{dateNow}/{remoteFileName}");
                                            result = true;
                                        }
                                        else
                                        {
                                            log.ErrorFormat(
                                                $@"PutFileCx_Pos: download file failed: \\10.121.2.207\NFSUAT\vnmuat\download\pos_test\{storeFolder}\{remoteFileName}");
                                            result = false;
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        log.ErrorFormat("", e.Message);
                                    }
                                    return result;
                                });
                                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(300000));

                                if (isCompletedSuccessfully)
                                {
                                    log.InfoFormat("PutFileCx_Pos - Other: Put file Cx to Pos susscessfully! result: {0}", task.Result);
                                }
                                else
                                {
                                    log.ErrorFormat("PutFileCx_Pos - Other: The function has taken longer than the maximum time allowed.");
                                }
                            }
                        }
                    }
                    else
                    {
                        log.Error("PutFileCx_Pos can not connected to FPT Cloud");
                    }
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("PutFileCx_Pos Exception: {0}", e.Message);
            }
            finally
            {
                MyCounter_PutFileCx2Pos.MuTexLock.WaitOne();
                MyCounter_PutFileCx2Pos.count--;
                MyCounter_PutFileCx2Pos.MuTexLock.ReleaseMutex();
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

                if (GetMaxTimePopTransaction(out var maxTimePopTransaction)) return;
                //check day before
                DownloadFilePopTransaction_DayBefore(GetFilesPathTransation_DayBefore(maxTimePopTransaction));
                //check day current
                DownloadFilePopTransaction_DayCurrent(GetFilesPathTransation_DayCurrent(maxTimePopTransaction));


                log.Info("UploadFile_POP3rdParty_master_PRD done!");
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_GetFile3rdParty_PRD_DoWork - Exception: {0}", ex.Message);
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

                DateTime max_time_pop_transaction = DateTime.MinValue;
                if (File.Exists(FileConfigTransation))
                {
                    using (var reader = new StreamReader(FileConfigTransation))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            var values = line.Split(',');

                            string max_time = values[1].ToString();
                            log.Info(max_time);
                            DateTime.TryParseExact(max_time, "yyyyMMddHHmmss", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out max_time_pop_transaction);
                        }
                    }
                }
                if (max_time_pop_transaction == DateTime.MinValue)
                {
                    log.Info(max_time_pop_transaction.ToString());
                    return;
                }
                log.Info(max_time_pop_transaction.ToString());
                TimeSpan duration_transaction = new TimeSpan(0, 0, 0, 1);
                DirectoryInfo info_transaction = new DirectoryInfo(string.Format(DirectoryFromTransation));
                List<string> filesPath_transaction = info_transaction.GetFiles("PO_*.csv")
                                                //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                                                .Where(x => x.LastWriteTime >= max_time_pop_transaction.Add(duration_transaction))
                                                .OrderByDescending(x => x.LastWriteTime)
                                              .Select(x => x.FullName)
                                              .ToList();
                if (filesPath_transaction.Count > 0)
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
                            log.Info("UploadFile_POP3rdParty_transaction Connected to FPT Cloud");

                            int maxtime_pos = 0;
                            foreach (string pathtg in filesPath_transaction)
                            {
                                string lastwritetime = File.GetLastWriteTime(pathtg).ToString("yyyyMMddHHmmss");
                                if (maxtime_pos == 0)
                                {
                                    log.Info(String.Format("last time pop transaction: {0}", lastwritetime));
                                    //string filename = string.Format("MaxTime_Pop.csv");
                                    //if (File.Exists(@"C:\FPTGetFile\Log\" + filename))
                                    //{
                                    //    File.Delete(@"C:\FPTGetFile\Log\" + filename);
                                    //}
                                    StreamWriter sw = new StreamWriter(FileConfigTransation, false, Encoding.Unicode);
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
                                        log.Info(string.Format("GetFilePOP3rdParty: UploadFile_transaction successfully: {0}", pathtg));
                                    }
                                    catch (Exception ex)
                                    {
                                        log.Error(string.Format("GetFilePOP3rdParty: UploadFile_transaction Exception: {0}", ex.Message));
                                    }
                                }
                            }
                        }
                        else
                        {
                            log.Error("UploadFile_POP3rdParty_transaction can not connected to FPT Cloud");
                        }
                    }
                    log.Info("UploadFile_POP3rdParty_transaction done!");
                }
                else
                {
                    log.Info(string.Format("UploadFile_POP3rdParty: service get no file from {0}!", DirectoryFromTransation));
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
                    var t = new Thread(() =>
                    {
                        UploadFile_Cx(i);
                    });
                    t.Name = i;
                    t.Start();
                }
                Thread.Sleep(270000);
                log.InfoFormat("myWorker_GetFileCx_DoWork - Sleep done, number thread: {0}", MyCounter_GetFilePos2Cx.count);
                if (MyCounter_GetFilePos2Cx.count > 0)
                {
                    while (Thread.CurrentThread.Name != null)
                    {
                        log.ErrorFormat("myWorker_GetFileCx_DoWork - kill thread: {0}", Thread.CurrentThread.Name);
                        Thread.CurrentThread.Abort();
                    }
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

                var task = Task.Run(() =>
                {
                    List<string> filesPathUpload_temp = new List<string>();
                    try
                    {
                        var infoUpload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\upload\pos\{i}\backup\");
                        //var infoUpload = new DirectoryInfo($@"C:\NFS\production\vnm\upload\pos\{i}\backup\");
                        var duration = new TimeSpan(0, 0, 0, 1);
                        filesPathUpload_temp = infoUpload.GetFiles("*.*")
                            //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                            .Where(x => (x.LastWriteTime >= maxTimeCxUpload.Add(duration)) &&
                                        (Path.GetFileName(x.FullName).Substring(0, 1) == "S" ||
                                         Path.GetFileName(x.FullName).Substring(0, 1) == "W"))
                            .OrderByDescending(x => x.LastWriteTime)
                            .Select(x => x.FullName)
                            .ToList();
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("UploadFile_Cx - Upload - Store {0}: {1}", i, e.Message);
                    }
                    return filesPathUpload_temp;
                });
                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(90000));
                if (isCompletedSuccessfully)
                {
                    var filesPathUpload = task.Result;
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
                else
                {
                    log.ErrorFormat("UploadFile_Cx - Upload - Store {0}: The function has taken longer than the maximum time allowed.", i);
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
                var task = Task.Run(() =>
                {
                    List<string> filesPathDownload_temp = new List<string>();
                    try
                    {
                        var infoDownload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\download\pos\{i}\backup\{DateTime.Now:yyyy}\{DateTime.Now:MM}\{DateTime.Now:dd}\");
                        //var infoDownload = new DirectoryInfo($@"C:\NFS\production\vnm\download\pos\{i}\backup\{DateTime.Now:yyyy}\{DateTime.Now:MM}\{DateTime.Now:dd}\");

                        var duration = new TimeSpan(0, 0, 0, 1);
                        filesPathDownload_temp = infoDownload.GetFiles("*.*")
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
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("UploadFile_Cx - Download_Day_Current - Store {0}: {1}", i, e.Message);
                    }
                    return filesPathDownload_temp;
                });

                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(90000));
                if (isCompletedSuccessfully)
                {
                    var filesPathDownload = task.Result;
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
                else
                {
                    log.ErrorFormat("UploadFile_Cx - Download_Day_Current - Store {0}: The function has taken longer than the maximum time allowed.", i);
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
                    day = DateTime.Now.AddDays(-1).ToString("dd");
                }

                var task = Task.Run(() =>
                {
                    List<string> filesPathDownload_temp = new List<string>();
                    try
                    {
                        var infoDownload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\download\pos\{i}\backup\{year}\{month}\{day}\");
                        //var infoDownload = new DirectoryInfo($@"C:\NFS\production\vnm\download\pos\{i}\backup\{year}\{month}\{day}\");
                        var duration = new TimeSpan(0, 0, 0, 1);
                        filesPathDownload_temp = infoDownload.GetFiles("*.*")
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
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("UploadFile_Cx - Download_Day_Current - Store {0}: {1}", i, e.Message);
                    }

                    return filesPathDownload_temp;
                });

                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(90000));
                if (isCompletedSuccessfully)
                {
                    var filesPathDownload = task.Result;
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
                else
                {
                    log.ErrorFormat("UploadFile_Cx - Download_Day_Before - Store {0}: The function has taken longer than the maximum time allowed.", i);
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
        class MyCounter_PutFileCx2Pos
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

            log.InfoFormat("MyCounter_PutFileCx2Pos.count: {0}", MyCounter_PutFileCx2Pos.count);
            if (MyCounter_PutFileCx2Pos.count == 0)
            {
                try
                {
                    myWorker_PutFileCx_Pos.RunWorkerAsync();
                }
                catch (Exception ex)
                {
                    log.Error(String.Format("Can not run backgroud_worker: myWorker_PutFileCx_Pos!|{0}", ex.Message));
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

        private System.Timers.Timer timer_CxPRD2Pos = null;
        private void button5_Click(object sender, EventArgs e)
        {
            try
            {
                // Tạo 1 timer từ libary System.Timers
                timer_CxPRD2Pos = new System.Timers.Timer();
                // Execute mỗi 1 phút
                timer_CxPRD2Pos.Interval = 60000;
                // Những gì xảy ra khi timer đó dc tick
                timer_CxPRD2Pos.Elapsed += timer_CxPRD2Pos_Tick;
                // Enable timer
                timer_CxPRD2Pos.Enabled = true;

                //myWorker_PutFileCx_Pos.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
        private void timer_CxPRD2Pos_Tick(object sender, ElapsedEventArgs args)
        {
            //if (args.SignalTime.Minute % 5 == 3)
            {
                log.InfoFormat("MyCounter_PutFileCx2Pos.count: {0}", MyCounter_PutFileCx2Pos.count);
                if (MyCounter_PutFileCx2Pos.count == 0)
                {
                    try
                    {
                        myWorker_PutFileCx_Pos.RunWorkerAsync();
                    }
                    catch (Exception e)
                    {
                        log.Error(String.Format("Can not run backgroud_worker: myWorker_PutFileCx_Pos!|{0}", e.Message));
                    }
                }
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

        private void button8_Click(object sender, EventArgs e)
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
        private System.Timers.Timer timer_CxPRD = null;
        private void button7_Click(object sender, EventArgs e)
        {
            //if (MyCounter_GetFilePos2CxPRD.count == 0)
            //{
            //    try
            //    {
            //        myWorker_GetFileCxPRD.RunWorkerAsync();
            //    }
            //    catch (Exception ex)
            //    {
            //        log.Error(String.Format("Can not run backgroud_worker: myWorker_GetFileCxPRD!|{0}", ex.Message));
            //    }
            //}
            try
            {
                // Tạo 1 timer từ libary System.Timers
                timer_CxPRD = new System.Timers.Timer();
                // Execute mỗi 1 phút
                timer_CxPRD.Interval = 60000;
                // Những gì xảy ra khi timer đó dc tick
                timer_CxPRD.Elapsed += timer_CxPRD_Tick;
                // Enable timer
                timer_CxPRD.Enabled = true;

                //myWorker_GetFileCx.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
        class MyCounter_GetFilePos2CxPRD
        {
            public static int count = 0;
            public static Mutex MuTexLock = new Mutex();
        }
        private void timer_CxPRD_Tick(object sender, ElapsedEventArgs args)
        {
            if (args.SignalTime.Minute % 5 == 3)
            {
                log.InfoFormat("MyCounter_GetFilePos2CxPRD.count: {0}", MyCounter_GetFilePos2CxPRD.count);
                if (MyCounter_GetFilePos2CxPRD.count == 0)
                {
                    try
                    {
                        myWorker_GetFileCxPRD.RunWorkerAsync();
                    }
                    catch (Exception e)
                    {
                        log.Error(String.Format("Can not run backgroud_worker: myWorker_GetFileCxPRD!|{0}", e.Message));
                    }
                }
            }
        }
        private void myWorker_GetFileCxPRD_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFileCxPRD_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_GetFileCxPRD_RunWorkerCompleted");
        }

        private void myWorker_GetFileCxPRD_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFileCxPRD_DoWork");
                List<string> store_list = new List<string>();
                var directory = Store_Directory;
                if (File.Exists(directory))
                {
                    using (var reader = new StreamReader(directory))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (!string.IsNullOrEmpty(line))
                            {
                                store_list.Add(line);
                            }
                        }
                    }
                }
                else
                {
                    log.ErrorFormat("myWorker_GetFileCxPRD_DoWork - Can not find config file: {0}", directory);
                    return;
                }

                ////var store = new string[] { "1001", "1002", "1003", "1004", "1005", "1006", "1008", "3002", "3003", "3005", "3008", "3011", "3013", "3014", "3015", "3016", "3018", "3099"
                ////    , "5101", "5102", "5103", "5104", "5105", "5106", "5107", "5108", "5109", "5171", "5172", "5173", "5174", "5175", "5176", "5199", "5201", "5202", "5401", "5501", "5502"
                ////    , "5503", "5599", "5701", "5702", "5703", "5704", "5801", "5802", "5803", "5804", "5805", "5871", "5872", "5873", "5874", "5875", "5876", "5899", "5901", "5902", "5999" };
                //var store_list = new string[] { "1001" };
                foreach (var i in store_list)
                {
                    var t = new Thread(() =>
                    {
                        UploadFile_Cx_PRD(i);
                    });
                    t.Name = i;
                    t.Start();
                }
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_GetFileCxPRD_DoWork - Exception: {0}", ex.Message);
            }
        }
        private void UploadFile_Cx_PRD(string i)
        {
            try
            {
                MyCounter_GetFilePos2CxPRD.MuTexLock.WaitOne();
                MyCounter_GetFilePos2CxPRD.count++;
                log.InfoFormat("UploadFile_CxPRD Store: {0}, MuTexLock++: {1}", i, MyCounter_GetFilePos2CxPRD.count);
                MyCounter_GetFilePos2CxPRD.MuTexLock.ReleaseMutex();

                var host = AVNAzureHost;
                var port = Convert.ToInt32(AVNAzurePort);
                var username = AVNAzureUser;
                var password = AVNAzurePwd;

                #region old

                var maxTimeCxDownload = GetMaxTimeCxPRDDownload(i);
                if (maxTimeCxDownload != DateTime.MinValue)
                {
                    log.InfoFormat("max time download store: {0}, {1}", i, maxTimeCxDownload.ToString(CultureInfo.InvariantCulture));

                    DownloadFileCxPRD_DayBefore(i, maxTimeCxDownload, host, port, username, password);

                    DownloadFileCxPRD_DayCurrent(i, maxTimeCxDownload, host, port, username, password);
                }

                //get file from dir upload
                var maxTimeCxUpload = GetMaxTimeCxPRDUpload(i);
                if (maxTimeCxUpload != DateTime.MinValue)
                {
                    //download file from folder Upload in Middle Server
                    DownloadFileCxPRD_Upload(i, maxTimeCxUpload, host, port, username, password);
                }

                #endregion
                log.Info("UploadFile_Cx_PRD done! Store: " + i);
            }
            catch (Exception ex)
            {
                log.ErrorFormat("UploadFile_Cx_PRD Thread Store: {0}, Exception: {1}", i, ex.Message);
            }
            finally
            {
                MyCounter_GetFilePos2CxPRD.MuTexLock.WaitOne();
                MyCounter_GetFilePos2CxPRD.count--;
                log.InfoFormat("UploadFile_CxPRD Store: {0}, MuTexLock--: {1}", i, MyCounter_GetFilePos2CxPRD.count);
                MyCounter_GetFilePos2CxPRD.MuTexLock.ReleaseMutex();
            }
        }
        private static DateTime GetMaxTimeCxPRDDownload(string i)
        {
            var maxTimeCxDownload = DateTime.MinValue;
            var directoryDownload = $@"C:\FPTGetFile\Config\MaxTime_Cx_Download_{i}_Winform.csv";
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
        private static DateTime GetMaxTimeCxPRDUpload(string i)
        {
            var maxTimeCxUpload = DateTime.MinValue;
            var directoryUpload = $@"C:\FPTGetFile\Config\MaxTime_Cx_Upload_{i}_Winform.csv";
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
        private static void DownloadFileCxPRD_DayCurrent(string i, DateTime maxTimeCxDownload, string host, int port, string username, string password)
        {
            try
            {
                var task = Task.Run(() =>
                {
                    List<string> filesPathDownload_temp = new List<string>();
                    try
                    {
                        var infoDownload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\download\pos\{i}\backup\{DateTime.Now:yyyy}\{DateTime.Now:MM}\{DateTime.Now:dd}\");
                        //var infoDownload = new DirectoryInfo($@"C:\NFS\production\vnm\download\pos\{i}\backup\{DateTime.Now:yyyy}\{DateTime.Now:MM}\{DateTime.Now:dd}\");

                        var duration = new TimeSpan(0, 0, 0, 1);
                        filesPathDownload_temp = infoDownload.GetFiles("*.*")
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
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("UploadFile_Cx - Download_Day_Current - Store {0}: {1}", i, e.Message);
                    }
                    return filesPathDownload_temp;
                });

                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(90000));
                if (isCompletedSuccessfully)
                {
                    var filesPathDownload = task.Result;
                    if (filesPathDownload.Count > 0)
                    {
                        var task_upload = Task.Run(() =>
                        {
                            using (var client = new SftpClient(host, port, username, password))
                            {
                                client.Connect();
                                if (client.IsConnected)
                                {
                                    log.Info("UploadFile_Cx - Download_Day_Current: Connected to FPT Cloud, Store: " + i);

                                    //var maxtimeDownload = 0;
                                    string file_path = "";
                                    string file_time = "";
                                    foreach (var path in filesPathDownload)
                                    {
                                        //if (maxtimeDownload == 0)
                                        //{
                                        //    log.InfoFormat("last time download store {0}: {1:yyyyMMddHHmmss}", i, File.GetLastWriteTime(path));
                                        //    var filename = $"MaxTime_Cx_Download_{i}_PRD.csv";
                                        //    var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                        //        Encoding.Unicode);
                                        //    sw.Write(path + ",");
                                        //    sw.Write(File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss"));
                                        //    sw.WriteLine();
                                        //    sw.Close();
                                        //}

                                        //maxtimeDownload++;
                                        file_path = path;
                                        file_time = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                                        using (var fileStream = new FileStream(path, FileMode.Open))
                                        {

                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                            client.ChangeDirectory("/SAP_CX_PRD/" + i);
                                            client.UploadFile(fileStream, Path.GetFileName(path));
                                            log.InfoFormat("UploadFile_Cx - Download_Day_Current: UploadFile successfully store: {0}, {1}", i, path);
                                        }
                                    }
                                    {
                                        log.InfoFormat("last time download store {0}: {1}", i, file_time);
                                        var filename = $"MaxTime_Cx_Download_{i}_Winform.csv";

                                        var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                            Encoding.Unicode);
                                        sw.Write(file_path + ",");
                                        sw.Write(file_time);
                                        sw.WriteLine();
                                        sw.Close();
                                    }
                                    client.Disconnect();
                                }
                                else
                                {
                                    log.Error("UploadFile_Cx - Download_Day_Current: Can not connected to AEON Azure, Store: " + i);
                                }
                            }
                        });

                        bool isCompletedSuccessfully_upload = task_upload.Wait(TimeSpan.FromMilliseconds(90000));
                        if (isCompletedSuccessfully_upload)
                        {
                            log.InfoFormat("UploadFile_Cx - Download_Day_Current: Store: {0}, File count: {1}, upload done", i, filesPathDownload.Count);
                        }
                        else
                        {
                            log.ErrorFormat("UploadFile_Cx - Download_Day_Current - Store {0}: The function upload has taken longer than the maximum time allowed.", i);
                        }
                    }
                    else
                    {
                        log.InfoFormat("UploadFile_Cx - Download_Day_Current: Store: {0}, File count: {1}", i, filesPathDownload.Count);
                    }
                }
                else
                {
                    log.ErrorFormat("UploadFile_Cx - Download_Day_Current - Store {0}: The function has taken longer than the maximum time allowed.", i);
                }
            }
            catch (Exception ex)
            {
                log.InfoFormat("UploadFile_Cx - Download_Day_Current - Store {0}: Exception: {1}", i, ex.Message);
            }

        }

        private static void DownloadFileCxPRD_DayBefore(string i, DateTime maxTimeCxDownload, string host, int port, string username, string password)
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
                    day = DateTime.Now.AddDays(-1).ToString("dd");
                }

                var task = Task.Run(() =>
                {
                    List<string> filesPathDownload_temp = new List<string>();
                    try
                    {
                        var infoDownload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\download\pos\{i}\backup\{year}\{month}\{day}\");
                        //var infoDownload = new DirectoryInfo($@"C:\NFS\production\vnm\download\pos\{i}\backup\{year}\{month}\{day}\");
                        var duration = new TimeSpan(0, 0, 0, 1);
                        filesPathDownload_temp = infoDownload.GetFiles("*.*")
                            //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                            .Where(x => (x.LastWriteTime >= maxTimeCxDownload.Add(duration)) &&
                                        (Path.GetFileName(x.FullName).Substring(0, 1) == "D"
                                         || Path.GetFileName(x.FullName).Substring(0, 1) == "T" ||
                                         Path.GetFileName(x.FullName).Substring(0, 1) == "A"
                                         || Path.GetFileName(x.FullName).Substring(0, 1) == "C" ||
                                         Path.GetFileName(x.FullName).Substring(0, 1) == "I"))
                            //.OrderByDescending(x => x.LastWriteTime)
                            .OrderBy(x => x.LastWriteTime)
                            .Select(x => x.FullName)
                            .ToList();
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("UploadFile_Cx - Download_Day_Before - Store {0}: {1}", i, e.Message);
                    }

                    return filesPathDownload_temp;
                });

                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(90000));
                if (isCompletedSuccessfully)
                {
                    var filesPathDownload = task.Result;
                    if (filesPathDownload.Count > 0)
                    {
                        var task_upload = Task.Run(() =>
                        {
                            using (var client = new SftpClient(host, port, username, password))
                            {
                                client.Connect();
                                if (client.IsConnected)
                                {
                                    log.Info("UploadFile_Cx - Download_Day_Before: Connected to AEON Azure, Store: " + i);

                                    //var maxtimeDownload = 0;
                                    string file_path = "";
                                    string file_time = "";
                                    foreach (var path in filesPathDownload)
                                    {
                                        //if (maxtimeDownload == 0)
                                        //{
                                        //    log.InfoFormat("last time download store {0}: {1:yyyyMMddHHmmss}", i, File.GetLastWriteTime(path));
                                        //    var filename = $"MaxTime_Cx_Download_{i}_PRD.csv";

                                        //    var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                        //        Encoding.Unicode);
                                        //    sw.Write(path + ",");
                                        //    sw.Write(File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss"));
                                        //    sw.WriteLine();
                                        //    sw.Close();
                                        //}

                                        //maxtimeDownload++;
                                        file_path = path;
                                        file_time = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                                        using (var fileStream = new FileStream(path, FileMode.Open))
                                        {

                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                            client.ChangeDirectory("/SAP_CX_PRD/" + i);
                                            client.UploadFile(fileStream, Path.GetFileName(path));
                                            log.InfoFormat("UploadFile_Cx - Download_Day_Before: UploadFile successfully store: {0}, {1}", i, path);
                                        }
                                    }
                                    {
                                        log.InfoFormat("last time download store {0}: {1}", i, file_time);
                                        var filename = $"MaxTime_Cx_Download_{i}_Winform.csv";

                                        var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                            Encoding.Unicode);
                                        sw.Write(file_path + ",");
                                        sw.Write(file_time);
                                        sw.WriteLine();
                                        sw.Close();
                                    }
                                    client.Disconnect();
                                }
                                else
                                {
                                    log.Error("UploadFile_Cx - Download_Day_Before: Can not connected to AEON Azure, Store: " + i);
                                }
                            }
                        });
                        bool isCompletedSuccessfully_upload = task_upload.Wait(TimeSpan.FromMilliseconds(90000));
                        if (isCompletedSuccessfully_upload)
                        {
                            log.InfoFormat("UploadFile_Cx - Download_Day_Before: Store: {0}, File count: {1}, upload done", i, filesPathDownload.Count);
                        }
                        else
                        {
                            log.ErrorFormat("UploadFile_Cx - Download_Day_Before - Store {0}: The function upload has taken longer than the maximum time allowed.", i);
                        }
                    }
                    else
                    {
                        log.InfoFormat("UploadFile_Cx - Download_Day_Before: Store: {0}, File count: {1}", i, filesPathDownload.Count);
                    }
                }
                else
                {
                    log.ErrorFormat("UploadFile_Cx - Download_Day_Before - Store {0}: The function has taken longer than the maximum time allowed.", i);
                }
            }
            catch (Exception ex)
            {
                log.InfoFormat("UploadFile_Cx - Download_Day_Before - Store {0}: Exception: {1}", i, ex.Message);
            }

        }
        private static void DownloadFileCxPRD_Upload(string i, DateTime maxTimeCxUpload, string host, int port, string username, string password)
        {
            try
            {
                log.InfoFormat("max time upload store: {0}, {1}", i, maxTimeCxUpload.ToString(CultureInfo.InvariantCulture));
                var task = Task.Run(() =>
                {
                    List<string> filesPathUpload_temp = new List<string>();
                    try
                    {
                        var infoUpload = new DirectoryInfo($@"\\10.121.2.207\NFS\production\vnm\upload\pos\{i}\backup\");
                        //var infoUpload = new DirectoryInfo($@"C:\profit\vnm\upload\pos\{i}\backup\");
                        var duration = new TimeSpan(0, 0, 0, 1);
                        filesPathUpload_temp = infoUpload.GetFiles("*.*")
                            //.Where(x => x.LastWriteTime.Date.Day == 3 && x.LastWriteTime.Date.Month == 3)
                            .Where(x => (x.LastWriteTime >= maxTimeCxUpload.Add(duration) && x.LastWriteTime <= DateTime.Now.AddDays(-2)) &&
                                        (Path.GetFileName(x.FullName).Substring(0, 1) == "S" ||
                                         Path.GetFileName(x.FullName).Substring(0, 1) == "W"))
                            .OrderByDescending(x => x.LastWriteTime)
                            .Select(x => x.FullName)
                            .ToList();
                    }
                    catch (Exception e)
                    {
                        log.ErrorFormat("UploadFile_Cx_PRD - Upload - Store {0}: {1}", i, e.Message);
                    }
                    return filesPathUpload_temp;
                });
                bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(600000));
                if (isCompletedSuccessfully)
                {
                    var filesPathUpload = task.Result;
                    if (filesPathUpload.Count > 0)
                    {
                        var task_upload = Task.Run(() =>
                        {
                            using (var client = new SftpClient(host, port, username, password))
                            {
                                client.Connect();
                                if (client.IsConnected)
                                {
                                    log.Info("UploadFile_Cx_PRD - Upload: Connected to FPT Cloud, Store: " + i);

                                    //var maxtimeUpload = 0;
                                    string file_path = "";
                                    string file_time = "";
                                    foreach (var path in filesPathUpload)
                                    {
                                        //if (maxtimeUpload == 0)
                                        //{
                                        //    log.InfoFormat("last time upload store {0}: {1:yyyyMMddHHmmss}", i, File.GetLastWriteTime(path));
                                        //    var filename = $"MaxTime_Cx_Upload_{i}_PRD.csv";

                                        //    var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                        //        Encoding.Unicode);
                                        //    sw.Write(path + ",");
                                        //    sw.Write(File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss"));
                                        //    sw.WriteLine();
                                        //    sw.Close();
                                        //}

                                        //maxtimeUpload++;
                                        file_path = path;
                                        file_time = File.GetLastWriteTime(path).ToString("yyyyMMddHHmmss");
                                        using (var fileStream = new FileStream(path, FileMode.Open))
                                        {

                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                            client.ChangeDirectory("/SAP_CX_PRD/" + i);
                                            client.UploadFile(fileStream, Path.GetFileName(path));
                                            log.InfoFormat("UploadFile_Cx_PRD - Upload: UploadFile successfully: {0}", path);
                                        }
                                    }
                                    {
                                        log.InfoFormat("last time upload store {0}: {1}", i, file_time);
                                        var filename = $"MaxTime_Cx_Upload_{i}_Winform.csv";

                                        var sw = new StreamWriter(string.Format(@"C:\FPTGetFile\Config\" + filename), false,
                                            Encoding.Unicode);
                                        sw.Write(file_path + ",");
                                        sw.Write(file_time);
                                        sw.WriteLine();
                                        sw.Close();
                                    }
                                    client.Disconnect();
                                }
                                else
                                {
                                    log.Error("UploadFile_Cx_PRD - Upload: Can not connected to FPT Cloud, Store: " + i);
                                }
                            }
                        });
                        bool isCompletedSuccessfully_upload = task_upload.Wait(TimeSpan.FromMilliseconds(300000));
                        if (isCompletedSuccessfully_upload)
                        {
                            log.InfoFormat("UploadFile_Cx - Upload: Store: {0}, File count: {1}, upload done", i, filesPathUpload.Count);
                        }
                        else
                        {
                            log.ErrorFormat("UploadFile_Cx - Upload - Store {0}: The function upload has taken longer than the maximum time allowed.", i);
                        }
                    }
                    else
                    {
                        log.InfoFormat("UploadFile_Cx_PRD - Upload: Store: {0}, File count: {1}", i, filesPathUpload.Count);
                    }
                }
                else
                {
                    log.ErrorFormat("UploadFile_Cx_PRD - Upload - Store {0}: The function has taken longer than the maximum time allowed.", i);
                }
            }
            catch (Exception ex)
            {
                log.InfoFormat("UploadFile_Cx_PRD - Upload - Store {0}: Exception: {1}", i, ex.Message);
            }
        }


        private System.Timers.Timer timer_Pop = null;
        private void button8_Click_1(object sender, EventArgs e)
        {
            try
            {
                // Tạo 1 timer từ libary System.Timers
                timer_Pop = new System.Timers.Timer();
                // Execute mỗi 1 phút
                timer_Pop.Interval = 60000;
                // Những gì xảy ra khi timer đó dc tick
                timer_Pop.Elapsed += timer_Pop_Tick;
                // Enable timer
                timer_Pop.Enabled = true;

                //myWorker_GetFileCx.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
        private void timer_Pop_Tick(object sender, ElapsedEventArgs args)
        {
            if (args.SignalTime.Minute % 5 == 0)
            {
                try
                {
                    myWorker_GetFile3rdParty_Azure_New.RunWorkerAsync();
                }
                catch (Exception e)
                {
                    log.Error(String.Format("Can not run backgroud_worker: myWorker_GetFile3rdParty_Azure_New!|{0}", e.Message));
                }
            }
        }

        private void myWorker_GetFile3rdParty_Azure_New_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_GetFile3rdParty_Azure_New_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_GetFile3rdParty_Azure_New_RunWorkerCompleted");
        }

        private void myWorker_GetFile3rdParty_Azure_New_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_GetFile3rdParty_Azure_New_DoWork");

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

                log.Info("myWorker_GetFile3rdParty_Azure_New_DoWork done!");
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_GetFile3rdParty_Azure_New_DoWork - Exception: {0}", ex.Message);
            }
        }

        private void button9_Click(object sender, EventArgs e)
        {
            try
            {

                List<string> store_list = new List<string>();
                var directory = Store_Directory;
                if (File.Exists(directory))
                {
                    using (var reader = new StreamReader(directory))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (!string.IsNullOrEmpty(line))
                            {
                                store_list.Add(line);
                            }
                        }
                    }
                }
                else
                {
                    log.ErrorFormat("myWorker_GetFileCxPRD_DoWork - Can not find config file: {0}", directory);
                    return;
                }


                var host = AVNAzureHost;
                var port = Convert.ToInt32(AVNAzurePort);
                var username = AVNAzureUser;
                var password = AVNAzurePwd;


                using (var client = new SftpClient(host, port, username, password))
                {
                    client.Connect();
                    if (client.IsConnected)
                    {
                        log.Info("Get1File - Upload: Connected to FPT Cloud");
                        foreach (var store in store_list)
                        {
                            string filename = "";
                            string month = "";
                            int month_temp = DateTime.Now.AddDays(-1).Month;
                            if (month_temp == 10)
                            {
                                month = "A";
                            }
                            else if (month_temp == 11)
                            {
                                month = "B";
                            }
                            else if (month_temp == 12)
                            {
                                month = "C";
                            }
                            else
                            {
                                month = month_temp.ToString();
                            }

                            string file_name = string.Format("S{0}{1}{2}00{3}", DateTime.Now.AddDays(-1).Year.ToString().Substring(3, 1), month, DateTime.Now.AddDays(-1).ToString("dd"), store.Substring(0, 1) + "." + store.Substring(1, 3));

                            string path = string.Format(@"\\10.121.2.207\NFS\production\vnm\upload\pos\{0}\backup\{1}", store, file_name);
                            try
                            {
                                using (var fileStream = new FileStream(path, FileMode.Open))
                                {
                                    client.BufferSize = 4 * 1024; // bypass Payload error large files
                                    client.ChangeDirectory("/SAP_CX_PRD/" + store);
                                    client.UploadFile(fileStream, Path.GetFileName(path));
                                    log.InfoFormat("Get1File - Upload: UploadFile successfully: {0}", path);
                                }
                            }
                            catch (Exception exx)
                            {
                                log.ErrorFormat("Get1File Upload, Exception: {0}", exx.Message);
                            }
                        }
                        client.Disconnect();
                    }
                    else
                    {
                        log.Error("Get1File - Upload: Can not connected to FPT Cloud");
                    }
                }
            }
            catch (Exception ex)
            {
                log.ErrorFormat("Get1File, Exception: {0}", ex.Message);
            }
        }

        private void button10_Click(object sender, EventArgs e)
        {
            try
            {

            }
            catch (Exception ex)
            {
                log.ErrorFormat("PostFileCx2Pos, Exception: {0}", ex.Message);
            }
        }

        private System.Timers.Timer timer_Pos_Cx = null;

        private void button10_Click_1(object sender, EventArgs e)
        {
            try
            {
                // Tạo 1 timer từ libary System.Timers
                timer_Pos_Cx = new System.Timers.Timer();
                // Execute mỗi 1 phút
                timer_Pos_Cx.Interval = 60000;
                // Những gì xảy ra khi timer đó dc tick
                timer_Pos_Cx.Elapsed += timer_Pos2Cx_Tick;
                // Enable timer
                timer_Pos_Cx.Enabled = true;

                //myWorker_PutFileCx_Pos.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
        private void timer_Pos2Cx_Tick(object sender, ElapsedEventArgs args)
        {
            if (args.SignalTime.Minute % 5 == 0)
            {
                log.InfoFormat("MyCounter_GetFilePos2CxPRD.count: {0}", MyCounter_GetFilePos2CxPRD.count);
                if (MyCounter_GetFilePos2CxPRD.count == 0)
                {
                    try
                    {
                        myWorker_GetFileCxPRD_new.RunWorkerAsync();
                    }
                    catch (Exception e)
                    {
                        log.Error(String.Format("Can not run backgroud_worker: myWorker_GetFileCxPRD_new!|{0}", e.Message));
                    }
                }
            }
        }


        private void GetFileVoucher(string store)
        {
            try
            {
                MyCounter_GetFilePos2CxPRD.MuTexLock.WaitOne();
                MyCounter_GetFilePos2CxPRD.count++;
                MyCounter_GetFilePos2CxPRD.MuTexLock.ReleaseMutex();

                List<string> rule_rs = new List<string>();
                string[] R1 = new string[]
                {
                    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F",
                    "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
                    "W", "X", "Y", "Z"
                };
                string[] R2 = new string[]
                {
                    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"
                };
                foreach (var R1_tmp in R1)
                {
                    foreach (var R2_tmp in R2)
                    {
                        rule_rs.Add(R1_tmp + R2_tmp);
                    }
                }

                var host = AVNAzureHost;
                var port = Convert.ToInt32(AVNAzurePort);
                var username = AVNAzureUser;
                var password = AVNAzurePwd;
                string CxtoPOS_folder_out = @"/SAP_CX_PRD/";

                using (var client = new SftpClient(host, port, username, password))
                {
                    client.Connect();
                    if (client.IsConnected)
                    {
                        log.InfoFormat("GetFile Upload Store {0}: Connected to FPT Cloud", store);
                        var task = Task.Run(() =>
                        {
                            try
                            {
                                //file W date before
                                foreach (var rs in rule_rs)
                                {
                                    if (rs != "00")
                                    {
                                        string filename = "";
                                        string month = "";
                                        int month_temp = DateTime.Now.AddDays(-1).Month;
                                        if (month_temp == 10)
                                        {
                                            month = "A";
                                        }
                                        else if (month_temp == 11)
                                        {
                                            month = "B";
                                        }
                                        else if (month_temp == 12)
                                        {
                                            month = "C";
                                        }
                                        else
                                        {
                                            month = month_temp.ToString();
                                        }

                                        filename = string.Format("W{0}{1}{2}{3}{4}",
                                                        DateTime.Now.AddDays(-1).Year.ToString().Substring(3, 1), month,
                                                        DateTime.Now.AddDays(-1).ToString("dd"), rs,
                                                        store.Substring(0, 1) + "." + store.Substring(1, 3));
                                        if (!client.Exists(CxtoPOS_folder_out + store + @"/" + filename))
                                        {
                                            if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" +
                                                                           DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + filename))
                                            {
                                                if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" +
                                                                               DateTime.Now.ToString("yyyyMMdd") + @"/" + filename))
                                                {
                                                    string path = string.Format(
                                                                    @"\\10.121.2.207\NFS\production\vnm\upload\pos\{0}\backup\{1}", store,
                                                                    filename);
                                                    try
                                                    {
                                                        using (var fileStream = new FileStream(path, FileMode.Open))
                                                        {
                                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                            client.ChangeDirectory("/SAP_CX_PRD/" + store);
                                                            client.UploadFile(fileStream, Path.GetFileName(path));
                                                            log.InfoFormat("GetFile W - Upload: UploadFile successfully: {0}",
                                                                            path);
                                                        }
                                                    }
                                                    catch (Exception exx)
                                                    {
                                                        //stop_flag = true;
                                                        log.ErrorFormat("GetFile W - Upload Store {0}: Exception: {1}", store,
                                                            exx.Message);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                log.InfoFormat("GetFile W - Upload Store {0}: Check file date before done!", store);
                                //file W date current
                                foreach (var rs in rule_rs)
                                {
                                    if (rs != "00")
                                    {
                                        string filename = "";
                                        string month = "";
                                        int month_temp = DateTime.Now.Month;
                                        if (month_temp == 10)
                                        {
                                            month = "A";
                                        }
                                        else if (month_temp == 11)
                                        {
                                            month = "B";
                                        }
                                        else if (month_temp == 12)
                                        {
                                            month = "C";
                                        }
                                        else
                                        {
                                            month = month_temp.ToString();
                                        }

                                        filename = string.Format("W{0}{1}{2}{3}{4}", DateTime.Now.Year.ToString().Substring(3, 1),
                                                        month, DateTime.Now.ToString("dd"), rs,
                                                        store.Substring(0, 1) + "." + store.Substring(1, 3));
                                        if (!client.Exists(CxtoPOS_folder_out + store + @"/" + filename))
                                        {
                                            if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" +
                                                                           DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + filename))
                                            {
                                                if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" +
                                                                               DateTime.Now.ToString("yyyyMMdd") + @"/" + filename))
                                                {
                                                    string path = string.Format(
                                                                    @"\\10.121.2.207\NFS\production\vnm\upload\pos\{0}\backup\{1}", store,
                                                                    filename);
                                                    try
                                                    {
                                                        using (var fileStream = new FileStream(path, FileMode.Open))
                                                        {
                                                            client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                            client.ChangeDirectory("/SAP_CX_PRD/" + store);
                                                            client.UploadFile(fileStream, Path.GetFileName(path));
                                                            log.InfoFormat("GetFile W - Upload: UploadFile successfully: {0}",
                                                                            path);
                                                        }
                                                    }
                                                    catch (Exception exx)
                                                    {
                                                        //stop_flag = true;
                                                        log.ErrorFormat("GetFile W - Upload Store {0}: Exception: {1}", store,
                                                            exx.Message);
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                log.InfoFormat("GetFile W - Upload Store {0}: Check file date current done!", store);

                                //file S date before
                                {
                                    string month = "";
                                    int month_temp = DateTime.Now.AddDays(-1).Month;
                                    if (month_temp == 10)
                                    {
                                        month = "A";
                                    }
                                    else if (month_temp == 11)
                                    {
                                        month = "B";
                                    }
                                    else if (month_temp == 12)
                                    {
                                        month = "C";
                                    }
                                    else
                                    {
                                        month = month_temp.ToString();
                                    }

                                    string file_name = string.Format("S{0}{1}{2}00{3}",
                                                    DateTime.Now.AddDays(-1).Year.ToString().Substring(3, 1), month,
                                                    DateTime.Now.AddDays(-1).ToString("dd"),
                                                    store.Substring(0, 1) + "." + store.Substring(1, 3));
                                    if (!client.Exists(CxtoPOS_folder_out + store + @"/" + file_name))
                                    {
                                        if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" + DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + file_name))
                                        {
                                            if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" + DateTime.Now.ToString("yyyyMMdd") + @"/" + file_name))
                                            {
                                                string path = string.Format(
                                                                @"\\10.121.2.207\NFS\production\vnm\upload\pos\{0}\backup\{1}", store,
                                                                file_name);
                                                try
                                                {
                                                    using (var fileStream = new FileStream(path, FileMode.Open))
                                                    {
                                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                        client.ChangeDirectory("/SAP_CX_PRD/" + store);
                                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                                        log.InfoFormat("GetFile S - Upload: UploadFile successfully: {0}", path);
                                                    }
                                                }
                                                catch (Exception exx)
                                                {
                                                    log.ErrorFormat("GetFile S - Upload Store {0}, Exception: {1}", store, exx.Message);
                                                }
                                            }
                                            else
                                            {
                                                log.InfoFormat("GetFile S - Upload Store {0} - {1}: file exist in folder backup today!", store, file_name);
                                            }
                                        }
                                        else
                                        {
                                            log.InfoFormat("GetFile S - Upload Store {0} - {1}: file exist in folder backup before!", store, file_name);
                                        }
                                    }
                                }
                                log.InfoFormat("GetFile S - Upload Store {0}: Check file date before done!", store);

                                //file S date current
                                {
                                    string month = "";
                                    int month_temp = DateTime.Now.Month;
                                    if (month_temp == 10)
                                    {
                                        month = "A";
                                    }
                                    else if (month_temp == 11)
                                    {
                                        month = "B";
                                    }
                                    else if (month_temp == 12)
                                    {
                                        month = "C";
                                    }
                                    else
                                    {
                                        month = month_temp.ToString();
                                    }

                                    string file_name = string.Format("S{0}{1}{2}00{3}",
                                                    DateTime.Now.Year.ToString().Substring(3, 1), month, DateTime.Now.ToString("dd"),
                                                    store.Substring(0, 1) + "." + store.Substring(1, 3));
                                    if (!client.Exists(CxtoPOS_folder_out + store + @"/" + file_name))
                                    {
                                        if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" +
                                                                       DateTime.Now.AddDays(-1).ToString("yyyyMMdd") + @"/" + file_name))
                                        {
                                            if (!client.Exists(CxtoPOS_folder_out + store + @"/backup/" +
                                                                           DateTime.Now.ToString("yyyyMMdd") + @"/" + file_name))
                                            {
                                                string path = string.Format(
                                                                @"\\10.121.2.207\NFS\production\vnm\upload\pos\{0}\backup\{1}", store,
                                                                file_name);
                                                try
                                                {
                                                    using (var fileStream = new FileStream(path, FileMode.Open))
                                                    {
                                                        client.BufferSize = 4 * 1024; // bypass Payload error large files
                                                        client.ChangeDirectory("/SAP_CX_PRD/" + store);
                                                        client.UploadFile(fileStream, Path.GetFileName(path));
                                                        log.InfoFormat("Get1File - Upload: UploadFile successfully: {0}", path);
                                                    }
                                                }
                                                catch (Exception exx)
                                                {
                                                    log.ErrorFormat("Get1File Upload Store {0}, Exception: {1}", store,
                                                                    exx.Message);
                                                }
                                            }
                                            else
                                            {
                                                log.InfoFormat("GetFile S - Upload Store {0} - {1}: file exist in folder backup today!", store, file_name);
                                            }
                                        }
                                        else
                                        {
                                            log.InfoFormat("GetFile S - Upload Store {0} - {1}: file exist in folder backup before!", store, file_name);
                                        }
                                    }
                                }
                                log.InfoFormat("GetFile S - Upload Store {0}: Check file date current done!", store);
                            }
                            catch (Exception e)
                            {
                                log.ErrorFormat("GetFile_new_Store {0} Exception: {1}", store, e.Message);
                            }
                        });
                        bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));
                        if (isCompletedSuccessfully)
                        {
                            log.InfoFormat("GetFile_new Store {0} successfully!", store);
                        }
                        else
                        {
                            log.ErrorFormat("GetFile_new: The function has taken longer than the maximum time allowed.");
                        }
                        client.Disconnect();
                    }
                    else
                    {
                        log.Error("GetFile W - Upload: Can not connected to FPT Cloud");
                    }
                }
            }
            catch (Exception ex)
            {
                log.ErrorFormat("GetFile_new Store {0} Exception: {1}", store, ex.Message);
            }
            finally
            {
                MyCounter_GetFilePos2CxPRD.MuTexLock.WaitOne();
                MyCounter_GetFilePos2CxPRD.count--;
                MyCounter_GetFilePos2CxPRD.MuTexLock.ReleaseMutex();
            }
        }

        private void button11_Click(object sender, EventArgs e)
        {
            try
            {
                // Tạo 1 timer từ libary System.Timers
                timer_Pos_Cx = new System.Timers.Timer();
                // Execute mỗi 1 phút
                timer_Pos_Cx.Interval = 60000;
                // Những gì xảy ra khi timer đó dc tick
                timer_Pos_Cx.Elapsed += timer_Cx2Pos_new_Tick;
                // Enable timer
                timer_Pos_Cx.Enabled = true;

                //myWorker_PutFileCx_Pos.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                log.ErrorFormat("Put file Cx PRD to Pos Exception: {0}", ex.Message);
            }
        }
        private void timer_Cx2Pos_new_Tick(object sender, ElapsedEventArgs args)
        {
            //if (args.SignalTime.Minute % 5 == 0)
            {
                log.InfoFormat("MyCounter_PutFileCx2Pos_new.count: {0}", MyCounter_PutFileCx2Pos_new.count);
                if (MyCounter_PutFileCx2Pos_new.count == 0)
                {
                    try
                    {
                        myWorker_PutFileCx_Pos_new.RunWorkerAsync();
                    }
                    catch (Exception e)
                    {
                        log.Error(String.Format("Can not run backgroud_worker: myWorker_PutFileCx_Pos_new!|{0}", e.Message));
                    }
                }
            }
        }
        private void myWorker_PutFileCx_Pos_new_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            //throw new NotImplementedException();
        }

        private void myWorker_PutFileCx_Pos_new_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            log.Info("myWorker_PutFileCx_Pos_new_RunWorkerCompleted");
        }
        class MyCounter_PutFileCx2Pos_new
        {
            public static int count = 0;
            public static Mutex MuTexLock = new Mutex();
        }
        private void myWorker_PutFileCx_Pos_new_DoWork(object sender, DoWorkEventArgs e)
        {
            try
            {
                log.Info("myWorker_PutFileCx_Pos_DoWork");

                List<string> store_list = new List<string>();
                //store_list.Add("1001");
                var directory = Store_Directory_Download;
                if (File.Exists(directory))
                {
                    using (var reader = new StreamReader(directory))
                    {
                        while (!reader.EndOfStream)
                        {
                            var line = reader.ReadLine();
                            if (!string.IsNullOrEmpty(line))
                            {
                                store_list.Add(line);
                            }
                        }
                    }
                }
                else
                {
                    log.ErrorFormat("myWorker_GetFileCxPRD_DoWork - Can not find config file: {0}", directory);
                    return;
                }

                foreach (var store in store_list)
                {
                    var t = new Thread(() =>
                    {
                        PutFileCx_Pos_PRD_new(store);
                    });
                    t.Start();
                }
            }
            catch (Exception ex)
            {
                log.ErrorFormat("myWorker_PutFileCx_Pos_DoWork Exception: {0}", ex.Message);
            }
        }

        private void PutFileCx_Pos_PRD_new(string store)
        {
            try
            {
                MyCounter_PutFileCx2Pos_new.MuTexLock.WaitOne();
                MyCounter_PutFileCx2Pos_new.count++;
                MyCounter_PutFileCx2Pos_new.MuTexLock.ReleaseMutex();

                var host = FPTHost;
                var port = Convert.ToInt32(FPTPort);
                var username = FPTUser;
                var password = FPTPwd;
                using (var client = new SftpClient(host, port, username, password))
                {
                    client.Connect();
                    if (client.IsConnected)
                    {
                        log.Info("PutFileCx_Pos Connected to AVN Azure");
                        var task_Cx2Pos = Task.Run(() =>
                        {
                            if (store == "Member")
                            {
                                var filesList = client.ListDirectory("/SAP_CX_PRD/Cx_Out/POS/Member").OrderBy(file => file.Name);
                                foreach (var file in filesList)
                                {
                                    var remoteFileName = file.Name;
                                    var task = Task.Run(() =>
                                    {
                                        bool result = false;
                                        try
                                        {
                                            //download file
                                            using (Stream file1 = File.Create(@"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName))
                                            {
                                                client.DownloadFile($"/SAP_CX_PRD/Cx_Out/POS/Member/{remoteFileName}", file1);
                                            }

                                            if (File.Exists(@"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName))
                                            {
                                                log.InfoFormat("PutFileCx_Pos: download file successfully: {0}",
                                                    @"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName);

                                                //move file backup on server
                                                var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                                if (!client.Exists($"/SAP_CX_PRD/Cx_Out/POS/Member/backup/{dateNow}"))
                                                {
                                                    client.CreateDirectory($"/SAP_CX_PRD/Cx_Out/POS/Member/backup/{dateNow}");
                                                }

                                                client.RenameFile($"/SAP_CX_PRD/Cx_Out/POS/Member/{remoteFileName}",
                                                    $"/SAP_CX_PRD/Cx_Out/POS/Member/backup/{dateNow}/{remoteFileName}");
                                                result = true;
                                            }
                                            else
                                            {
                                                log.ErrorFormat("PutFileCx_Pos: download file failed: {0}",
                                                    @"\\10.121.2.207\NFS\production\vnm\download\oro2\" + remoteFileName);
                                                result = false;
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            log.ErrorFormat("PutFileCx_Pos - M Exception: {0}", e.Message);
                                        }
                                        return result;
                                    });
                                    bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));

                                    if (isCompletedSuccessfully)
                                    {
                                        log.InfoFormat("PutFileCx_Pos - M: Put file Cx to Pos susscessfully! result: {0}", task.Result);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("PutFileCx_Pos - M: The function has taken longer than the maximum time allowed.");
                                    }
                                }    
                            }
                            else
                            {
                                var filesList = client.ListDirectory(string.Format("/SAP_CX_PRD/Cx_Out/POS/{0}", store)).OrderBy(file => file.Name);
                                foreach (var file in filesList)
                                {
                                    var remoteFileName = file.Name;
                                    var task = Task.Run(() =>
                                    {
                                        bool result = false;
                                        try
                                        {
                                            var storeFolder = remoteFileName.Substring(remoteFileName.Length - 5, 1) + Path.GetExtension(remoteFileName).Substring(1, 3);
                                            //download file
                                            using (Stream file1 = File.Create($@"\\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}"))
                                            {
                                                client.DownloadFile($"/SAP_CX_PRD/Cx_Out/POS/{store}/{remoteFileName}", file1);
                                            }

                                            if (File.Exists($@"\\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}"))
                                            {
                                                log.InfoFormat($@"PutFileCx_Pos: download file successfully: \\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}");

                                                //move file backup on server
                                                var dateNow = DateTime.Now.ToString("yyyyMMdd");
                                                if (!client.Exists($"/SAP_CX_PRD/Cx_Out/POS/{store}/backup/{dateNow}"))
                                                {
                                                    client.CreateDirectory($"/SAP_CX_PRD/Cx_Out/POS/{store}/backup/{dateNow}");
                                                }

                                                client.RenameFile($"/SAP_CX_PRD/Cx_Out/POS/{store}/{remoteFileName}",
                                                    $"/SAP_CX_PRD/Cx_Out/POS/{store}/backup/{dateNow}/{remoteFileName}");
                                                result = true;
                                            }
                                            else
                                            {
                                                log.ErrorFormat($@"PutFileCx_Pos: download file failed: \\10.121.2.207\NFS\production\vnm\download\pos\{storeFolder}\{remoteFileName}");
                                                result = false;
                                            }
                                        }
                                        catch (Exception e)
                                        {
                                            log.ErrorFormat("PutFileCx_Pos - Other Exception: {0} - {1}", remoteFileName, e.Message);
                                        }
                                        return result;
                                    });
                                    bool isCompletedSuccessfully = task.Wait(TimeSpan.FromMilliseconds(240000));

                                    if (isCompletedSuccessfully)
                                    {
                                        log.InfoFormat("PutFileCx_Pos - Other: Put file Cx to Pos susscessfully! result: {0}", task.Result);
                                    }
                                    else
                                    {
                                        log.ErrorFormat("PutFileCx_Pos - Other: The function has taken longer than the maximum time allowed.");
                                    }
                                }    
                            }
                        });

                        bool isCompletedSuccessfully_Cx2Pos = task_Cx2Pos.Wait(TimeSpan.FromMilliseconds(300000));
                        if (!isCompletedSuccessfully_Cx2Pos)
                        {
                            log.ErrorFormat("PutFileCx_Pos: The function has taken longer than the maximum time allowed.");
                        }
                        client.Disconnect();
                    }
                    else
                    {
                        log.Error("PutFileCx_Pos can not connected to FPT Cloud");
                    }
                }

            }
            catch (Exception ex)
            {
                log.ErrorFormat("PutFileCx_Pos_PRD_new Exception: {0}", ex.Message);
            }
            finally
            {
                MyCounter_PutFileCx2Pos_new.MuTexLock.WaitOne();
                MyCounter_PutFileCx2Pos_new.count--;
                MyCounter_PutFileCx2Pos_new.MuTexLock.ReleaseMutex();
            }
        }
    }
}
