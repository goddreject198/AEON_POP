﻿using Renci.SshNet;
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
                DirectoryInfo info = new DirectoryInfo(string.Format(@"C:\NFS\production\vnm\download\fpt_bi\pop_system\"));
                List<string> filesPath = info.GetFiles("*.csv").Where(x => x.LastWriteTime.Date == DateTime.Today.AddDays(0)
                                                                                        && x.LastWriteTime.Hour == DateTime.Now.Hour
                                                                                        && (x.LastWriteTime.Minute == minute_now
                                                                                         || x.LastWriteTime.Minute == minute_now - 1
                                                                                         || x.LastWriteTime.Minute == minute_now - 2
                                                                                         || x.LastWriteTime.Minute == minute_now - 3
                                                                                         || x.LastWriteTime.Minute == minute_now - 4))
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
                        log.Info("UploadFile_POP3rdParty Connected to FPT Cloud");

                        foreach (string pathtg in filesPath)
                        {
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
                DirectoryInfo info_download = new DirectoryInfo(string.Format(@"C:\NFS\production\vnm\download\pos\{0}\backup\{1}\{2}\{3}\", i, DateTime.Now.ToString("yyyy"), DateTime.Now.ToString("MM"), DateTime.Now.ToString("dd")));
                List<string> filesPath_download = info_download.GetFiles("*.*").Where(x => x.LastWriteTime.Hour == DateTime.Now.Hour
                                                                                        && (x.LastWriteTime.Minute == minute_now
                                                                                         || x.LastWriteTime.Minute == minute_now - 1
                                                                                         || x.LastWriteTime.Minute == minute_now - 2
                                                                                         || x.LastWriteTime.Minute == minute_now - 3
                                                                                         || x.LastWriteTime.Minute == minute_now - 4))
                                              .Select(x => x.FullName)
                                              .ToList();

                DirectoryInfo info_upload = new DirectoryInfo(string.Format(@"C:\NFS\production\vnm\upload\pos\{0}\backup\", i));
                List<string> filesPath_upload = info_upload.GetFiles("*.*").Where(x => x.LastWriteTime.Date == DateTime.Today.AddDays(0) 
                                                                                        && x.LastWriteTime.Hour == DateTime.Now.Hour 
                                                                                        && (x.LastWriteTime.Minute == minute_now
                                                                                         || x.LastWriteTime.Minute == minute_now - 1
                                                                                         || x.LastWriteTime.Minute == minute_now - 2
                                                                                         || x.LastWriteTime.Minute == minute_now - 3
                                                                                         || x.LastWriteTime.Minute == minute_now - 4))
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

                        foreach (string pathtg in filesPath_download)
                        {
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
                        foreach (string pathtg in filesPath_upload)
                        {
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

                myWorker_GetFileCx.RunWorkerAsync();
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
    }
}