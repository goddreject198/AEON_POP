using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.ServiceProcess;

namespace AEON_Service_Control
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        public static void StartService(string serviceName, int timeoutMilliseconds)
        {
            ServiceController service = new ServiceController(serviceName);
            string svcStatus = service.Status.ToString();
            try
            {
                TimeSpan timeout = TimeSpan.FromMilliseconds(timeoutMilliseconds);

                service.Start();
                service.WaitForStatus(ServiceControllerStatus.Running, timeout);
            }
            catch (Exception e)
            {
                MessageBox.Show(e.ToString());
            }
        }

        //stop services
        public static void StopService(string serviceName, int timeoutMilliseconds)
        {
            ServiceController service = new ServiceController(serviceName);
            try
            {
                TimeSpan timeout = TimeSpan.FromMilliseconds(timeoutMilliseconds);

                service.Stop();
                service.WaitForStatus(ServiceControllerStatus.Stopped, timeout);
            }
            catch (Exception e)
            {
                MessageBox.Show(e.ToString());
            }
        }

        private void CxUATStop_Click(object sender, EventArgs e)
        {
            try
            {
                StopService("AEON_Cx_GetFileService_UAT", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void CxUATStart_Click(object sender, EventArgs e)
        {
            try
            {
                StartService("AEON_Cx_GetFileService_UAT", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void CxPRDStop_Click(object sender, EventArgs e)
        {
            try
            {
                StopService("AEON_Cx_GetFileService_PRD", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void CxPRDStart_Click(object sender, EventArgs e)
        {
            try
            {
                StartService("AEON_Cx_GetFileService_PRD", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void POPUATStop_Click(object sender, EventArgs e)
        {
            try
            {
                StopService("AEON_POP_3rdParty_GetFileService", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void POPUATStart_Click(object sender, EventArgs e)
        {
            try
            {
                StartService("AEON_POP_3rdParty_GetFileService", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void POPPRDStop_Click(object sender, EventArgs e)
        {
            try
            {
                StopService("AEON_POP_3rdParty_GetFileService_PRD", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void POPPRDStart_Click(object sender, EventArgs e)
        {
            try
            {
                StartService("AEON_POP_3rdParty_GetFileService_PRD", 60000);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }
    }
}
