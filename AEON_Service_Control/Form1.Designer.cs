
namespace AEON_Service_Control
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.groupBox3 = new System.Windows.Forms.GroupBox();
            this.groupBox4 = new System.Windows.Forms.GroupBox();
            this.CxUATStop = new System.Windows.Forms.Button();
            this.CxUATStart = new System.Windows.Forms.Button();
            this.POPUATStart = new System.Windows.Forms.Button();
            this.POPUATStop = new System.Windows.Forms.Button();
            this.CxPRDStart = new System.Windows.Forms.Button();
            this.CxPRDStop = new System.Windows.Forms.Button();
            this.POPPRDStart = new System.Windows.Forms.Button();
            this.POPPRDStop = new System.Windows.Forms.Button();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.groupBox3.SuspendLayout();
            this.groupBox4.SuspendLayout();
            this.SuspendLayout();
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.CxUATStart);
            this.groupBox1.Controls.Add(this.CxUATStop);
            this.groupBox1.Location = new System.Drawing.Point(12, 12);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(200, 104);
            this.groupBox1.TabIndex = 2;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "AEON_Cx_GetFileService_UAT";
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.CxPRDStart);
            this.groupBox2.Controls.Add(this.CxPRDStop);
            this.groupBox2.Location = new System.Drawing.Point(12, 122);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(200, 105);
            this.groupBox2.TabIndex = 3;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "AEON_Cx_GetFileService_PRD";
            // 
            // groupBox3
            // 
            this.groupBox3.Controls.Add(this.POPUATStart);
            this.groupBox3.Controls.Add(this.POPUATStop);
            this.groupBox3.Location = new System.Drawing.Point(218, 12);
            this.groupBox3.Name = "groupBox3";
            this.groupBox3.Size = new System.Drawing.Size(242, 104);
            this.groupBox3.TabIndex = 4;
            this.groupBox3.TabStop = false;
            this.groupBox3.Text = "AEON_POP_3rdParty_GetFileService";
            // 
            // groupBox4
            // 
            this.groupBox4.Controls.Add(this.POPPRDStart);
            this.groupBox4.Controls.Add(this.POPPRDStop);
            this.groupBox4.Location = new System.Drawing.Point(218, 122);
            this.groupBox4.Name = "groupBox4";
            this.groupBox4.Size = new System.Drawing.Size(242, 105);
            this.groupBox4.TabIndex = 5;
            this.groupBox4.TabStop = false;
            this.groupBox4.Text = "AEON_POP_3rdParty_GetFileService_PRD";
            // 
            // CxUATStop
            // 
            this.CxUATStop.Location = new System.Drawing.Point(6, 19);
            this.CxUATStop.Name = "CxUATStop";
            this.CxUATStop.Size = new System.Drawing.Size(188, 36);
            this.CxUATStop.TabIndex = 0;
            this.CxUATStop.Text = "Stop Service";
            this.CxUATStop.UseVisualStyleBackColor = true;
            this.CxUATStop.Click += new System.EventHandler(this.CxUATStop_Click);
            // 
            // CxUATStart
            // 
            this.CxUATStart.Location = new System.Drawing.Point(6, 61);
            this.CxUATStart.Name = "CxUATStart";
            this.CxUATStart.Size = new System.Drawing.Size(188, 36);
            this.CxUATStart.TabIndex = 1;
            this.CxUATStart.Text = "Start Service";
            this.CxUATStart.UseVisualStyleBackColor = true;
            this.CxUATStart.Click += new System.EventHandler(this.CxUATStart_Click);
            // 
            // POPUATStart
            // 
            this.POPUATStart.Location = new System.Drawing.Point(6, 61);
            this.POPUATStart.Name = "POPUATStart";
            this.POPUATStart.Size = new System.Drawing.Size(230, 36);
            this.POPUATStart.TabIndex = 3;
            this.POPUATStart.Text = "Start Service";
            this.POPUATStart.UseVisualStyleBackColor = true;
            this.POPUATStart.Click += new System.EventHandler(this.POPUATStart_Click);
            // 
            // POPUATStop
            // 
            this.POPUATStop.Location = new System.Drawing.Point(6, 19);
            this.POPUATStop.Name = "POPUATStop";
            this.POPUATStop.Size = new System.Drawing.Size(230, 36);
            this.POPUATStop.TabIndex = 2;
            this.POPUATStop.Text = "Stop Service";
            this.POPUATStop.UseVisualStyleBackColor = true;
            this.POPUATStop.Click += new System.EventHandler(this.POPUATStop_Click);
            // 
            // CxPRDStart
            // 
            this.CxPRDStart.Location = new System.Drawing.Point(6, 61);
            this.CxPRDStart.Name = "CxPRDStart";
            this.CxPRDStart.Size = new System.Drawing.Size(188, 36);
            this.CxPRDStart.TabIndex = 3;
            this.CxPRDStart.Text = "Start Service";
            this.CxPRDStart.UseVisualStyleBackColor = true;
            this.CxPRDStart.Click += new System.EventHandler(this.CxPRDStart_Click);
            // 
            // CxPRDStop
            // 
            this.CxPRDStop.Location = new System.Drawing.Point(6, 19);
            this.CxPRDStop.Name = "CxPRDStop";
            this.CxPRDStop.Size = new System.Drawing.Size(188, 36);
            this.CxPRDStop.TabIndex = 2;
            this.CxPRDStop.Text = "Stop Service";
            this.CxPRDStop.UseVisualStyleBackColor = true;
            this.CxPRDStop.Click += new System.EventHandler(this.CxPRDStop_Click);
            // 
            // POPPRDStart
            // 
            this.POPPRDStart.Location = new System.Drawing.Point(6, 61);
            this.POPPRDStart.Name = "POPPRDStart";
            this.POPPRDStart.Size = new System.Drawing.Size(230, 36);
            this.POPPRDStart.TabIndex = 5;
            this.POPPRDStart.Text = "Start Service";
            this.POPPRDStart.UseVisualStyleBackColor = true;
            this.POPPRDStart.Click += new System.EventHandler(this.POPPRDStart_Click);
            // 
            // POPPRDStop
            // 
            this.POPPRDStop.Location = new System.Drawing.Point(6, 19);
            this.POPPRDStop.Name = "POPPRDStop";
            this.POPPRDStop.Size = new System.Drawing.Size(230, 36);
            this.POPPRDStop.TabIndex = 4;
            this.POPPRDStop.Text = "Stop Service";
            this.POPPRDStop.UseVisualStyleBackColor = true;
            this.POPPRDStop.Click += new System.EventHandler(this.POPPRDStop_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(471, 237);
            this.Controls.Add(this.groupBox4);
            this.Controls.Add(this.groupBox3);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox1);
            this.Name = "Form1";
            this.Text = "AEON_Services_Control";
            this.groupBox1.ResumeLayout(false);
            this.groupBox2.ResumeLayout(false);
            this.groupBox3.ResumeLayout(false);
            this.groupBox4.ResumeLayout(false);
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Button CxUATStart;
        private System.Windows.Forms.Button CxUATStop;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.Button CxPRDStart;
        private System.Windows.Forms.Button CxPRDStop;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.Button POPUATStart;
        private System.Windows.Forms.Button POPUATStop;
        private System.Windows.Forms.GroupBox groupBox4;
        private System.Windows.Forms.Button POPPRDStart;
        private System.Windows.Forms.Button POPPRDStop;
    }
}

