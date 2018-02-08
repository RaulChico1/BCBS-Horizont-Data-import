namespace WindowsForm
    {
    partial class Test_Development
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
            this.button1 = new System.Windows.Forms.Button();
            this.label1 = new System.Windows.Forms.Label();
            this.fd = new System.Windows.Forms.Label();
            this.pd = new System.Windows.Forms.Label();
            this.button2 = new System.Windows.Forms.Button();
            this.Results = new System.Windows.Forms.Label();
            this.button3 = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(23, 31);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(156, 35);
            this.button1.TabIndex = 0;
            this.button1.Text = "Move files VLTrader";
            this.button1.UseVisualStyleBackColor = true;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(252, 31);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(35, 13);
            this.label1.TabIndex = 1;
            this.label1.Text = "label1";
            // 
            // fd
            // 
            this.fd.AutoSize = true;
            this.fd.Location = new System.Drawing.Point(441, 9);
            this.fd.Name = "fd";
            this.fd.Size = new System.Drawing.Size(19, 13);
            this.fd.TabIndex = 2;
            this.fd.Text = "pd";
            // 
            // pd
            // 
            this.pd.AutoSize = true;
            this.pd.Location = new System.Drawing.Point(252, 9);
            this.pd.Name = "pd";
            this.pd.Size = new System.Drawing.Size(19, 13);
            this.pd.TabIndex = 3;
            this.pd.Text = "pd";
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(12, 96);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(181, 66);
            this.button2.TabIndex = 4;
            this.button2.Text = "Test Download files from FMTP";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // Results
            // 
            this.Results.AutoSize = true;
            this.Results.Location = new System.Drawing.Point(252, 67);
            this.Results.Name = "Results";
            this.Results.Size = new System.Drawing.Size(35, 13);
            this.Results.TabIndex = 5;
            this.Results.Text = "label2";
            // 
            // button3
            // 
            this.button3.Location = new System.Drawing.Point(828, 635);
            this.button3.Name = "button3";
            this.button3.Size = new System.Drawing.Size(75, 23);
            this.button3.TabIndex = 6;
            this.button3.Text = "Open drives";
            this.button3.UseVisualStyleBackColor = true;
            this.button3.Click += new System.EventHandler(this.button3_Click);
            // 
            // Test_Development
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(970, 706);
            this.Controls.Add(this.button3);
            this.Controls.Add(this.Results);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.pd);
            this.Controls.Add(this.fd);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.button1);
            this.Name = "Test_Development";
            this.Text = "Test_Development";
            this.FormClosed += new System.Windows.Forms.FormClosedEventHandler(this.Test_Development_FormClosed);
            this.Load += new System.EventHandler(this.Test_Development_Load);
            this.ResumeLayout(false);
            this.PerformLayout();

            }

        #endregion

        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label fd;
        private System.Windows.Forms.Label pd;
        private System.Windows.Forms.Button button2;
        private System.Windows.Forms.Label Results;
        private System.Windows.Forms.Button button3;
        }
    }