using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using Horizon_EOBS_Parse;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Globalization;
using System.Data.OleDb;
using System.Data.SqlClient;
using Microsoft.VisualBasic;
using System.Configuration;

namespace WindowsForm
    {
    public partial class Test_Development : Form
        {
        DBUtility dbU;
        public Test_Development()
            {
            InitializeComponent();

            }
       
        private void Test_Development_Load(object sender, EventArgs e)
            {
            ProcessVars.gTest = true;
            appSets appsets = new appSets();
            appsets.setVars();
            pd.Text = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            fd.Text = GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd");
            label1.Text = ProcessVars.InputDirectory;
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\AbilTo");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\from_FTP");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\fromCass");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Decrypted");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\HNJH\Chams");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\HNJH\WK");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\HNJH\DSNP_WK");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards\ID_Cards_Omnia");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards\ID_Cards_Reg");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards\ID_Cards_Test");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Renewals");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Errors");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\w_Process");
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            }

        private void button2_Click(object sender, EventArgs e)
            {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Downloading Files for Ticket 02 ..   TEST  .";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            string extractPath = ProcessVars.InputDirectory + "From_FTP";
            string ResultsPdf = "";
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            NParse_pdfs parse_pdfs = new NParse_pdfs();
            downloadDta.MoveFilesFrom_VLTrader();

            Results.Text = "Download files  for Ticket 02 ready   TEST";
           
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();

            }

        private void Test_Development_FormClosed(object sender, FormClosedEventArgs e)
            {
            System.Windows.Forms.Application.Exit();
            }

        private void button3_Click(object sender, EventArgs e)
            {
            string verRuning = "";
            if (ProcessVars.gTest)
                verRuning = "TEST";

            Process.Start(ProcessVars.InputDirectory + @"\from_FTP");
            Process.Start(@"\\criticalapps\Horizon\fromVLTrader" + verRuning);
            }
        }
    }
