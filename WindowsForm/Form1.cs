using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;
using Horizon_EOBS_Parse;
namespace WindowsForm
{
    public partial class Form1 : Form
    {
        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            System.Windows.Forms.Application.Exit();
        }
        public Form1()
        {
            InitializeComponent();
            //t.Tick += new EventHandler(t_Tick);
            //t.Interval = 500;
        }
        int timeElapsed = 0;
        System.Windows.Forms.Timer t = new System.Windows.Forms.Timer();

        private void button1_Click(object sender, EventArgs e)
        {
            //t.Start();
            //ThreadPool.QueueUserWorkItem((x) =>
            //{
            //    TimeConsumingFunction();
            //});

            System.Diagnostics.Stopwatch watch = new Stopwatch();
            watch.Start();
            TimeConsumingFunction();
            watch.Stop();
            string label = watch.Elapsed.ToString();
            string strlabel1 = watch.Elapsed.TotalSeconds.ToString() + "  seconds";
            label1.Text = strlabel1;

        }
        void TimeConsumingFunction()
        {
            appSets appsets = new appSets();
            appsets.setVars();
            ParseWorker processFiles = new ParseWorker();
            string result = processFiles.ProcessFiles(DateTime.Now.ToShortDateString());
            //label1.Text = "";
            //t.Stop();
            if (result == "")
                result = "Done " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");

            label1.Text = result;
        }

        void t_Tick(object sender, EventArgs e)
        {
            timeElapsed += t.Interval;
            label1.Text = timeElapsed.ToString();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            Parse_GBill processFiles = new Parse_GBill();
            string result = processFiles.ProcessFiles(DateTime.Now.ToShortDateString());
            //label1.Text = "";
            //t.Stop();
            if (result == "")
                result = "Done " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");

            label2.Text = result ;
        }

        private void button3_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            Parse_CBill processFiles = new Parse_CBill();
            string result = processFiles.ProcessFiles(DateTime.Now.ToShortDateString());
            //label1.Text = "";
            //t.Stop();
            if (result == "")
                result = "Done " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
            label3.Text = result;
        }

        private void button4_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            Notice_Letter processFiles = new Notice_Letter();
            string result = processFiles.ProcessFiles(DateTime.Now.ToShortDateString());


            //NotticeLetter_5303 processFiles = new NotticeLetter_5303();
            //string result = processFiles.ProcessFiles(DateTime.Now.ToShortDateString());

            label5.Text = result;
        }

        private void button5_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            ParseChecks processFiles = new ParseChecks();
            string result = processFiles.ProcessFiles(DateTime.Now.ToShortDateString());
            if (result == "")
                result = "Done " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");

            label6.Text = result;
        }

        private void button6_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            ParseNL processFiles = new ParseNL();
            string result = processFiles.ProcessFiles(DateTime.Now.ToShortDateString());

            label7.Text = result;
        }

        private void button7_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            string result2 = downloadDta.downloadFiles_ID_Cards(GlobalVar.DateofProcess);

            //unzip any CON2 or GRP2 in from_FTP dirdctory  (manually a file may have dropped here)

            downloadDta.unzip_ID_Cards();

            Parse_IDCards processFiles = new Parse_IDCards();
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

            string DirLocal = ProcessVars.InputDirectory + @"ID_Cards";
            string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), DirLocal);
          

            var t0 = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t0.Wait();
            createEmail createemail = new createEmail();

            createemail.produceSummary_ID_NON_Maintenence(DirLocal);

            label8.Text =  "ID Cards done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
        }

        private void button8_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string starttime = DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
            BackCASS processRedturns = new BackCASS();
            label9.Text = processRedturns.ProcessFiles("");
            label9.Text =  starttime + "    Done at" + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
        }

        private void button9_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            NParse_pdfs processPdfs = new NParse_pdfs();
            label10.Text = processPdfs.ProcessFiles(DateTime.Now.ToShortDateString());
        }

        private void button10_Click(object sender, EventArgs e)
        {
            Form2 newprocess = new Form2();
            newprocess.Show();
        }

        private void button11_Click(object sender, EventArgs e)
        {
            NParse_Pdfs_DueDilligence parsePDFs_D = new NParse_Pdfs_DueDilligence();
            parsePDFs_D.ProcessFiles(DateTime.Now.ToShortDateString());
        }

        private void button12_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            Parse_IDCards processMaintenanceFiles = new Parse_IDCards();
            string result = processMaintenanceFiles.MaintenanceFilestoProcess(DateTime.Now.ToShortDateString());
            createEmail createemail = new createEmail();
            createemail.produceSummary_ID_Maintenence();
            label8.Text = "ID Cards Maintenance ticket ready " + result;
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            pd.Text = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            fd.Text = GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd");
        }

        private void button13_Click(object sender, EventArgs e)
        {
            NParse_Fraud nparseFraud = new NParse_Fraud();
            nparseFraud.create_csv_Fraud(GlobalVar.DateofProcess.ToShortDateString());
        }

        private void button14_Click(object sender, EventArgs e)
        {
            Parse_IDCards processFiles = new Parse_IDCards();
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

            string DirLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\IDCards\";
            string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), DirLocal);
        }

        private void button15_Click(object sender, EventArgs e)
        {
            createEmail createemail = new createEmail();
            createemail.produceSummary_ID_NON_Maintenence("");

            label8.Text = "ID Cards email ready";
        }

        private void button16_Click(object sender, EventArgs e)
        {
            NParse_1099 parse1099 = new NParse_1099();
            string result = parse1099.ProcessFiles(GlobalVar.DateofProcess.ToShortDateString());
        }

        private void button17_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable table = dbU.ExecuteDataTable("select *  from HOR_parse_Maintenance_ID_Cards where filename = 'GRP2_20160113_DLY_1_PROCESSED.DAT' order by recnum");
            string recnum = "";
            foreach (DataRow row in table.Rows)
            {
                if (recnum == row["recnum"].ToString())
                {
                    row["type"] = "x";
                }
                else
                    recnum = row["recnum"].ToString();
            }
        

            createCSV createcsvT = new createCSV();
            string pNameT = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2016-01-14\ID_Cards\data.csv";
            if (File.Exists(pNameT))
                File.Delete(pNameT);
            var fieldnamesT = new List<string>();
            for (int index = 0; index < table.Columns.Count; index++)
            {
                fieldnamesT.Add(table.Columns[index].ColumnName);
            }
            bool respT = createcsvT.addRecordsCSV(pNameT, fieldnamesT);
            foreach (DataRow row in table.Rows)
            {
                if (row["type"] != "x")
                {
                    var rowData = new List<string>();
                    for (int index = 0; index < table.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    respT = false;
                    respT = createcsvT.addRecordsCSV(pNameT, rowData);
                }
            }
        }
       
    }
}