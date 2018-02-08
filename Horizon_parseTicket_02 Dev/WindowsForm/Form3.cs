using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Horizon_EOBS_Parse;
using System.IO;
using System.Data.SqlClient;

namespace WindowsForm
{
    public partial class Form3 : Form
    {
        DBUtility dbU;
        public Form3()
        {
            InitializeComponent();
        }
        private void Form3_Load(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            
        }
        private void button1_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Checking files Horizon Gift Card program...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            
            appSets appsets = new appSets();
            appsets.setVars();

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            string location = @"\\freenas\Internal_Production\Horizon_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + "\\Gift_Cards";
            string locationLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\GiftCards\";
            System.IO.Directory.CreateDirectory(locationLocal);
            NParse_GiftCards procesgiftcards = new NParse_GiftCards();

            DirectoryInfo txts = new DirectoryInfo(location);

            FileInfo[] files = txts.GetFiles("*.xlsx");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1 && file.Name.IndexOf("._") == -1)
                {
                    DataTable filesProcessed = dbU.ExecuteDataTable("select filename from HOR_parse_Campaigns where filename = '" + file.Name + "'");
                    if (filesProcessed.Rows.Count == 0)
                        errors = procesgiftcards.Process_GiftCards(file.FullName, locationLocal);
                    if (errors == "")
                    {
                        File.Move(file.FullName, file.Directory + "\\__" + file.Name);
                    }
                }
            }
            //check null values

            createCSV printcsv = new createCSV();
           // string[] daysWeek = procesgiftcards.GetWeekRange(DateTime.Now);
            
            string strsql = "select Recnum, '' as digUId,OutputFileName as FName,'' as artifactId,'' as LetterName, UpdAddr1 as CoverPageName,UpdAddr2 as CoverpageAddress1,UpdAddr3 as CoverpageAddress2,UpdAddr3 as CoverpageAddress3,UpdAddr4 as CoverpageAddress4,UpdCity as City,UpdState as State,UpdZip as Zip, '' as BRE, '' as TOD, '' as DL " +
                                       "from HOR_parse_Campaigns where cycledate = '" + DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd") + "' and letter_Type <> 'ACA' and len(UpdAddr2) > 1 order by recnum";
            DataTable tocsv = dbU.ExecuteDataTable(strsql);
            string filename = locationLocal + "HOR_GiftCard_" + DateTime.Now.AddDays(-0).ToString("yyyyMMdd") + "_00001.csv";
            if (tocsv.Rows.Count > 0)
            {

                if (File.Exists(filename))
                    File.Delete(filename);

                printcsv.printCSV_fullProcess(filename, tocsv, "", "");
            }

            strsql = "select Recnum, '' as digUId,OutputFileName as FName,'' as artifactId,'' as LetterName, UpdAddr1 as CoverPageName,UpdAddr2 as CoverpageAddress1,UpdAddr3 as CoverpageAddress2,UpdAddr3 as CoverpageAddress3,UpdAddr4 as CoverpageAddress4,UpdCity as City,UpdState as State,UpdZip as Zip, " +
                    "'CMC0007942 (0516), CMC0008179_A (0517)' as BRE, '' as TOD, '' as DL " +
                                       "from HOR_parse_Campaigns where cycledate = '" + DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd") + "' and letter_Type = 'ACA' and len(UpdAddr2) > 1 order by recnum";
            DataTable tocsv2 = dbU.ExecuteDataTable(strsql);
            filename = locationLocal + "HOR_GiftCard_ACA_" + DateTime.Now.AddDays(-0).ToString("yyyyMMdd") + "_00001.csv";
            if (tocsv2.Rows.Count > 0)
            {

                if (File.Exists(filename))
                    File.Delete(filename);

                printcsv.printCSV_fullProcess(filename, tocsv2, "", "");
            }


            SqlParameter[] sqlParams;
            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@Date", DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd")) };

            dbU.ExecuteNonQuery("HOR_rpt_GiftCards_to_XMpie", sqlParams);

            //DataTable gifcardsXmpie = dbU.ExecuteDataTable("select * from HOR_GiftCard_XMPie");
            //filename = locationLocal + "HOR_GiftCard_to_XMPie_" + DateTime.Now.AddDays(-3).ToString("yyyyMMdd") + "00001.csv";
            // if (gifcardsXmpie.Rows.Count > 0)
            //{

            //    if (File.Exists(filename))
            //        File.Delete(filename);

            //    printcsv.printCSV_fullProcess(filename, gifcardsXmpie, "","Y");
            //}

            // DataTable gifcardsXmpieACA = dbU.ExecuteDataTable("select * from HOR_GiftCard_ACA_XMPie");
            // filename = locationLocal + "HOR_GiftCard_to_XMPie_ACA_" + DateTime.Now.AddDays(-3).ToString("yyyyMMdd") + "00001.csv";
            // if (gifcardsXmpieACA.Rows.Count > 0)
            // {

            //     if (File.Exists(filename))
            //         File.Delete(filename);

            //     printcsv.printCSV_fullProcess(filename, gifcardsXmpieACA, "", "Y");
            // }

            Results.Text = "Horizon Gift Card ready ...";
            objPleaseWait.Close();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            ftp_downloads ftpFuncion = new ftp_downloads();
            ftpFuncion.checkFile();
           
        }

        private void button3_Click(object sender, EventArgs e)
        {
            Nparse_Historical historicals = new Nparse_Historical();
            historicals.proc_Hist();
        }

        private void Form_FormClosing(object sender, EventArgs e)
            {
            // this.Hide();
            Application.Exit();
            }
    }
}
