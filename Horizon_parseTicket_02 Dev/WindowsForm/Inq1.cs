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

namespace WindowsForm
{
    public partial class Inq1 : Form
    {

        public Inq1()
        {
            InitializeComponent();
            loadDownloaded();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket02 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");
            dataGridView1.DataSource = resultsTicket02;

            foreach (DataGridViewColumn column in dataGridView1.Columns)
            {
                //DataGridViewColumn column = dataGridView1.Columns[2];
                column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            }
        }
        private void loadDownloaded()
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsDownloaded = dbU.ExecuteDataTable("select * from HOR_parse_files_downloaded " +
                                            "where convert(date,importdate_start) = convert(date,getdate()) " +
                                            "order by seqnum");
            dataGridView2.DataSource = resultsDownloaded;

            //foreach (DataGridViewColumn column in dataGridView2.Columns)
            //{
            //    //DataGridViewColumn column = dataGridView1.Columns[2];
            //    column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            //}
        }
        private void button2_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am");
            dataGridView1.DataSource = resultsTicket01;
            foreach (DataGridViewColumn column in dataGridView1.Columns)
            {
                //DataGridViewColumn column = dataGridView1.Columns[2];
                column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            }

        }

        private void button3_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string strsql = " select * from HOR_parse_Log " +
                            "WHERE logdate BETWEEN CONVERT(date,GETDATE()-2) and GETDATE() order by logdate desc";
            DataTable resultsLog = dbU.ExecuteDataTable(strsql);
            dataGridView1.DataSource = resultsLog;
            foreach (DataGridViewColumn column in dataGridView1.Columns)
            {
                //DataGridViewColumn column = dataGridView1.Columns[2];
                column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            }
            try
            {
                foreach (DataGridViewRow Myrow in dataGridView1.Rows)
                {            //Here 2 cell is target value and 1 cell is Volume
                    if (Myrow.Cells["LogDate"].Value.ToString().Substring(0, 10) == DateTime.Now.AddDays(-1).ToString("MM/dd/yyyy"))
                    {
                        Myrow.DefaultCellStyle.BackColor = Color.LightBlue;
                    }
                    
                }
            }
            catch (Exception ex)
            {

            }

        }
       
        private void button4_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string strsql = " select * from HOR_parse_files_to_CASS " +
                            "WHERE ImportDate BETWEEN CONVERT(date,GETDATE()-2) and GETDATE() " +
                            "and FileNameCASS not like '%File not to CASS%' " +
                            "and CASSReceiveDate is  null order by CONVERT(date,ImportDate) desc, FileName";
            DataTable resultsTicket01 = dbU.ExecuteDataTable(strsql);
            dataGridView1.DataSource = resultsTicket01;
            foreach (DataGridViewColumn column in dataGridView1.Columns)
            {
                //DataGridViewColumn column = dataGridView1.Columns[2];
                column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            }
            try
            {
                foreach (DataGridViewRow Myrow in dataGridView1.Rows)
                {            //Here 2 cell is target value and 1 cell is Volume
                    if (Myrow.Cells["ImportDate"].Value.ToString().Substring(0, 10) == DateTime.Now.AddDays(-1).ToString("MM/dd/yyyy"))
                    {
                        Myrow.DefaultCellStyle.BackColor = Color.LightBlue;
                    }
                   
                }
            }
            catch (Exception ex)
            {

            }
        }

        private void button5_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string strsql = " select Recnum, FileName,Sheet_Count,Addr1, Addr2, Addr3, Addr4,Addr5,Addr6, DE_Flag,Dl,Med_Flag " +
                            "from HOR_parse_UCDS where CONVERT(date,importdate) = CONVERT(date,getdate()) order by FileName ";
            DataTable resultsUCDS = dbU.ExecuteDataTable(strsql);
            dataGridView1.DataSource = resultsUCDS;
            foreach (DataGridViewColumn column in dataGridView1.Columns)
            {
                //DataGridViewColumn column = dataGridView1.Columns[2];
                column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            }
        }

        private void button6_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string strsql = " select Recnum, FileName,Sheet_Count,Addr1, Addr2, Addr3, Addr4,Addr5,Addr6, DE_Flag,Dl,Med_Flag " +
                            "from HOR_parse_UCDS where CONVERT(date,importdate) = CONVERT(date,getdate()) order by FileName ";
            DataTable resultsUCDS = dbU.ExecuteDataTable(strsql);
            dataGridView1.DataSource = resultsUCDS;
            foreach (DataGridViewColumn column in dataGridView1.Columns)
            {
                //DataGridViewColumn column = dataGridView1.Columns[2];
                column.AutoSizeMode = DataGridViewAutoSizeColumnMode.DisplayedCells;
            }
        }

        private void button7_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            
            DataTable resultsData = dbU.ExecuteDataTable("HOR_ZZ_rpt_IDs_Some_Files");
            createCSV createfile = new createCSV();
            string fName = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\SpecialRpts\MultyMail_MemberID_" + DateTime.Now.ToString("yyyy_MM_dd_hh_mm_ ss") + ".csv";
            createfile.printCSV_fullProcess(fName, resultsData, "", "N");
        }

      
    }
}
