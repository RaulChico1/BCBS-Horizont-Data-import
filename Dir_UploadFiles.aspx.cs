using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using System.Configuration;
using System.Drawing;
using System.IO.Compression;
using System.Data;
using iTextSharp.text.pdf;
using System.Data.SqlClient;

public partial class Dir_UploadFiles : System.Web.UI.Page
{
    string originalDataPath = @"C:\CierantProjects_dataLocal\MCDM";
    DBUtility dbU;
    protected void Page_Load(object sender, EventArgs e)
    {
        if (!Page.IsPostBack)
        {

            LabelLocation.InnerText = originalDataPath;

            List<ListItem> filesP = new List<ListItem>();
            List<ListItem> filesU = new List<ListItem>();

            string[] fileDirs = Directory.GetFiles(originalDataPath, "*.csv",
                                        SearchOption.TopDirectoryOnly);

            foreach (string filePath in fileDirs)
            {
                if (filePath.IndexOf("__") == -1)
                    filesP.Add(new ListItem(filePath, Path.GetFileName(filePath)));
            }
            if (filesP.Count > 0)
            {
                CheckBoxListFilesP.DataSource = filesP;
                CheckBoxListFilesP.DataTextField = "Value";
                CheckBoxListFilesP.DataValueField = "Text";
                CheckBoxListFilesP.DataBind();
            }
            DataTable DatesToExport = new DataTable();
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DatesToExport = dbU.ExecuteDataTable("HOR_MCDM_selection");

            if (DatesToExport != null)
            {
                foreach (DataRow dr in DatesToExport.Rows)
                {
                    filesU.Add(new ListItem(dr[0].ToString() + "-" + dr[1].ToString(), dr[0].ToString() + "-" + dr[1].ToString()));
                }
            }

            if (filesU.Count > 0)
            {
                CheckBoxListFilesU.DataSource = filesU;
                CheckBoxListFilesU.DataTextField = "Value";
                CheckBoxListFilesU.DataValueField = "Text";
                CheckBoxListFilesU.DataBind();
            }

        }
    }
    protected void selectAllP(object sender, EventArgs e)
    {
        if (All1.Text == "All")
        {
            foreach (ListItem li in CheckBoxListFilesP.Items)
            {
                li.Selected = true;
            };
            All1.Text = "all";
        }
        else
        {
            foreach (ListItem li in CheckBoxListFilesP.Items)
            {
                li.Selected = false;
            };
            All1.Text = "All";
        }
    }
    protected void UploadFiles(object sender, EventArgs e)
    {
        string NextCycle = "00";
        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
        var LastCycle = dbU.ExecuteScalar("select Max(cycle) as LastCycle  from  HOR_MCDM where ImportDate = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
        if (LastCycle != null)
            if (LastCycle.ToString() != "")
                NextCycle = (Convert.ToInt16(LastCycle.ToString()) + 1).ToString("00");

        for (int chkcount = 0; chkcount < CheckBoxListFilesP.Items.Count; chkcount++)
        {
            if (CheckBoxListFilesP.Items[chkcount].Selected)
            //lblCheckBoxList.Text += ", " + chkList.Items[chkcount].Text; 
            {
                DirUpload dirload = new DirUpload();
                string result = dirload.DIR_Upload_CSV(CheckBoxListFilesP.Items[chkcount].Value, NextCycle);

            }
        }
        Button1.Attributes.Add("style", "color:green");
    }
    protected void ExportCSV(object sender, EventArgs e)
    {
        for (int chkcount = 0; chkcount < CheckBoxListFilesU.Items.Count; chkcount++)
        {
            if (CheckBoxListFilesU.Items[chkcount].Selected)
            //lblCheckBoxList.Text += ", " + chkList.Items[chkcount].Text; 
            {
                //  Mail file
                string pName = originalDataPath + "\\PSA_Mail_" + CheckBoxListFilesU.Items[chkcount].Value.Replace("/", "_") + ".csv";
                string Updsql = "update HOR_PreSalesKit_MedicareMailData set flag = 'CSV ' + convert(varchar(10),getdate(),126) + ',' where recnum = ";
                string[] words = CheckBoxListFilesU.Items[chkcount].Value.Split('-');
                produce_CSV("HOR_rpt_PreSalesKit_Medicare", pName, Updsql, words);

                // Null file
                pName = originalDataPath + "\\PSA_NULL_" + CheckBoxListFilesU.Items[chkcount].Value.Replace("/", "_") + ".csv";
                produce_CSV("HOR_rpt_PreSalesKit_Medicare_NULL", pName, "", words);

                //+++++  3PL
                pName = originalDataPath + "\\PSA_3PL_" + CheckBoxListFilesU.Items[chkcount].Value.Replace("/", "_") + ".txt";
                produce_Delimited("HOR_rpt_PreSales_Kit_Medicare_3PL", pName, "", words);

            }
        }
        Button2.Attributes.Add("style", "color:green");
    }

    protected void produce_CSV(string SelectSQL, string pName, string UpdSQL, string[] words)
    {
        DataTable DatesToExport = new DataTable();
        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
        DataTable plData = new DataTable();
        SqlParameter[] sqlParams;

        sqlParams = new SqlParameter[] { new SqlParameter("@Idate", words[0]),
                                                 new SqlParameter("@Icycle", words[1])};

        plData = dbU.ExecuteDataTable(SelectSQL, sqlParams);
        if (plData != null)
        {
            CreateCSV createcsv = new CreateCSV();
            if (File.Exists(pName))
                File.Delete(pName);
            var fieldnames = new List<string>();
            for (int index = 0; index < plData.Columns.Count; index++)
            {
                fieldnames.Add(plData.Columns[index].ColumnName);
            }
            bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            foreach (DataRow row in plData.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < plData.Columns.Count; index++)
                {
                    rowData.Add(row[index].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsCSV(pName, rowData);
                if (UpdSQL != "")
                    dbU.ExecuteScalar(UpdSQL + row[0]);
            }

        }

    }

    protected void produce_Delimited(string SelectSQL, string pName, string UpdSQL, string[] words)
    {
        DataTable DatesToExport = new DataTable();
        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
        DataTable plData = new DataTable();
        SqlParameter[] sqlParams;

        sqlParams = new SqlParameter[] { new SqlParameter("@Idate", words[0]),
                                                 new SqlParameter("@Icycle", words[1])};

        plData = dbU.ExecuteDataTable(SelectSQL, sqlParams);
        if (plData != null)
        {
            CreateCSV createcsv = new CreateCSV();
            if (File.Exists(pName))
                File.Delete(pName);
            var fieldnames = new List<string>();
            for (int index = 0; index < plData.Columns.Count; index++)
            {
                fieldnames.Add(plData.Columns[index].ColumnName);
            }
            bool resp = createcsv.addRecordsTabDelimited(pName, fieldnames);
            foreach (DataRow row in plData.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < plData.Columns.Count; index++)
                {
                    rowData.Add(row[index].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsTabDelimited(pName, rowData);
                if (UpdSQL != "")
                    dbU.ExecuteScalar(UpdSQL + row[0]);
            }

        }

    }
}