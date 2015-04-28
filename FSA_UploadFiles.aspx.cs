using System;
using System.Collections.Generic;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using System.Data.Linq;
using System.Data;
using System.Data.SqlClient;

public partial class FSA_UploadFiles : System.Web.UI.Page
{
    string originalDataPath = @"C:\CierantProjects_dataLocal\Horizont_FSA";
    DBUtility dbU;

    protected void Page_Load(object sender, EventArgs e)
    {
        if (!Page.IsPostBack)
        {

            LabelLocation.InnerText = originalDataPath;

            List<ListItem> filesP = new List<ListItem>();
            List<ListItem> filesU = new List<ListItem>();


            string[] fileDirs = Directory.GetFiles(originalDataPath, "*.xml",
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
            DatesToExport = dbU.ExecuteDataTable("HOR_FSA_selection");

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
        for (int chkcount = 0; chkcount < CheckBoxListFilesP.Items.Count; chkcount++)
        {
            if (CheckBoxListFilesP.Items[chkcount].Selected)
            //lblCheckBoxList.Text += ", " + chkList.Items[chkcount].Text; 
            {
                CR_Upload cr_upload = new CR_Upload();
                cr_upload.CR_UploadXML(CheckBoxListFilesP.Items[chkcount].Value);
            }
        }
    }
    protected void ExportCSV(object sender, EventArgs e)
    {
        for (int chkcount = 0; chkcount < CheckBoxListFilesU.Items.Count; chkcount++)
        {
            if (CheckBoxListFilesU.Items[chkcount].Selected)
            //lblCheckBoxList.Text += ", " + chkList.Items[chkcount].Text; 
            {
                DataTable DatesToExport = new DataTable();
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                DataTable plData = new DataTable();
                SqlParameter[] sqlParams;
                string[] words = CheckBoxListFilesU.Items[chkcount].Value.Split('-');
                sqlParams = new SqlParameter[] { new SqlParameter("@Idate", words[0]),
                                                 new SqlParameter("@Icycle", words[1])};

                plData = dbU.ExecuteDataTable("HOR_rpt_FSA", sqlParams);
                if (plData != null)
                {
                    CreateCSV createcsv = new CreateCSV();
                    string pName = originalDataPath + "\\" + CheckBoxListFilesU.Items[chkcount].Value.Replace("/", "_") + "FSA_Result.csv";
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
                    }
                    SqlParameter[] sqlParams2;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@Idate", words[0]),
                                                 new SqlParameter("@Icycle", words[1])};
                    dbU.ExecuteScalar("HOR_upd_rpt_FSA", sqlParams2);
                }

            }
        }
    }
}