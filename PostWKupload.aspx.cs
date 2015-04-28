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
using System.Configuration;
using System.Text;

public partial class PostWKupload : System.Web.UI.Page
{
    string originalDataPath = @"\\CIERANT-TAPER\Clients\Horizon BCBS\14-0358_Post_Welcome_Kits\SECURE DATA\PROD_Commercial";
    string resultDir = @"\\CIERANT-TAPER\Clients\Horizon BCBS\14-0358_Post_Welcome_Kits\SECURE DATA\PROD_Commercial\Final";
    DBUtility dbU;

    protected void Page_Load(object sender, EventArgs e)
    {
        LabelLocation.InnerText = originalDataPath;

        listFiles();
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
    }
    protected void listFiles()
    {
        List<ListItem> filesP = new List<ListItem>();
        List<ListItem> filesU = new List<ListItem>();
        CheckBoxListFilesP.DataSource = null;
        CheckBoxListFilesP.DataBind();
        CheckBoxListFilesU.DataSource = null;
        CheckBoxListFilesU.DataBind();
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
        DatesToExport = dbU.ExecuteDataTable("HOR_HOR_PreSalesKit_selection");

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
    protected void ExportCSV(object sender, EventArgs e)
    {

    }
}