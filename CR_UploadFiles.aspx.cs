using System;
using System.Collections.Generic;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using System.Data.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Text;

class CR_Database : DataContext
{

    private const String LoginString = @"Server=BusinessSQL\sqlserver2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
    public Table<DCdata> HOR_Care_Radius_BatchDataXML;
    public CR_Database()
        : base(LoginString)
    {
    }
}
public partial class CR_UploadFiles : System.Web.UI.Page
{

    string originalDataPath = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_PickUp";
    string OutputDataPath = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed";
    string OutputFname = "";
    DBUtility dbU;
    List<ListItem> filesProcessed = new List<ListItem>();
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
            DatesToExport = dbU.ExecuteDataTable("HOR_CareRadius_selection");

            if (DatesToExport != null)
            {
                foreach (DataRow dr in DatesToExport.Rows)
                {
                    filesU.Add(new ListItem(dr[0].ToString() + "-" + dr[1].ToString(), dr[0].ToString() + "-" + dr[1].ToString()));
                }
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
                
                filesProcessed.Add(new ListItem(Path.GetFileName( CheckBoxListFilesP.Items[chkcount].Value)));
            }
            ExportCSV();
        }
        Button1.Attributes.Add("style", "color:green");
    }
    protected void ExportCSV()
    {
        string fileNAMES = "";
        for (int i = 0; i < filesProcessed.Count; i++) 
        {
                DataTable DatesToExport = new DataTable();
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                DataTable plData = new DataTable();
                SqlParameter[] sqlParams;

                sqlParams = new SqlParameter[] { new SqlParameter("@Ofilename", filesProcessed[i].ToString()),
                                                 new SqlParameter("@Idate",DateTime.Now.ToString("yyyy-MM-dd"))};

                plData = dbU.ExecuteDataTable("HOR_rpt_Care_Radius", sqlParams);
                if (plData != null)
                {
                    DataView dv = plData.DefaultView;
                    dv.Sort = "OriginalFileName";
                    DataTable sortedDT = dv.ToTable();
                    string pName = "";
                    CreateCSV createcsv = new CreateCSV();
                    //string pName = OutputDataPath + "\\" + CheckBoxListFilesU.Items[chkcount].Value.Replace("/", "_") + "_Care_Radius_Result.csv";
      
                    string prevFname = "";
                    foreach (DataRow row in sortedDT.Rows)
                    {
                        string JustName = row[0].ToString().Substring(0, row[0].ToString().Length - 4);
                        if (prevFname != row[0].ToString() )
                        {
                            prevFname = row[0].ToString();
                            pName = OutputDataPath + "\\" + JustName + ".csv";
                            if (File.Exists(pName))
                                File.Delete(pName);
                            var fieldnames = new List<string>();
                            for (int index = 1; index < sortedDT.Columns.Count; index++)
                            {
                                fieldnames.Add(plData.Columns[index].ColumnName);
                            }
                            bool resp = createcsv.addRecordsCSV(pName, fieldnames);

                        }
                        var rowData = new List<string>();
                        for (int index = 1; index < sortedDT.Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());
                        }
                        bool resp2 = false;
                        resp2 = createcsv.addRecordsCSV(pName, rowData);
                    }
                    SqlParameter[] sqlParams2;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@Ofilename", filesProcessed[i]),
                                                 new SqlParameter("@Idate",DateTime.Now.ToString("yyyy-MM-dd"))};

                    fileNAMES = fileNAMES + filesProcessed[i].ToString() + "<p>";
                    //produceSummary(dateProc, filesProcessed[i].ToString());
                }
                string dateProc = DateTime.Now.ToString("yyyy-MM-dd");
                produceSummary(dateProc, fileNAMES);

            
        }

        
    }
    protected void produceSummary(string dateProc, string filename)
    {

        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

        DataTable processedData = new DataTable();
        SqlParameter[] sqlParamsSum;

        sqlParamsSum = new SqlParameter[] { new SqlParameter("@Pdate", dateProc) };

        processedData = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_Input_Care", sqlParamsSum);
        StringBuilder strHTMLBuilder = new StringBuilder();
        strHTMLBuilder.Append("<p>File : " + filename + "</p>");
        if (processedData.Rows.Count > 0)
        {
            strHTMLBuilder.Append(" Care Radius Upload Results: <br>");
            strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
            foreach (DataColumn myColumn in processedData.Columns)
            {
                strHTMLBuilder.Append("<td >");
                strHTMLBuilder.Append(myColumn.ColumnName);
                strHTMLBuilder.Append("</td>");

            }
            foreach (DataRow dr in processedData.Rows)
            {
                strHTMLBuilder.Append("<tr >");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                    strHTMLBuilder.Append("</td>");

                }
                strHTMLBuilder.Append("</tr>");
            }

            sendMails sendmail = new sendMails();
            sendmail.SendMail("Care Radius Upload", "tkrompinger@apps.cierant.com, kcarpenter@apps.cierant.com, alalla@apps.cierant.com, rchico@apps.cierant.com",
                //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                         strHTMLBuilder);  //tkrompinger@apps.cierant.com

        }


    }
}