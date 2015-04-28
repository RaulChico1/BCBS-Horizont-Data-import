using System;
using System.Collections.Generic;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using System.Data.Linq;
using System.Data;
using System.Data.SqlClient;
using Excel;
using System.Linq;


public partial class Queries : System.Web.UI.Page
{
    string originalDataPath = @"C:\CierantProjects_dataLocal\Horizon_Inq";
    DataTable qryData = new DataTable();
    DBUtility dbU;
    string errors = "";
    protected void Page_Load(object sender, EventArgs e)
    {
        if (!Page.IsPostBack)
        {
            LabelLocation.InnerText = originalDataPath;

            List<ListItem> filesP = new List<ListItem>();

            var ext = new List<string> { ".xls", ".xlsx" };
            var myFiles = Directory.GetFiles(originalDataPath, "*.*", SearchOption.AllDirectories)
                 .Where(s => ext.Any(ex => s.EndsWith(ex)));

            foreach (string file in myFiles)
            {

                filesP.Add(new ListItem(file, Path.GetFileName(file)));
            }
            if (filesP.Count > 0)
            {
                CheckBoxListFilesP.DataSource = filesP;
                CheckBoxListFilesP.DataTextField = "Value";
                CheckBoxListFilesP.DataValueField = "Text";
                CheckBoxListFilesP.DataBind();
            }

        }
    }
    protected void UploadFiles(object sender, EventArgs e)
    {
        for (int chkcount = 0; chkcount < CheckBoxListFilesP.Items.Count; chkcount++)
        {
            if (CheckBoxListFilesP.Items[chkcount].Selected)
            //lblCheckBoxList.Text += ", " + chkList.Items[chkcount].Text; 
            {
                
                qryData = evaluate_XLSs(CheckBoxListFilesP.Items[chkcount].Value);
            }
        }
        if (errors.Length != 0)
            queriesTOMSG.InnerText = errors;

        Button1.Attributes.Add("style", "color:green");

    }
    public DataTable evaluate_XLSs(string fileName)
    {
 
        FileInfo fileInfo = new System.IO.FileInfo(fileName);
        DataSet result = null;
        FileStream stream = File.Open(fileName, FileMode.Open, FileAccess.Read);
        try
        {
            if (fileInfo.Extension.ToUpper() == ".XLS")
            {
                //1. Reading from a binary Excel file ('97-2003 format; *.xls)
                IExcelDataReader excelReader = ExcelReaderFactory.CreateBinaryReader(stream);
                excelReader.IsFirstRowAsColumnNames = true;
                result = excelReader.AsDataSet();

                excelReader.Close();
            }
            if (fileInfo.Extension.ToUpper() == ".XLSX")
            {
                //2. Reading from a OpenXml Excel file (2007 format; *.xlsx)
                IExcelDataReader excelReader = ExcelReaderFactory.CreateOpenXmlReader(stream);
                excelReader.IsFirstRowAsColumnNames = true;
                result = excelReader.AsDataSet();

                excelReader.Close();
            }
            var fieldnames = new List<string>();
            for (int index = 0; index < result.Tables[0].Columns.Count; index++)
            {
                fieldnames.Add(result.Tables[0].Columns[index].ColumnName);
            }
            //=====
            if (result.Tables[0].Rows.Count > 1)
            {
                foreach (DataRow row in result.Tables[0].Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < result.Tables[0].Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());

                    }
 
                }
            }
        }
        catch (Exception ex)
        {

            errors = ex.Message;
        }

        DataTable qryTable = result.Tables[0];

        qryTable.Columns.Add("Seq", typeof(Int16)).SetOrdinal(0);
        int Count = 1;
        foreach (DataRow row in qryTable.Rows)
        {
            row["Seq"]= Count;
            Count++;
        }
        return qryTable;
    }
}