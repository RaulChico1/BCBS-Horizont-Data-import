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
using System.Configuration;
using System.Text;

public partial class PreSalesKitUpd : System.Web.UI.Page
{
    string originalDataPath = @"\\CIERANT-TAPER\Clients\Horizon BCBS\Pre-Sales_MEDICARE\MPSA\MPSA_Orders\Ship Confirmation";
    DataTable qryData = qryData_Table();
    //DataTable tblErrors = new DataTable();
    DBUtility dbU;
    string errors = "";
    protected void Page_Load(object sender, EventArgs e)
    {
        if (!Page.IsPostBack)
        {
            LabelLocation.InnerText = originalDataPath;

            List<ListItem> filesP = new List<ListItem>();

            var ext = new List<string> { ".xls", ".xlsx" };
            var myFiles = Directory.GetFiles(originalDataPath, "*.*", SearchOption.TopDirectoryOnly)
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

                DataTable Tmp_qryData = evaluate_XLSs(CheckBoxListFilesP.Items[chkcount].Value);
                foreach (DataRow dr in Tmp_qryData.Rows)
                {
                    DataRow drNew = qryData.NewRow();
                    drNew.ItemArray = dr.ItemArray;
                    qryData.Rows.Add(drNew);
                }
            }
        }
        if (errors.Length != 0)
            queriesTOMSG.InnerText = errors;




        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
        dbU.ExecuteScalar("delete from Tempo_PSK_ship");

        //var MaxRecnum = dbU.ExecuteScalar("select MAX(Recnum) from HOR_MCDM");
        //if (MaxRecnum != null)
        //    if (MaxRecnum.ToString() != "")
        //        recnum = Convert.ToInt16(MaxRecnum.ToString()) + 1;
        SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

        Connection.Open();

        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
        {
            bulkCopy.DestinationTableName =
                "[dbo].[Tempo_PSK_ship]";

            try
            {
                // Write from the source to the destination.
                bulkCopy.WriteToServer(qryData);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        Connection.Close();
        string strSql = "select * from Tempo_PSK_ship left join HOR_PreSalesKit_MedicareMailData " +
                        "on Tempo_PSK_ship.recnum = HOR_PreSalesKit_MedicareMailData.recnum " +
                        "where HOR_PreSalesKit_MedicareMailData.recnum is null";
        DataTable tblErrors = new DataTable();
        tblErrors = dbU.ExecuteDataTable(strSql);
        if (tblErrors.Rows.Count > 0)
        {
            List<ListItem> filesE = new List<ListItem>();
            foreach (DataRow dr in tblErrors.Rows)
            {
                filesE.Add(new ListItem(dr[0].ToString(), dr[0].ToString() + "   " + dr[1].ToString() + "   " + dr[2].ToString() + " not found"));
            }

            if (filesE.Count > 0)
            {
                CheckBoxListFilesU.DataSource = filesE;
                CheckBoxListFilesU.DataTextField = "Value";
                CheckBoxListFilesU.DataValueField = "Text";
                CheckBoxListFilesU.DataBind();
            }
        }
        Button1.Attributes.Add("style", "color:green");
        BtnUpdateShip.Enabled = true;
    }
    protected void UpdateShip(object sender, EventArgs e)
    {
        DataTable tblErrors = new DataTable();
        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
        string updateSQL = "update HOR_PreSalesKit_MedicareMailData " +
                            "set HOR_PreSalesKit_MedicareMailData.ShipDate = Tempo_PSK_ship.Shipdate " +
                            "from Tempo_PSK_ship inner join HOR_PreSalesKit_MedicareMailData " +
                            "on Tempo_PSK_ship.recnum = HOR_PreSalesKit_MedicareMailData.recnum ";
        dbU.ExecuteNonQuery(updateSQL);


        string strSql = "select distinct FileName from Tempo_PSK_ship";
        DataTable filesProcessed = dbU.ExecuteDataTable(strSql);
        StringBuilder strHTMLBuilder = new StringBuilder();
        strHTMLBuilder.Append("<p>File(s) Processed:</p>");

        foreach (DataRow dr in filesProcessed.Rows)
        {
            strHTMLBuilder.Append(dr[0].ToString());
            strHTMLBuilder.Append("<br>");
            //move file
            File.Move(originalDataPath + "\\" + dr[0].ToString(), originalDataPath + "\\Archive\\" + dr[0].ToString());
        }



        strSql = "select Tempo_PSK_ship.* from Tempo_PSK_ship left join HOR_PreSalesKit_MedicareMailData " +
                        "on Tempo_PSK_ship.recnum = HOR_PreSalesKit_MedicareMailData.recnum " +
                        "where HOR_PreSalesKit_MedicareMailData.recnum is null";
        tblErrors = dbU.ExecuteDataTable(strSql);
        if (tblErrors.Rows.Count > 0)
        {
            strHTMLBuilder.Append(" Errors: <br>");
            strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
            foreach (DataColumn myColumn in tblErrors.Columns)
            {
                strHTMLBuilder.Append("<td >");
                strHTMLBuilder.Append(myColumn.ColumnName);
                strHTMLBuilder.Append("</td>");

            }
            foreach (DataRow dr in tblErrors.Rows)
            {
                strHTMLBuilder.Append("<tr >");
                foreach (DataColumn myColumn in tblErrors.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                    strHTMLBuilder.Append("</td>");

                }
                strHTMLBuilder.Append("</tr>");
            }


        }
        else
            strHTMLBuilder.Append("No errors");

        sendMails sendmail = new sendMails();
        sendmail.SendMail("Pre Sales Welcome Kits Shipping Updates", "tkrompinger@apps.cierant.com, kcarpenter@apps.cierant.com, rchico@apps.cierant.com",
                                    "noreply@apps.cierant.com", "\n\n" +
                                     strHTMLBuilder);  //tkrompinger@apps.cierant.com

        BtnUpdateShip.Attributes.Add("style", "color:green");
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
        DataTable tmpqryTable = result.Tables[0];

        DataColumnCollection columns = tmpqryTable.Columns;

        if (columns.Contains("Unique_ID"))
        {
            tmpqryTable.Columns["Unique_ID"].ColumnName = "Recnum";
        }
        if (columns.Contains("Ship Date"))
        {
            tmpqryTable.Columns["Ship Date"].ColumnName = "ShipDate";
        }

        DataColumn newCol = new DataColumn("FileName", typeof(string));
        newCol.AllowDBNull = true;
        tmpqryTable.Columns.Add(newCol);
        foreach (DataRow row in tmpqryTable.Rows)
        {
            row["FileName"] = fileInfo.Name;
        }

        return tmpqryTable;
    }
    private static DataTable qryData_Table()
    {
        DataTable newt = new DataTable();
        newt.Clear();
        newt.Columns.Add("Recnum");
        newt.Columns.Add("ShipDate");
        newt.Columns.Add("FileName");

        return newt;
    }

}