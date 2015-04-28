using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.IO;
using System.Text.RegularExpressions;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using System.Data.Linq;
using System.Data.Linq.Mapping;


class PSK_Database : DataContext
{

    private const String LoginString2 = @"Server=BusinessSQL\sqlserver2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
    public Table<DCdata> HOR_PreSalesKit_MedicareMailData;
    public PSK_Database()
        : base(LoginString2)
    {
    }
}


/// <summary>
/// Summary description for PSK_Upload
/// </summary>
public class PSK_Upload
{
    DataTable dataPSKToUpdate = PSK_data_Table();
    DBUtility dbU;

    private static DataTable PSK_data_Table()
    {
        DataTable newt = new DataTable();
        newt.Clear();
        newt.Columns.Add("FirstName");
        newt.Columns.Add("LastName");
        newt.Columns.Add("Company");
        newt.Columns.Add("Category");
        newt.Columns.Add("BusUnit");
        newt.Columns.Add("TotalAvail");
        newt.Columns.Add("IsPod");
        newt.Columns.Add("ReportOn");
        newt.Columns.Add("ACRONYM");
        newt.Columns.Add("ACTION");
        newt.Columns.Add("Result");

        return newt;
    }
    public string PSK_Upload_CSV(string fileName, string nextCycle)
    {
        string result = "ok";
        string header = "FirstName,LastName,Company,Street Addr,Other Addr,Other Addr2,Mail Stop,City,St,ZipCode,Country,Phone,Request_ID,Project_ID,Region_Code,Ship_Method,Special Instructions,Package_Code";
        DataTable csvData = new DataTable();
        try
        {
            using (TextFieldParser csvReader = new TextFieldParser(fileName))
            {
                csvReader.SetDelimiters(new string[] { "," });
                csvReader.HasFieldsEnclosedInQuotes = true;
                //read column names
                string[] colFields = csvReader.ReadFields();
                string actualHeader = string.Join(",", colFields);
                if (header.ToUpper() == actualHeader.Replace("\\", "").Replace("\"", "").ToUpper())
                {
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                            }
                        }
                        csvData.Rows.Add(fieldData);
                    }
                }
                else
                { result = "error in header " + fileName; }
            }
        }
        catch (Exception ex)
        {
            result = "error in file " + fileName + " " + ex.Message;
        }
        if (result == "ok")
        {
            if (csvData != null)
            {


                string resultSQL = uploadtoSQL(csvData, Path.GetFileName(fileName), nextCycle);
                if (resultSQL == "ok")
                    File.Move(fileName, Path.GetDirectoryName(fileName) + "\\__" + Path.GetFileName(fileName));
                else
                    result = result + " " + resultSQL;
            }
        }

        return result;

    }
    public string uploadtoSQL(DataTable csvData, string Fname, string nextCycle)
    {
        int errors = 0;
        string results = "ok";

        int recnum = 1;
        DataTable DatesToExport = new DataTable();
        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
        var MaxRecnum = dbU.ExecuteScalar("select MAX(Recnum) from HOR_PreSalesKit_MedicareMailData");
        if (MaxRecnum != null)
            if (MaxRecnum.ToString() != "")
                recnum = Convert.ToInt16(MaxRecnum.ToString()) + 1;



        string strsql = "delete from  HOR_PreSalesKit_MedicareMailData_tmp";
        dbU.ExecuteScalar(strsql);
        try
        {
            foreach (DataRow row in csvData.Rows)
            {
                strsql = "insert into HOR_PreSalesKit_MedicareMailData_tmp (Recnum,FileName,Cycle,ImportDate,FirstName,LastName,Company,StreetAddr,OtherAddr,OtherAddr2,MailStop,City,State,Zip,Country,Phone,RequestID,ProjectID,RegionCode,ShipMethod,SpecialIns,PackageCode ) values (" +
                                recnum + ",'" + Fname + "','" +
                                nextCycle + "',GETDATE(),'" +
                                row["FirstName"].ToString().Replace("'", " ") + "', '" +
                                row["LastName"].ToString().Replace("'", " ") + "', '" +
                                row["Company"].ToString().Replace("'", " ") + "', '" +
                                row["Street Addr"].ToString().Replace("'", " ") + "', '" +
                                row["Other Addr"].ToString().Replace("'", " ") + "', '" +
                                row["Other Addr2"].ToString().Replace("'", " ") + "', '" +
                                row["Mail Stop"].ToString().Replace("'", " ") + "', '" +
                                row["City"].ToString().Replace("'", " ") + "', '" +

                                row["St"].ToString().Replace("'", " ") + "', '" +
                                row["ZipCode"].ToString().Replace("'", " ") + "', '" +
                                row["Country"].ToString().Replace("'", " ") + "', '" +
                                row["Phone"].ToString().Replace("'", " ") + "', '" +
                                row["Request_ID"].ToString().Replace("'", " ") + "', '" +

                                row["Project_ID"].ToString().Replace("'", " ") + "', '" +
                                row["Region_Code"].ToString().Replace("'", " ") + "', '" +

                                row["Ship_Method"].ToString().Replace("'", " ") + "', '" +
                                row["Special Instructions"].ToString().Replace("'", " ") + "', '" +
                                row["Package_Code"].ToString().Replace("'", " ") + "')";
                dbU.ExecuteScalar(strsql);
                recnum++;

            }
        }
        catch (Exception ex)
        {
            errors++;
            results = ex.Message;
        }
        if (errors == 0)
        {
            strsql = "PSK_copyfrom_tmp_MailingData";
            dbU.ExecuteScalar(strsql);

        }

        return results;
    }
}