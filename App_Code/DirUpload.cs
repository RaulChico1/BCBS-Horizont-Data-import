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


class DIR_Database : DataContext
{

    private const String LoginString2 = @"Server=BusinessSQL\sqlserver2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
    public Table<DCdata> HOR_ManagedCareDirectories;
    public DIR_Database()
        : base(LoginString2)
    {
    }
}


public class DirUpload
{
    DataTable dataDIRToUpdate = DIR_data_Table();
    DBUtility dbU;

    private static DataTable DIR_data_Table()
    {
        DataTable newt = new DataTable();
        newt.Clear();
        newt.Columns.Add("First Name");
        newt.Columns.Add("Last Name");
        newt.Columns.Add("Address");
        newt.Columns.Add("City");
        newt.Columns.Add("State");
        newt.Columns.Add("Zip");
        newt.Columns.Add("Status");
        newt.Columns.Add("Classification");
        newt.Columns.Add("UPS Suggested Addr1");
        newt.Columns.Add("UPS Suggested Addr2");
        newt.Columns.Add("UPS Suggested City");
        newt.Columns.Add("UPS Suggested State");
        newt.Columns.Add("UPS Suggested Zip");
        newt.Columns.Add("UPS Suggested Plus 4");
        newt.Columns.Add("Error");

        return newt;
    }
    public string DIR_Upload_CSV(string fileName, string nextCycle)
    {
        string result = "ok";
        string header = "First Name,Last Name,Address,City,State,Zip,Status,Classification,UPS Suggested Addr1,UPS Suggested Addr2,UPS Suggested City,UPS Suggested State,UPS Suggested Zip,UPS Suggested Plus 4,Error";
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
                        int totNulls = 0;
                        string[] fieldData = csvReader.ReadFields();
                        //Making empty value as null
                        for (int i = 0; i < fieldData.Length; i++)
                        {
                            if (fieldData[i] == "")
                            {
                                fieldData[i] = null;
                                totNulls++;
                            }
                        }
                        if (totNulls > 8)
                            totNulls = totNulls;
                        if (totNulls < 9)
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
        var MaxRecnum = dbU.ExecuteScalar("select MAX(Recnum) from HOR_MCDM");
        if (MaxRecnum != null)
            if (MaxRecnum.ToString() != "")
                recnum = Convert.ToInt16(MaxRecnum.ToString()) + 1;



        string strsql = "delete from  HOR_MCDM_tmp";
        dbU.ExecuteScalar(strsql);
        try
        {
            foreach (DataRow row in csvData.Rows)
            {
                strsql = "insert into HOR_MCDM_tmp (Recnum,FileName,Cycle,ImportDate,ID,FulFillmentItem,FirstName,LastName,Address,City,State,Zip,Status,Classification,UPSSuggestedAddr1,UPSSuggestedAddr2,UPSSuggestedCity,UPSSuggestedState,UPSSuggestedZip,UPSSuggestedPlus4,Error ) values (" +
                                recnum + ",'" + Fname + "','" +
                                nextCycle + "',GETDATE(),'MCDM','HMODIR','" +
                                row["First Name"].ToString().Replace("'", " ") + "', '" +
                                row["Last Name"].ToString().Replace("'", " ") + "', '" +
                                row["Address"].ToString().Replace("'", " ") + "', '" +
                                row["City"].ToString().Replace("'", " ") + "', '" +
                                row["State"].ToString().Replace("'", " ") + "', '" +
                                row["Zip"].ToString().Replace("'", " ") + "', '" +
                                row["Status"].ToString().Replace("'", " ") + "', '" +
                                row["Classification"].ToString().Replace("'", " ") + "', '" +

                                row["UPS Suggested Addr1"].ToString().Replace("'", " ") + "', '" +
                                row["UPS Suggested Addr2"].ToString().Replace("'", " ") + "', '" +
                                row["UPS Suggested City"].ToString().Replace("'", " ") + "', '" +
                                row["UPS Suggested State"].ToString().Replace("'", " ") + "', '" +
                                row["UPS Suggested Zip"].ToString().Replace("'", " ") + "', '" +

                                row["UPS Suggested Plus 4"].ToString().Replace("'", " ") + "', '" +
                                row["Error"].ToString().Replace("'", " ") + "')";


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
            strsql = "MCDM_copyfrom_tmp_MailingData";
            dbU.ExecuteScalar(strsql);

        }

        return results;
    }
}