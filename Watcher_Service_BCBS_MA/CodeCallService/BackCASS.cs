using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using System.Xml;
using System.Xml.Linq;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using System.Configuration;
using Microsoft.VisualBasic.FileIO;

namespace CodeCallService
{
    public class BackCASS
    {
        public DataTable readQualifiedMAS023(string fileName, string fname)
        {
            
            DataTable QualifiedRecords = readcsvMAS023(fileName, fname);
            return QualifiedRecords;
        }
        public DataTable readQualified(string fileName)
        {

            DataTable QualifiedRecords = readcsv(fileName);
            return QualifiedRecords;
        }
        public DataTable readNonDeliverable(string fileName)
        {

            DataTable NonDeliverable = readcsvError(fileName);
            return NonDeliverable;
        }
        public DataTable readcsvMAS023(string fileName, string fname)
        {
            DataTable csvData = new DataTable();

            try
            {

                using (TextFieldParser csvReader = new TextFieldParser(fileName))
                {

                    csvReader.SetDelimiters(new string[] { "," });

                    csvReader.HasFieldsEnclosedInQuotes = true;

                    string[] colFields = csvReader.ReadFields();

                    foreach (string column in colFields)
                    {

                        DataColumn datecolumn = new DataColumn(column);

                        datecolumn.AllowDBNull = true;

                        csvData.Columns.Add(datecolumn);

                    }
                    csvData.Columns.Add("FileName");
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

                    foreach (DataRow dr in csvData.Rows)
                    {
                        dr["FileName"] = fname;
                    }
                }

            }

            catch (Exception ex)
            {

            }

            return csvData;

        }
            public DataTable readcsv(string fileName)
        {
            DataTable dataToUpdate = Result_data_Table();
            int currLine = 0;
            int valueOk = 0;
            string line;
            System.IO.StreamReader file =
           new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                currLine++;
                if (currLine == 1)
                    if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,RETURN_FIELD_13,RETURN_FIELD_14,NAME_FULL,ADDRESS_LINE_3,ALT_ADDRESS_2,ALT_ADDRESS_1,DELIVERY_ADDRESS,CITY,St,ZIP+4,De,Re,IMB CHARACTERS,IMB DIGITS")
                        valueOk = 1;

                    else if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,ADDRESS_LINE_3,ALT_ADDRESS_2,ALT_ADDRESS_1,DELIVERY_ADDRESS,CITY,St,ZIP+4,De,Re,IMB CHARACTERS,IMB DIGITS")
                        valueOk = 1;
                    else if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,St,ZIP+4,De,Re,Intelligent Mail barcode,Intelligent Mail barcode")
                        valueOk = 1;
                    else

                        valueOk = 0;

                if (currLine > 1 && valueOk == 1)
                {
                    Regex csvSplit = null;
                    var row = dataToUpdate.NewRow();
                    //var lineO = line.Split(',').ToList();
                    csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
                    int xMatch = 0;
                    foreach (Match match in csvSplit.Matches(line.ToString()))
                    {
                        // was 5
                        if (xMatch == 0)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 11)
                            row["County"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 14)
                            row["Uaddr1"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 15)
                            row["Uaddr2"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 16)
                            row["Uaddr3"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 17)
                            row["Uaddr4"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 18)
                            row["Uaddr5"] = match.Value.Replace("\"", "").Replace(",", "");

                        if (xMatch == 19)
                            row["City"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 20)
                            row["State"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 21)
                            row["Zip"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 24)
                            row["IMBChar"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 25)
                            row["IMBDig"] = match.Value.Replace("\"", "").Replace(",", "");
                        xMatch++;
                    }
                    dataToUpdate.Rows.Add(row);
                }

            }

            file.Close();
            return dataToUpdate;
        }
        public DataTable readcsvError(string fileName)
        {
            DataTable dataToUpdate = Result_data_Table();
            int currLine = 0;
            int valueOk = 0;
            string line;
            int type = 0;
            System.IO.StreamReader file =
           new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                currLine++;
                if (currLine == 1)
                    if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,St,ZIP+4,De,Re")
                    {
                        valueOk = 1;

                    }
                    else if (line.Replace("\"", "") == "RECNO,RETURN_FIELD_01,RETURN_FIELD_02,RETURN_FIELD_03,RETURN_FIELD_04,RETURN_FIELD_05,RETURN_FIELD_06,RETURN_FIELD_07,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,RETURN_FIELD_13,RETURN_FIELD_14,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,State,ZIP+4,Delivery Point,Return Code")
                    {
                        valueOk = 1;
                        type = 1;
                    }
                    else if (line.Replace("\"", "") == "RETURN_FIELD_01,RETURN_FIELD_02,RETURN_FIELD_03,RETURN_FIELD_04,RETURN_FIELD_05,RETURN_FIELD_06,RETURN_FIELD_07,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,RETURN_FIELD_13,RETURN_FIELD_14,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,State,ZIP+4,Delivery Point,Return Code")
                    {
                        valueOk = 1;
                        type = 2;
                    }
                    else
                        valueOk = 0;

                if (currLine > 1 && valueOk == 1)
                {
                    Regex csvSplit = null;
                    var row = dataToUpdate.NewRow();
                    //var lineO = line.Split(',').ToList();
                    csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
                    int xMatch = 0;
                    foreach (Match match in csvSplit.Matches(line.ToString()))
                    {//  WAS 5
                        if (xMatch == 0 && type == 0)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 1 && type == 1)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 0 && type == 2)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 23)
                            row["ReturnCode"] = match.Value.Replace("\"", "").Replace(",", "");
                        
                        xMatch++;
                    }
                    dataToUpdate.Rows.Add(row);
                }
                if (currLine > 1 && valueOk == 2)
                {
                    Regex csvSplit = null;
                    var row = dataToUpdate.NewRow();
                    //var lineO = line.Split(',').ToList();
                    csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
                    int xMatch = 0;
                    foreach (Match match in csvSplit.Matches(line.ToString()))
                    {
                        if (xMatch == 1)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");

                        xMatch++;
                    }
                    dataToUpdate.Rows.Add(row);
                }
            }

            file.Close();
            return dataToUpdate;

        }
        private static DataTable Result_data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("Uaddr1");
            newt.Columns.Add("Uaddr2");
            newt.Columns.Add("Uaddr3");
            newt.Columns.Add("Uaddr4");
            newt.Columns.Add("Uaddr5");
            newt.Columns.Add("City");
            newt.Columns.Add("State");
            newt.Columns.Add("Zip");
            newt.Columns.Add("County");
            newt.Columns.Add("IMBChar");
            newt.Columns.Add("IMBDig");
            newt.Columns.Add("ReturnCode");
            return newt;
        }
    }
}
