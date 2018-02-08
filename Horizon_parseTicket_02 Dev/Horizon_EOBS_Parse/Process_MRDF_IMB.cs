using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Configuration;
using System.Collections;
using System.Xml;
using System.Xml.Linq;
using Microsoft.VisualBasic.FileIO;

namespace Horizon_EOBS_Parse
{
    public class Process_MRDF_IMB
    {
        DBUtility dbU;
        private static DataTable MRDF_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();

            newt.Columns.Add("IJobID");
            newt.Columns.Add("IPieceID");
            newt.Columns.Add("TotalSheetsInputFdr1");
            newt.Columns.Add("TotalSheetsInputFdr2");
            newt.Columns.Add("IAccountIdentifier");
            newt.Columns.Add("InputWeight");
            newt.Columns.Add("ChargebackAccount");
            newt.Columns.Add("ChargebackSubAccount");
            newt.Columns.Add("InsertFeeder01");
            newt.Columns.Add("InsertFeeder02");
            newt.Columns.Add("InsertFeeder03");
            newt.Columns.Add("InsertFeeder04");
            newt.Columns.Add("InsertFeeder05");
            newt.Columns.Add("InsertFeeder06");
            newt.Columns.Add("InsertFeeder07");
            newt.Columns.Add("InsertFeeder08");
            newt.Columns.Add("InsertFeeder09");
            newt.Columns.Add("InsertFeeder10");
            newt.Columns.Add("InsertFeeder11");
            newt.Columns.Add("InsertFeeder12");
            newt.Columns.Add("InsertFeeder13");
            newt.Columns.Add("InsertFeeder14");
            newt.Columns.Add("InsertFeeder15");
            newt.Columns.Add("InsertFeeder16");
            newt.Columns.Add("ApplicationPulls");
            newt.Columns.Add("ForeignMail-Divert");
            newt.Columns.Add("AlertAndClear");
            newt.Columns.Add("RecipientName");
            newt.Columns.Add("RecipientAddress1");
            newt.Columns.Add("RecipientAddress2");
            newt.Columns.Add("RecipientAddress3");
            newt.Columns.Add("RecipientAddress4");
            newt.Columns.Add("RecipientAddress5");
            newt.Columns.Add("RecipientAddress6");
            newt.Columns.Add("Zip5");
            newt.Columns.Add("Zip4");
            newt.Columns.Add("Imb");
            newt.Columns.Add("BusinessReturnAddress1");
            newt.Columns.Add("BusinessReturnAddress2");
            newt.Columns.Add("BusinessReturnAddress3");
            newt.Columns.Add("BusinessReturnAddress4");
            newt.Columns.Add("BusinessReturnAddress5");
            newt.Columns.Add("PreviousJobID");
            newt.Columns.Add("PreviousPieceID");
            newt.Columns.Add("SCIJobType");
            newt.Columns.Add("SCICycleID");
            newt.Columns.Add("SCIProcessTime");
            newt.Columns.Add("SCIPrintTime");
            newt.Columns.Add("RemainingFiller");
            newt.Columns.Add("JobID");
            newt.Columns.Add("PieceID");
            newt.Columns.Add("AccountIdentifier");
            newt.Columns.Add("ActualSheetsInput1");
            newt.Columns.Add("ActualSheetsInput2");
            newt.Columns.Add("ActFeeder01");
            newt.Columns.Add("ActFeeder02");
            newt.Columns.Add("ActFeeder03");
            newt.Columns.Add("ActFeeder04");
            newt.Columns.Add("ActFeeder05");
            newt.Columns.Add("ActFeeder06");
            newt.Columns.Add("ActFeeder07");
            newt.Columns.Add("ActFeeder08");
            newt.Columns.Add("ActFeeder09");
            newt.Columns.Add("ActFeeder10");
            newt.Columns.Add("ActFeeder11");
            newt.Columns.Add("ActFeeder12");
            newt.Columns.Add("ActFeeder13");
            newt.Columns.Add("ActFeeder14");
            newt.Columns.Add("ActFeeder15");
            newt.Columns.Add("ActFeeder16");
            newt.Columns.Add("Computed_Weight");
            newt.Columns.Add("Remote_Divert");
            newt.Columns.Add("Actual_Postage");
            newt.Columns.Add("MachineID");
            newt.Columns.Add("OperatorID");
            newt.Columns.Add("MailpieceStatus");
            newt.Columns.Add("Disposition");
            newt.Columns.Add("Disposition_Text");
            newt.Columns.Add("Cause");
            newt.Columns.Add("Cause_Text");
            newt.Columns.Add("StatusSource");
            newt.Columns.Add("ExitLocation");
            newt.Columns.Add("Process_TimeStamp");
            newt.Columns.Add("ENDRECORD");
            newt.Columns.Add("ImportFileName");
            newt.Columns.Add("ImportFileDate");


            return newt;
        }

        
        public string Process_IMB(string filename, string locationLocal)
        {
            int updErrors = 0;
            string errors = "";

            FileInfo fileInfo = new System.IO.FileInfo(filename);
            DataTable dataCSV = GetDataTabletFromCSVFile(filename);

            dataCSV.Columns.Add("DATETIME_IMPORTED");
            dataCSV.Columns.Add("IMPORT_FILENAME");
            foreach (DataRow row in dataCSV.Rows) // Loop over the rows.
            {
                row["IMPORT_FILENAME"] = fileInfo.Name;
                row["DATETIME_IMPORTED"] = DateTime.Now.ToShortDateString() + ' ' + DateTime.Now.ToShortTimeString();

            }
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from IMBDATA_IMPORT_tmp");
            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);
            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                bulkCopy.DestinationTableName = "[dbo].[IMBDATA_IMPORT_tmp]";

                try
                {
                    bulkCopy.WriteToServer(dataCSV);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
            }
            Connection.Close();
            //}


            return errors;
        }
        public string Process_MRDF(string filename, string locationLocal)
        {

            int updErrors = 0;
            string errors = "";
            FileInfo fileInfo = new System.IO.FileInfo(filename);
            string result = "";


            if (!File.Exists(ProcessVars.gmappingFileMRDF))
                //throw new Exception("Mapping file not found.");
                errors = "Mapping file not found.";
            List<List<Field>> records =
                ParseFile(filename);
            if (errors == "")
            {
                DataTable table = new DataTable();

                List<Field> fields = GetFields();
                foreach (Field field in fields)
                {

                    table.Columns.Add(field.Name);
                }

                foreach (List<Field> record in records)
                {
                    var row = table.NewRow();

                    foreach (Field field in record)
                    {
                        row[field.Name] = field.Value;

                    }
                    table.Rows.Add(row);
                }
                table.Columns.Add("ENDRECORD");
                table.Columns.Add("ImportFileName");
                table.Columns.Add("ImportFileDate");
                foreach (DataRow row in table.Rows) // Loop over the rows.
                {
                    row["ImportFileName"] = fileInfo.Name;
                    row["ImportFileDate"] = DateTime.Now.ToShortDateString() + ' ' + DateTime.Now.ToShortTimeString();

                }
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from MRDF_FILE_OUT_FULL_TMP");
                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);
                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    bulkCopy.DestinationTableName = "[dbo].[MRDF_FILE_OUT_FULL_TMP]";

                    try
                    {
                        bulkCopy.WriteToServer(table);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;    //colid 27   Member Gender
                        updErrors++;
                    }
                }
                Connection.Close();
            }


            return errors;
        }
        private List<List<Field>> ParseFile(string inputFile)
        {
            var test = "";
            //Get the field mapping.
            List<Field> fields = GetFields();
            //Create a List<List<Field>> collection of collections.
            // The main collection contains our records, and the
            // sub collection contains the fields each one of our
            // records contains.
            List<List<Field>> records = new List<List<Field>>();

            //Open the flat file using a StreamReader.
            using (StreamReader reader = new StreamReader(inputFile))
            {
                //Load the first line of the file.
                string line = reader.ReadLine();

                //Loop through the file until there are no lines
                // left.
                while (line != null)
                {
                    //Create out record (field collection)
                    List<Field> record = new List<Field>();
                    if (line.Length > 1700)
                    {
                        //Loop through the mapped fields
                        foreach (Field field in fields)
                        {
                            Field fileField = new Field();

                            //Use the mapped field's start and length
                            // properties to determine where in the
                            // line to pull our data from.
                            fileField.Value =
                                line.Substring(field.Start, field.Length);

                            //Set the name of the field.
                            fileField.Name = field.Name;

                            //Add the field to our record.
                            record.Add(fileField);
                            if (fileField.Name == "ExitLocation")
                                test = "here";
                        }

                        //Add the record to our record collection
                        records.Add(record);
                    }
                    else
                    {
                        var resultss = line.Length.ToString();

                    }
                    //Read the next line.
                    line = reader.ReadLine();
                }
            }

            //Return all of our records.
            return records;
        }

        private List<Field> GetFields()
        {
            List<Field> fields = new List<Field>();
            XmlDocument map = new XmlDocument();

            //Load the mapping file into the XmlDocument
            map.Load(ProcessVars.gmappingFileMRDF);

            //Load the field nodes.
            XmlNodeList fieldNodes = map.SelectNodes("/FileMap/Field");

            //Loop through the nodes and create a field object
            // for each one.
            foreach (XmlNode fieldNode in fieldNodes)
            {
                Field field = new Field();

                //Set the field's name
                field.Name = fieldNode.Attributes["Name"].Value;

                //Set the field's length
                field.Length =
                        Convert.ToInt32(fieldNode.Attributes["Length"].Value);

                //Set the field's starting position
                field.Start =
                        Convert.ToInt32(fieldNode.Attributes["Start"].Value) - 1;

                //Add the field to the Field list.
                fields.Add(field);
            }

            return fields;
        }

        public string Upd_MRDF_to_ID_Cards(string filename)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("MRDF_Update_IDCards");
            return "Update ID CArds done";

            //SQL to check updates

            // select HOR_parse_Maintenance_ID_Cards.Recnum, HOR_parse_Maintenance_ID_Cards.InsertDate, * from MRDF_ID_CARDS_Historical
            //inner join HOR_parse_Maintenance_ID_Cards
            //on MRDF_ID_CARDS_Historical.RecordNum = HOR_parse_Maintenance_ID_Cards.Recnum
            
            // SQL Counts by file
            //select importfilename, importfiledate, count(importfiledate) from MRDF_ID_CARDS_Historical group by importfilename, importfiledate
        }

        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)

        {

            DataTable csvData = new DataTable();

            try

            {

              using(TextFieldParser csvReader = new TextFieldParser(csv_file_path))

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

            }

            catch (Exception ex)

            {

            }

            return csvData;

        }

      
    }
}