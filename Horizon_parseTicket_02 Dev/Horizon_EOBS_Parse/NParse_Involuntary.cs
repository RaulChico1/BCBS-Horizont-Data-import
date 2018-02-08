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
using System.Text.RegularExpressions;
using System.Linq;
using Microsoft.VisualBasic.FileIO;

namespace Horizon_EOBS_Parse
{
    class ImportedData
    {
        public string MEMBER_ID { get; set; }
        public string MEME_FIRST_NAME { get; set; }
        public string MEME_LAST_NAME { get; set; }
        public string MEPE_EFF_DT { get; set; }
        public string MEPE_TERM_DT { get; set; }
        public string TERM_RSN { get; set; }
        public string ADDRESS { get; set; }
        public string SBAD_CITY { get; set; }
        public string SBAD_STATE { get; set; }
        public string PHONE { get; set; }
        public string ADDI_PHONE { get; set; }
        public string MEME_SEX { get; set; }
        public string MEME_BIRTH_DT { get; set; }
        public string CSPI_ID { get; set; }
        public string MEME_FAM_LINK_ID { get; set; }
        public string MEMBER_ID2 { get; set; }
        public string MEME_FIRST_NAME2 { get; set; }
        public string MEME_LAST_NAME2 { get; set; }
        public string MEPE_EFF_DT2 { get; set; }
        public string MEPE_TERM_DT2 { get; set; }
        public string SBAD_ADDR1 { get; set; }
        public string SBAD_ADDR2 { get; set; }
        public string SBAD_ADDR3 { get; set; }
        public string SBAD_CITY1 { get; set; }
        public string SBAD_STATE1 { get; set; }
        public string SBAD_ZIP1 { get; set; }
        public string Textbox39 { get; set; }
        public string Textbox58 { get; set; }


    }

    public class NParse_Involuntary
    {
        DBUtility dbU;
        DataTable dt = new DataTable();
         public string importInvoluntary(string location)
        {
            string totErrors = "";
            DirectoryInfo xlss = new DirectoryInfo(location);

            FileInfo[] files = xlss.GetFiles("*.csv");

            string errors = "";
            foreach (FileInfo file in files)
            {
                string partFilename = file.Name.Substring(0, 3);
                if (partFilename.IndexOf("_") == -1 && partFilename.IndexOf("._") == -1)
                {
                    errors = processInvol(file.FullName);
                    if (errors == "")
                    {
                        Directory.Move(file.FullName, file.Directory + "\\__" + file.Name);
                        //errors = printCSV(GlobalVar.DateofProcess.ToShortDateString(), file.FullName);
                       
                    }
                    else
                        totErrors = totErrors + ", " + errors;
                }
            }
            
            //process return BCC
            var t0 = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t0.Wait();

            appSets appsets = new appSets();
            appsets.setVars();

            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            //string strsql = "select filenamecass from HOR_parse_files_to_CASS where TableName = 'HOR_parse_HNJH_Panel_Roster_Provider' ";
            string strsql = "select filenamecass, Processed from HOR_parse_files_to_CASS where TableName =  " +
                            "'HOR_parse_Involuntary_Disenrollment' and Processed is null";
            DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            foreach (DataRow row in table_BCCToProcess.Rows)
            {
                ProcessBackData(row[0].ToString());

               
                //Update XMPie file name

            }

            //output XMpie
            dbU.ExecuteScalar("delete from HOR_XMPIE_Involuntary_Disenrollment");
            SqlParameter[] sqlParams2;
            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@Date", DateTime.Now.ToString("yyyy-MM-dd")) };

            dbU.ExecuteScalar("HOR_upd_Involuntary_Xmpie",sqlParams2);

            DataTable xmpiedata = dbU.ExecuteDataTable("select * from HOR_XMPIE_Involuntary_Disenrollment order by recnum");
            string pName = location + "Disenrollment_data_for_XMpie.csv";
            createCSV createFilecsv = new createCSV();
                    createFilecsv.printCSV_fullProcess(pName, xmpiedata, "", "Y");


            string reportdate = DateTime.Now.ToString("yyyy-MM-dd");
            string xName = location + "Disenrollment_Summary.xls";
            HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
            processXMPie.pritnSummary(reportdate, "HOR_rpt_HNJH_Involuntary__Summary_Date", xName);


            return errors;
        }
         public string reprint(string location)
         {
             appSets appsets = new appSets();
             appsets.setVars();

             string result = "";
             GlobalVar.dbaseName = "BCBS_Horizon";
             dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
             dbU.ExecuteScalar("delete from HOR_XMPIE_Involuntary_Disenrollment");
             SqlParameter[] sqlParams2;
             sqlParams2 = null;
             sqlParams2 = new SqlParameter[] { new SqlParameter("@Date", DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd")) };

             dbU.ExecuteScalar("HOR_upd_Involuntary_Xmpie", sqlParams2);

             string reportdate = DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd");
             string xName = location + "Disenrollment_Summary.xls";
             HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
             processXMPie.pritnSummary(reportdate, "HOR_rpt_HNJH_Involuntary__Summary_Date", xName);

             return "";
         }
         public string processInvol(string filename)
         {
             int updErrors = 0;
             string errors = "";
             GlobalVar.dbaseName = "BCBS_Horizon";
             dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

             int GRecnum = 1;
             var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
             int recordnumber = 0;
             if (recnum.ToString() == "")
                 GRecnum = 1;
             else
                 GRecnum = Convert.ToInt32(recnum.ToString()) + 1;



             string result = "";
             FileInfo fileInfo = new System.IO.FileInfo(filename);





             //DataTable fromTAB_tmp = loadTABdata(filename);
             DataTable fromTAB_tmp = GetDataTabletFromCSVFile(filename);

             DataTable newTABTable = fromTAB_tmp.Select().Where(x => !x.IsNull(0)).CopyToDataTable();

             foreach (var column in newTABTable.Columns.Cast<DataColumn>().ToArray())
             {
                 if (newTABTable.AsEnumerable().All(dr => dr.IsNull(column)))
                     newTABTable.Columns.Remove(column);
             }

             newTABTable.Columns.Add("FileName").SetOrdinal(0);
             newTABTable.Columns.Add("DateImport").SetOrdinal(0);
             newTABTable.Columns.Add("Recnum").SetOrdinal(0);
             //newTABTable.Columns.Add("FixZip").SetOrdinal(31); 
             foreach (DataRow row in newTABTable.Rows)
             {
                 row["FileName"] = fileInfo.Name;
                 row["DateImport"] = DateTime.Now;
                 row["Recnum"] = GRecnum;
             //    row["FixZip"] = row["SBAD_ZIP1"].ToString().PadLeft(5, '0');
                 GRecnum++;
             }
             //newTABTable.Columns.Remove("SBAD_ZIP1");
             //newTABTable.Columns["FixZip"].ColumnName = "SBAD_ZIP1";
            // newTABTable.Columns.Remove("FixZip");
             GlobalVar.dbaseName = "BCBS_Horizon";
             dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
             dbU.ExecuteScalar("delete from HOR_parse_Involuntary_Disenrollment_tmp");
             foreach (DataRow row in newTABTable.Rows) // Loop over the rows.
             {
                 //dbU.ExecuteScalar("Insert into HOR_parse_Involuntary_Disenrollment_tmp (Recnum, DateImport, Filename, Member_ID,meme_first_Name,meme_Last_Name,MEPE_EFF_DT,MEPE_TERM_DT,SBAD_ADDR1,SBAD_ADDR2,SBAD_ADDR3,sbad_City1,sbad_State1,sbad_ZIP1) values(" +
                 dbU.ExecuteScalar("Insert into HOR_parse_Involuntary_Disenrollment_tmp (Recnum, DateImport, Filename, Member_ID,meme_first_Name,meme_Last_Name,MEPE_TERM_DT,Term_RSN,SBAD_ADDR1,SBAD_ADDR2,SBAD_ADDR3,sbad_City1,sbad_State1,sbad_ZIP1,SBAD_NAME) values(" +
                     "" + row[0].ToString().TrimStart().TrimEnd() +
                     ",'" + row[1].ToString().TrimStart().TrimEnd().Replace("'","") +
                     "','" + row[2].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                     "','" + row[3].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                     "','" + row[4].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                     " " + row[5].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                     "','" + row[6].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                     "','" + row[7].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                     "','" + row[8].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                                          "','" + row[9].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                                          "','" + row[10].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                                          "','" + row[11].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                                          "','" + row[12].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                                          "','" + row[13].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                                          "','" + row[14].ToString().TrimStart().TrimEnd().Replace("'", "''") +
                    "','" + row[15].ToString().TrimStart().TrimEnd() + "')");

             }

             //SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

             //Connection.Open();

             //using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
             //{
             //    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_Involuntary_Disenrollment_tmp]";

             //    try
             //    {
             //        bulkCopy.WriteToServer(newTABTable);
             //    }
             //    catch (Exception ex)
             //    {
             //        errors = errors + ex.Message;    //colid 27   Member Gender
             //        updErrors++;
             //    }
             //}
             //Connection.Close();
             string BCCname = "";
             if (updErrors == 0)
             {
                 try
                 {
                     //replace nulls
                     SqlParameter[] sqlParams2;
                     sqlParams2 = null;
                     sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", "HOR_parse_Involuntary_Disenrollment_tmp") };

                     dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);



                     dbU.ExecuteScalar("Insert into HOR_parse_Involuntary_Disenrollment select * from HOR_parse_Involuntary_Disenrollment_tmp");

                     BCCname = "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - fileInfo.Extension.Length) + "_toBCC.csv";
                     string directoryAfterCass = fileInfo.DirectoryName;  // ProcessVars.InputDirectory + "FromCASS";


                     dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_Involuntary_Disenrollment', GETDATE())");

                     dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                                     newTABTable.Rows.Count + ",'" + BCCname + "','" + fileInfo.Name + "','" + fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "','HOR_parse_Involuntary_Disenrollment','" +
                                                     directoryAfterCass + "','','','','Receive','" + GlobalVar.DateofProcess + "')");
                 }
                 catch (Exception ex)
                 {
                     errors = errors + ex.Message;    //colid 27   Member Gender
                     updErrors++;
                 }
                 sendBCC(fileInfo.DirectoryName, BCCname, fileInfo.Name);
             }

             return errors;
         }
         static IEnumerable<string> ReadAsLines(string filename)
         {
             using (var reader = new StreamReader(filename))
                 while (!reader.EndOfStream)
                     yield return reader.ReadLine();
         }
        public DataTable loadTABdata(string filename)
         {
            
             
            
            //var filename = "tabfile.txt";
             var reader = ReadAsLines(filename);

             var data = new DataTable();

             //this assume the first record is filled with the column names
            var headers = reader.First().Split('\t');
             foreach (var header in headers)
                 data.Columns.Add(header);

             var records = reader.Skip(1);
             foreach (var record in records)
                 data.Rows.Add(record.Split('\t'));

           return data;
         }
        public void sendBCC(string fileDirectory, string Bname, string fname)
        {
            string BCCname = ProcessVars.gDMPs +  Bname;   // "HNJH-PR_" + DateTime.Now.ToString("yyyy-MM-dd") + "_toBCC.csv";
            string pNameT = fileDirectory + "\\" + Bname;
            DataTable table_BCC = dbU.ExecuteDataTable(
             "SELECT Recnum, [SBAD_ADDR1],[SBAD_ADDR2],[SBAD_ADDR3], [SBAD_CITY1] + ', ' + [SBAD_STATE1] + ' ' + [SBAD_ZIP1] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_parse_Involuntary_Disenrollment] where filename ='" + fname + "'");
            //CSV  data===================================================================

            if (File.Exists(pNameT))
                File.Delete(pNameT);

            var fieldnames = new List<string>();
            fieldnames.Add("Recnum");
            fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
            fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
            fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");

            createCSV createcsvT = new createCSV();
            bool resp = createcsvT.addRecordsCSV(pNameT, fieldnames);
            foreach (DataRow row in table_BCC.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < table_BCC.Columns.Count; index++)
                {
                    if (index == 0)
                        rowData.Add(row[index].ToString());

                    else if (index == 1)
                    {
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");  rowData.Add("");
                        rowData.Add(row[index].ToString());
                    }
                    else if (index == 2)
                        rowData.Add(row[index].ToString().Replace(",", " "));
                    else if (index == 3)
                        rowData.Add(row[index].ToString());
                   
                    else if (index == 4)
                    {
                        rowData.Add(""); rowData.Add(""); rowData.Add(row[index].ToString());
                    }
                }
                resp = false;
                resp = createcsvT.addRecordsCSV(pNameT, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            //copy to CASS
            string cassFileName = ProcessVars.gDMPs + Bname;
            File.Copy(pNameT, cassFileName);
            //File.Move(pNameT, fileDirectory +  BCCname);
           
          
            
        }
        public void ProcessBackData(string BCCname)
        {

            int updErrors = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
            if (File.Exists(processedFiles + "\\" +  BCCname.Replace(".csv", "-OUTPUT.csv")))
            {
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + BCCname + "' and Processed is null");
                DataRow dr;

                dr = ds.Tables[0].Rows[0];
                string FFName = dr[0].ToString();
                string TTName = dr[1].ToString();
                string TTDir = dr[2].ToString();
                BackCASS readFilesBackBCC = new BackCASS();
                DataTable QualifiedRecords = readFilesBackBCC.readcsvHNJH(processedFiles + BCCname.Replace(".csv", "-OUTPUT.csv"));
                string resultUPD = "";
                if (QualifiedRecords.Rows.Count > 0)
                {
                    resultUPD = readFilesBackBCC.updateTableSQL_HNJH(QualifiedRecords, "Y", FFName, TTName);
                }
                //string updateCounty = "update HOR_parse_Involuntary_Disenrollment set UpdCounty = SBAD_COUNTY " +
                //                        "where FileName = '" + FFName +
                //                        "' and SBAD_COUNTY <> UpdCounty and (UpdCounty is null or UpdCounty = '')";
                //dbU.ExecuteNonQuery(updateCounty);

                //READ NON DELIVERABLE
                string files = "";
                string erroFile = ProcessVars.gODMPsMedicaid + BCCname.Replace(".csv", "-NON-DELIVERABLE.csv");
                if (File.Exists(erroFile))
                {
                    DataTable NonD_Records = readFilesBackBCC.readcsvError(erroFile);

                    if (NonD_Records.Rows.Count > 0)
                    {
                        if (files != "")
                        {
                            resultUPD = readFilesBackBCC.updateTableSQL_HNJH(NonD_Records, "N", FFName, TTName);
                            string strsql = "update  HOR_parse_Involuntary_Disenrollment set dl = 'N' where city = '' and state = ''  and FileName = '" + FFName + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                        foreach (DataRow row in NonD_Records.Rows)
                        {
                            //TextBox1.Text = row["Recordnum"].ToString();
                            string strsql = "update  HOR_parse_Involuntary_Disenrollment set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                    }
                }

                if (updErrors == 0)
                {
                    //good and bad records to update
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };

                    DataSet ds2 = new DataSet();
                    try
                    {

                        ds2 = dbU.ExecuteDataSet("HOR_upd_tot_Parse_NonDeliverables", sqlParams2);
                    }
                    catch (Exception ez)
                    {
                        var errors = ez.Message;
                    }
                    DataRow dr2;
                    string toND = "";
                    string totOK = "";
                    if (ds2 != null)
                    {
                        dr2 = ds2.Tables[0].Rows[0];
                        toND = dr2[1].ToString();
                        totOK = dr2[2].ToString();
                    }
                    //string OutputpName = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed\" + FFName.ToUpper().Replace(".PDF", ".csv");
                    //string pNameCass = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                    //File.Copy(pNameCass, OutputpName);  // prevent user open before ready

                    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set processed = 'Y', CASSReceiveDate =  GETDATE() , " +
                                        " RecordsOK = " + totOK + ", " +
                                        " RecordsNonDeliverable = " + toND + " where filename = '" + FFName + "'");

                }
             
            }
        }

        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {

            DataTable csvData = new DataTable();

            try
            {

                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
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
