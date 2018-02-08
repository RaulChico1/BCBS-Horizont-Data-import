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


namespace Horizon_EOBS_Parse
{
    public class NParse_DirectMail
    {
         DBUtility dbU;
        DataTable dt = new DataTable();
        public string importDirectMail(string location)
        {
            string totErrors = "";
            DirectoryInfo xlss = new DirectoryInfo(location);

            FileInfo[] files = xlss.GetFiles("*.txt");

            string errors = "";
            foreach (FileInfo file in files)
            {
                string partFilename = file.Name.Substring(0, 3);
                if (partFilename.IndexOf("_") == -1 && partFilename.IndexOf("._") == -1)
                {
                    errors = processData(file.FullName);
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
                            "'HOR_DirectMail' and Processed is null";
            DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            foreach (DataRow row in table_BCCToProcess.Rows)
            {
                ProcessBackData(row[0].ToString(), location);


                //Update XMPie file name

            }

            //output XMpie
            //dbU.ExecuteScalar("delete from HOR_XMPIE_Involuntary_Disenrollment");
            //SqlParameter[] sqlParams2;
            //sqlParams2 = null;
            //sqlParams2 = new SqlParameter[] { new SqlParameter("@Date", DateTime.Now.ToString("yyyy-MM-dd")) };

            //dbU.ExecuteScalar("HOR_upd_Involuntary_Xmpie", sqlParams2);

            //string reportdate = DateTime.Now.ToString("yyyy-MM-dd");
            //string xName = location + "Disenrollment_Summary.xls";
            //HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
            //processXMPie.pritnSummary(reportdate, "HOR_rpt_HNJH_Involuntary__Summary_Date", xName);


            return errors;
        }

        public string processData(string filename)
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





            DataTable fromTAB_tmp = loadPipedata(filename);

            DataTable newTABTable = fromTAB_tmp.Select().Where(x => !x.IsNull(0)).CopyToDataTable();

            foreach (var column in newTABTable.Columns.Cast<DataColumn>().ToArray())
            {
                if (newTABTable.AsEnumerable().All(dr => dr.IsNull(column)))
                    newTABTable.Columns.Remove(column);
            }

            newTABTable.Columns.Add("DateImport").SetOrdinal(0);
            newTABTable.Columns.Add("FileName").SetOrdinal(0);
            newTABTable.Columns.Add("Recnum").SetOrdinal(0);
            newTABTable.Columns.Add("FullName").SetOrdinal(17);
            newTABTable.Columns.Add("ST").SetOrdinal(22);

            newTABTable.Columns.Add("IMAGE_SELECT").SetOrdinal(25);
           // newTABTable.Columns.Add("IMAGE_SELECT");
            newTABTable.Columns.Add("UpdAddr1");
            newTABTable.Columns.Add("UpdAddr2");
            newTABTable.Columns.Add("UpdAddr3");
            newTABTable.Columns.Add("UpdAddr4");
            newTABTable.Columns.Add("UpdAddr5");
            newTABTable.Columns.Add("UpdCity");
            newTABTable.Columns.Add("UpdState");
            newTABTable.Columns.Add("UpdZip");
            newTABTable.Columns.Add("DL");
            newTABTable.Columns.Add("IMBChar");
            newTABTable.Columns.Add("IMBDig");
            newTABTable.Columns.Add("opr_code");
            newTABTable.Columns.Add("opr_date");
            newTABTable.Columns.Add("opr_Status");
            newTABTable.Columns.Add("opr_Update");

             DataColumnCollection columns = newTABTable.Columns;

                if (columns.Contains("zip code"))
                {
                   newTABTable.Columns.Remove("zip code");
                }
            newTABTable.Columns["ZIP(ZIP+4)"].ColumnName = "ZIP";
            foreach (DataRow row in newTABTable.Rows)
            {
                row["FileName"] = fileInfo.Name;
                row["DateImport"] = DateTime.Now;
                row["Recnum"] = GRecnum;
                row["FullName"] = (row["PREFIX"].ToString().Trim() + " " +
                                  row["FIRST_NAME"].ToString().Trim() + " " +
                                  row["MIDDEL_NAME"].ToString().Trim() + " " +
                                  row["LAST_NAME"].ToString().Trim() + " " +
                                  row["SUFFIX"].ToString().Trim()).Trim();  

                
                GRecnum++;
            }
            //newTABTable.Columns.Remove("SBAD_ZIP1");
            //newTABTable.Columns["FixZip"].ColumnName = "SBAD_ZIP1";
            DataView view = new DataView(newTABTable);
            DataTable distinctValues = view.ToTable(true, "STATE");
            foreach (DataRow row in distinctValues.Rows)
            {
                string LongState = row[0].ToString().Trim();
                string state = (string)dbU.ExecuteScalar("Select Cod from US_States where State = '" + LongState + "'");
                if (state != "")
                {
                    foreach (DataRow rowt in newTABTable.Rows)
                    {
                        if (rowt["STATE"].ToString() == LongState)
                            rowt["ST"] = state;
                        //else
                        //    rowt["ST"] = rowt["STATE"].ToString();
                    }

                }
            }
            foreach (DataRow rowb in newTABTable.Rows)
            {
                if (rowb["ST"].ToString() == "")
                    rowb["ST"] = rowb["STATE"].ToString();
            }

            //createCSV createcsv = new createCSV();
            //createcsv.printCSV_fullProcess(@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\DirectMail\test.csv", newTABTable, "", "N");
           

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from HOR_DirectMail_TMP");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                bulkCopy.DestinationTableName = "[dbo].[HOR_DirectMail_TMP]";

                try
                {
                    bulkCopy.WriteToServer(newTABTable);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
            }
            Connection.Close();
            string BCCname = "";
            if (updErrors == 0)
            {
                try
                {
                    //replace nulls
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", "HOR_DirectMail_TMP") };

                    dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);



                    dbU.ExecuteScalar("Insert into HOR_DirectMail select * from HOR_DirectMail_TMP");

                    BCCname = "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - fileInfo.Extension.Length) + "_toBCC.csv";
                    string directoryAfterCass = fileInfo.DirectoryName;  // ProcessVars.InputDirectory + "FromCASS";


                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_DirectMail', GETDATE())");

                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                                    newTABTable.Rows.Count + ",'" + BCCname + "','" + fileInfo.Name + "','" + fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "','HOR_DirectMail','" +
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
         public DataTable loadPipedata(string filename)
         {
            
             
            
            //var filename = "tabfile.txt";
             var reader = ReadAsLines(filename);

             var data = new DataTable();

             //this assume the first record is filled with the column names
            var headers = reader.First().Split('|');
             foreach (var header in headers)
                 data.Columns.Add(header);

             var records = reader.Skip(1);
             foreach (var record in records)
                 data.Rows.Add(record.Split('|'));

           return data;
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
            string BCCname = ProcessVars.gDMPs + Bname;   // "HNJH-PR_" + DateTime.Now.ToString("yyyy-MM-dd") + "_toBCC.csv";
            string pNameT = fileDirectory + "\\" + Bname;
            DataTable table_BCC = dbU.ExecuteDataTable(
             "SELECT Recnum, FullName,ADDRESS_1,ADDRESS_2, [CITY] + ', ' + [STATE] + ' ' + [ZIP] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_DirectMail] where filename ='" + fname + "'");
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
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
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
        public void PrintCSV_only(string location, string processDate)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            dbU.ExecuteNonQuery("Delete from HOR_DirectMail where convert(date,Dateimport) = '" + processDate + "' and filename like 'SEDDS_%'");

            DataTable seedsVersions = dbU.ExecuteDataTable("Select distinct FileName, Message_code, hospital_System from HOR_DirectMail where convert(date,Dateimport) = '" + processDate + "'");
            foreach (DataRow row in seedsVersions.Rows)
            {
                SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@MESSAGE_CODE", row[1].ToString())
                    , new SqlParameter("@IMAGE_SELECT", row[2].ToString())  , new SqlParameter("@FileName", row[0].ToString()) };
                try
                {
                    dbU.ExecuteNonQuery("HOR_upd_DirectMail_Seeds", sqlParams2);
                }
                catch (Exception ez)
                {
                    var errors = ez.Message;
                }
                
            }

            // int GRecnum = 1;
            //var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            //int recordnumber = 0;
            //if (recnum.ToString() == "")
            //    GRecnum = 1;
            //else
            //    GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            //DataTable seedsVersionsRecnum = dbU.ExecuteDataTable("Select Recnum from HOR_DirectMail where convert(date,Dateimport) = '" + processDate + "' and filename like 'SEDDS_%' order by recnum");
            //foreach (DataRow row in seedsVersionsRecnum.Rows)
            //{
            //     dbU.ExecuteNonQuery("update HOR_DirectMail set recnum = " + GRecnum + " where recnum  = " + Int32.Parse(row[0].ToString()));
            //    GRecnum++;
            //}

            // dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_DirectMail', GETDATE())");


            string pName = location + "DirectMail_" + processDate.Replace("-","_") + ".csv";
            DataTable dataTocsv = dbU.ExecuteDataTable("Select RIGHT('000000000' + CAST(Recnum AS VARCHAR(9)),9) as Recnum, PRTY_ID, TRACKING_CODE, Message_code, Content_ID, HOSPITAL_SYSTEM, UpdAddr1 as Name, UpdAddr5 as Address1, UpdAddr2 as Address2, UpdCity as City, UpdState as State, UpdZip as Zip " +
                                    "from HOR_DirectMail where DL = 'Y'  and convert(date,Dateimport) = '" + processDate + "' order by recnum");
            createCSV createFilecsv = new createCSV();
            createFilecsv.printCSV_fullProcess(pName, dataTocsv, "", "N");

            pName = location + "DirectMail_" + processDate.Replace("-", "_") + "_DL_N.csv";
            DataTable data_N_Tocsv = dbU.ExecuteDataTable("Select Recnum, PRTY_ID, TRACKING_CODE, Message_code, Content_ID,HOSPITAL_SYSTEM,  UpdAddr1 as Name, UpdAddr5 as Address1, UpdAddr2 as Address2, UpdCity as City, UpdState as State, UpdZip as Zip, Filename, DateImport " +
                                   "from HOR_DirectMail where DL <> 'Y'  and convert(date,Dateimport) = '" + processDate + "'");
            if (data_N_Tocsv.Rows.Count > 0)
            {
                createFilecsv.printCSV_fullProcess(pName, data_N_Tocsv, "", "N");
            }

            DataTable dataVersions = dbU.ExecuteDataTable("Select distinct Message_code, HOSPITAL_SYSTEM from HOR_DirectMail where DL = 'Y'  and convert(date,Dateimport) = '" + processDate + "'");
            foreach (DataRow row in dataVersions.Rows)
            {
                string strsql = "select  RIGHT('000000000' + CAST(Recnum AS VARCHAR(9)),9) as Recnum, PRTY_ID, TRACKING_CODE, HOSPITAL_SYSTEM, Content_ID,Image_Select,  UpdAddr1 as Name, UpdAddr5 as Address1, UpdAddr2 as Address2, UpdCity as City, UpdState as State, UpdZip as Zip from HOR_DirectMail where recnum in " +
                                "(select top 10  recnum from HOR_DirectMail  where DL = 'Y'  and message_code = '" + row[0].ToString() + "' and " +
                                "convert(date,Dateimport) = '" + processDate + "' and IMAGE_SELECT = '" + row[1].ToString() + "' order by newid())";
                DataTable recVersion = dbU.ExecuteDataTable(strsql);
                if (recVersion.Rows.Count > 0)
                {
                    pName = location + "DirectMail_Samples_" + processDate.Replace("-", "_") + "_Version_" + row[0].ToString() + "_" + row[1].ToString() + ".csv";
                    createFilecsv.printCSV_fullProcess(pName, recVersion, "", "N");
                }
            }
            foreach (DataRow row in dataVersions.Rows)
            {
                string strsql = "select  RIGHT('000000000' + CAST(Recnum AS VARCHAR(9)),9) as Recnum, PRTY_ID, TRACKING_CODE, Message_code, Content_ID, HOSPITAL_SYSTEM, UpdAddr1 as Name, UpdAddr5 as Address1, UpdAddr2 as Address2, UpdCity as City, UpdState as State, UpdZip as Zip from HOR_DirectMail where " +
                                "DL = 'Y'  and message_code = '" + row[0].ToString() + "' and " +
                                "convert(date,Dateimport) = '" + processDate + "' order by recnum";
                DataTable recVersion = dbU.ExecuteDataTable(strsql);
                if (recVersion.Rows.Count > 0)
                {
                    pName = location + "DirectMail_" + processDate.Replace("-", "_") + "_Version_" + row[0].ToString() + ".csv";
                    createFilecsv.printCSV_fullProcess(pName, recVersion, "", "N");
                }
            }


        }
        public void ProcessBackData(string BCCname, string location)
        {

            int updErrors = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
            if (File.Exists(processedFiles + "\\" + BCCname.Replace(".csv", "-OUTPUT.csv")))
            {
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + BCCname + "' and processed is null");
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
                            string strsql = "update  HOR_DirectMail set dl = 'N' where city = '' and state = ''  and FileName = '" + FFName + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                        foreach (DataRow row in NonD_Records.Rows)
                        {
                            //TextBox1.Text = row["Recordnum"].ToString();
                            string strsql = "update  HOR_DirectMail set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                    }
                }

                //if (updErrors == 0)
                //{
                //    //good and bad records to update
                //    SqlParameter[] sqlParams2;
                //    sqlParams2 = null;
                //    sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };

                //    DataSet ds2 = new DataSet();
                //    try
                //    {

                //        ds2 = dbU.ExecuteDataSet("HOR_upd_tot_Parse_NonDeliverables", sqlParams2);
                //    }
                //    catch (Exception ez)
                //    {
                //        var errors = ez.Message;
                //    }
                //    DataRow dr2;
                //    string toND = "";
                //    string totOK = "";
                //    if (ds2 != null)
                //    {
                //        dr2 = ds2.Tables[0].Rows[0];
                //        toND = dr2[1].ToString();
                //        totOK = dr2[2].ToString();
                //    }
                //    //string OutputpName = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed\" + FFName.ToUpper().Replace(".PDF", ".csv");
                //    //string pNameCass = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                //    //File.Copy(pNameCass, OutputpName);  // prevent user open before ready

                //    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set processed = 'Y', CASSReceiveDate =  GETDATE() , " +
                //                        " RecordsOK = " + totOK + ", " +
                //                        " RecordsNonDeliverable = " + toND + " where filename = '" + FFName + "'");

                //    string pName = location +  FFName.Substring(0,FFName.Length - 4) + "_FINAL.csv";
                //    DataTable dataTocsv = dbU.ExecuteDataTable("Select RIGHT('000000000' + CAST(Recnum AS VARCHAR(9)),9) as Recnum, PRTY_ID, TRACKING_CODE, Message_code, Content_ID, UpdAddr1 as Name, UpdAddr5 as Address1, UpdAddr2 as Address2, UpdCity as City, UpdState as State, UpdZip as Zip " +
                //                            "from HOR_DirectMail where DL = 'Y'  and FileName = '" + FFName + "' order by recnum");
                //   createCSV createFilecsv = new createCSV();
                //   createFilecsv.printCSV_fullProcess(pName, dataTocsv, "", "N");

                //   pName = location + FFName.Substring(0, FFName.Length - 4) + "_DL_N.csv";
                //   DataTable data_N_Tocsv = dbU.ExecuteDataTable("Select Recnum, PRTY_ID, TRACKING_CODE, Message_code, Content_ID, UpdAddr1 as Name, UpdAddr5 as Address1, UpdAddr2 as Address2, UpdCity as City, UpdState as State, UpdZip as Zip, Filename, DateImport " +
                //                          "from HOR_DirectMail where DL <> 'Y'  and FileName = '" + FFName + "'");
                //    if(data_N_Tocsv.Rows.Count > 0)
                //    {
                //        createFilecsv.printCSV_fullProcess(pName, data_N_Tocsv, "", "N");
                //    }

                //    DataTable dataVersions = dbU.ExecuteDataTable("Select distinct Message_code from HOR_DirectMail where DL = 'Y'  and FileName = '" + FFName + "'");
                //    foreach (DataRow row in dataVersions.Rows)
                //    {
                //        string strsql = "select Recnum, PRTY_ID, TRACKING_CODE, Message_code, Content_ID, UpdAddr1 as Name, UpdAddr5 as Address1, UpdAddr2 as Address2, UpdCity as City, UpdState as State, UpdZip as Zip from HOR_DirectMail where recnum in " +
                //                        "(select top 10  recnum from HOR_DirectMail  where DL = 'Y'  and message_code = '" + row[0].ToString() + "' and " +
                //                        "FileName = '" + FFName + "' order by newid())";
                //        DataTable recVersion = dbU.ExecuteDataTable(strsql);
                //        if (recVersion.Rows.Count > 0)
                //        {
                //            pName = location + FFName.Substring(0, FFName.Length - 4) + "_Version_" + row[0].ToString() + ".csv";
                //            createFilecsv.printCSV_fullProcess(pName, recVersion, "", "N");
                //        }
                //    }
                //}

            }
        }

    }
}
