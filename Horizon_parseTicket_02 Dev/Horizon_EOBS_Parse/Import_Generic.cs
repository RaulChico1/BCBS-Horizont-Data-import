using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Configuration;
using System.Text.RegularExpressions;

namespace Horizon_EOBS_Parse
{
    public class Import_Generic
    {
        string errors = "";
        DBUtility dbU;

        int errorcount = 0;
        int Recnum = 1;
        int GRecnum = 1;
        int currLine = 0;
        int seqBundle = 0;
        int FileSeq = 0;



        public string ProcessThisFileXLS(string filename, string locationLocal, string dateProcess, string tableName, string zipName)        
        {
            string errors = "";

            int updErrors = 0;
           
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
            dbU.ExecuteNonQuery("delete from HOR_parse_files_to_CASS where filename = '" + fileInfo.Name + "' and tablename = '" + tableName + "'");
            var filesInCass = dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where filename = '" + fileInfo.Name + "'");
            int totrecs = Convert.ToInt32(filesInCass.ToString());
            if (totrecs != 0)
                errors = "File " + fileInfo.Name + " exist in HOR_parse_files_to_CASS";
            if (errors == "")
            {
                dbU.ExecuteNonQuery("delete from " + tableName + " where filename = '" + fileInfo.Name + "'");


                //DataTable fromXLSTmp = loadXLSX(filename);
                DataTable fromXLS = loadXLSX(filename);
                //foreach (var column in fromXLSTmp.Columns.Cast<DataColumn>().ToArray())
                //{
                //    if (fromXLSTmp.AsEnumerable().All(dr => dr.IsNull(column)))
                //        fromXLSTmp.Columns.Remove(column);
                //}


               // DataTable fromXLS = fromXLSTmp.Select().Where(x => !x.IsNull(0)).CopyToDataTable();

                //foreach (var column in fromXLS.Columns.Cast<DataColumn>().ToArray())
                //{
                //    if (fromXLS.AsEnumerable().All(dr => dr.IsNull(column)))
                //        fromXLS.Columns.Remove(column);
                //}
                fromXLS.Columns.Add("ImportDate").SetOrdinal(0);
                fromXLS.Columns.Add("FileName").SetOrdinal(0);
                fromXLS.Columns.Add("Recnum").SetOrdinal(0);
                
                //fromXLS.Columns.Add("Telephone");
                //fromXLS.Columns.Add("Gender");
                //fromXLS.Columns.Add("Age");
                //fromXLS.Columns.Add("PcpID");
                //fromXLS.Columns.Add("PcpName");
                //fromXLS.Columns.Add("PcpPhone");

                fromXLS.Columns.Add("UpdAddr1");
                fromXLS.Columns.Add("UpdAddr2");
                fromXLS.Columns.Add("UpdAddr3");
                fromXLS.Columns.Add("UpdAddr4");
                fromXLS.Columns.Add("UpdAddr5");
                fromXLS.Columns.Add("UpdCity");
                fromXLS.Columns.Add("UpdState");
                fromXLS.Columns.Add("UpdZip");
                fromXLS.Columns.Add("UpdCounty");
                fromXLS.Columns.Add("UpdLat");
                fromXLS.Columns.Add("UpdLong");
                fromXLS.Columns.Add("IMBChar");
                fromXLS.Columns.Add("IMBDigit");
                fromXLS.Columns.Add("DL");
                fromXLS.Columns.Add("JobNumber");
                foreach (DataRow row in fromXLS.Rows)
                {
                    row["FileName"] = fileInfo.Name;
                    row["ImportDate"] = DateTime.Now;
                    row["Recnum"] = GRecnum;
                    row["JobNumber"] = zipName;
                    GRecnum++;
                }
              
                dbU.ExecuteScalar("delete from " + tableName + "_tmp");
              
                try
                {
                    fromXLS.Columns.Add("DoNotSend", typeof(String)).SetOrdinal(8);
                }
                catch
                {
                   
                }
                try
                {
                    fromXLS.Columns.Add("cnt", typeof(String)).SetOrdinal(3);
                }
                catch
                {
                   
                }
                fromXLS.Columns["Member_ID"].SetOrdinal(32);

            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                bulkCopy.DestinationTableName = "[dbo].[" +  tableName + "_tmp]";

                try
                {
                    bulkCopy.WriteToServer(fromXLS);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
            }
            Connection.Close();
            SqlParameter[] sqlParams2;
            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", tableName + "_tmp") };

            dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);


            string pNameT = "";
            string BCCname = "";
            if (updErrors == 0)
            {
                try
                {
                    dbU.ExecuteScalar("Insert into " + tableName + " select * from " + tableName + "_tmp");

                    pNameT = locationLocal + "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                    BCCname = "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                    string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";

                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'" + tableName + "', GETDATE())");

                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                                    fromXLS.Rows.Count + ",'" + BCCname + "','" + fileInfo.Name + "','" + 
                                                    fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "','" + tableName + "','" +
                                                    directoryAfterCass + "','','','','Receive','" + GlobalVar.DateofProcess + "')");

                    dbU.ExecuteScalar("delete from " + tableName + "_tmp");

                    File.Move(fileInfo.FullName, fileInfo.Directory + "\\__" + fileInfo.Name);

                }
                catch (Exception ex)
                {
                    var excc = ex.Message;
                }
            }
            else
            {
                var errorsss = "here";
            }


            DataTable table_BCC = dbU.ExecuteDataTable(
             "SELECT Recnum, rtrim(ltrim([FIRSTNAME])) + ' ' +  rtrim(ltrim([LASTNAME])) as Name,[AddressLine1],[AddressLine2], [CITY] + ', ' + [STATE] + ' ' + [ZIP] as CSZ FROM " + tableName + " where filename ='" + fileInfo.Name + "'");
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
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); ; rowData.Add("");
                        rowData.Add(row[index].ToString());
                    }
                    else if (index == 2)
                        rowData.Add(row[index].ToString());
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
            string cassFileName = ProcessVars.gDMPs + BCCname;
            File.Copy(pNameT, cassFileName);
            //File.Move(pNameT, fileInfo.Directory + @"\" +BCCname);
            
            var t0 = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t0.Wait();

            ProcessBackData(tableName);



            }
            return errors;
        }

        public string ProcessBackData(string tableName)
        {

            appSets appsets = new appSets();
            appsets.setVars();

            BackCASS processRedturns = new BackCASS();
            HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            //string strsql = "select filenamecass from HOR_parse_files_to_CASS where TableName = 'HOR_parse_HNJH_Panel_Roster_Provider' ";
            string strsql = "select filenamecass, Processed from HOR_parse_files_to_CASS where TableName =  '" + tableName + "' and Processed is null";
            DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            foreach (DataRow row in table_BCCToProcess.Rows)
            {
                string Ftxtname = row[0].ToString().Replace("_toBCC.csv", "");
                if (DBNull.Value.Equals(row[1]))
                {
                    result = ProcessReturnfromBCC(row[0].ToString()); 
                }
                //Update XMPie file name

            }
            return result;
        }

        public DataTable loadXLSX(string filename)
        {
            DataTable dtSchema = new DataTable();
            var connectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0}; Extended Properties=Excel 12.0;", filename);
            string sheetName = "";

            using (OleDbConnection conn = new OleDbConnection(connectionString))
            {
                conn.Open();
                dtSchema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                foreach (DataRow row in dtSchema.Rows)
                {
                    if (!row["TABLE_NAME"].ToString().Contains("FilterDatabase"))
                    {
                        // sheetNames.Add(new SheetName() { sheetName = row["TABLE_NAME"].ToString(), sheetType = row["TABLE_TYPE"].ToString(), sheetCatalog = row["TABLE_CATALOG"].ToString(), sheetSchema = row["TABLE_SCHEMA"].ToString() });
                        //if (group == "Commercial")
                        //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");   // was 1  when carry 2 tabs
                        //else
                        //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");
                        sheetName = row["TABLE_NAME"].ToString();
                    }
                }


                //if (group == "Commercial")
                //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");   // was 1  when carry 2 tabs
                //else
                //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");
            }

            DataTable XLSdataTable = new DataTable();
            var adapter = new OleDbDataAdapter("SELECT * FROM [" + sheetName + "]", connectionString);

            adapter.Fill(XLSdataTable);
            return XLSdataTable;
        }
        public string ProcessReturnfromBCC(string fileName)
        {
            int updErrors = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
            if (File.Exists(processedFiles + fileName.Replace(".csv", "-OUTPUT.csv")))
            {
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + fileName + "' and Processed is null");
                DataRow dr;

                dr = ds.Tables[0].Rows[0];
                string FFName = dr[0].ToString();
                
                string TTName = dr[1].ToString();
                if (TTName == "HOR_parse_CPlans")
                    FFName = FFName.Replace(".csv", ".pdf");
                string TTDir = dr[2].ToString();
                BackCASS readFilesBackBCC = new BackCASS();
                DataTable QualifiedRecords = readcsvGeneric(processedFiles + fileName.Replace(".csv", "-OUTPUT.csv"));
                string resultUPD = "";
                if (QualifiedRecords.Rows.Count > 0)
                {
                    resultUPD = updateTableSQL_Generic(QualifiedRecords, "Y", FFName, TTName);
                }
                //string updateCounty = "update HOR_parse_HNJH_WK set UpdCounty = SBAD_COUNTY " +
                //                        "where FileName = '" + FFName +
                //                        "' and SBAD_COUNTY <> UpdCounty and (UpdCounty is null or UpdCounty = '')";
                //dbU.ExecuteNonQuery(updateCounty);

                //READ NON DELIVERABLE
                string files = "";
                string erroFile = ProcessVars.gODMPsMedicaid + fileName.Replace(".csv", "-NON-DELIVERABLE.csv");
                if (File.Exists(erroFile))
                {
                    DataTable NonD_Records = readcsvErrorGeneric(erroFile);

                    if (NonD_Records.Rows.Count > 0)
                    {
                       
                        foreach (DataRow row in NonD_Records.Rows)
                        {
                          
                            string strsql = "update  " + TTName + " set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
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
                                        " RecordsNonDeliverable = " + toND + " where filename = '" + FFName.Replace(".pdf", ".csv") + "'");
                    //check files HOR_parse_CPlans
                }
                else
                {
                    var msg = updErrors;
                }
            }



            return updErrors.ToString();
        }
        public DataTable readcsvGeneric(string fileName)
        {
            DataTable dataToUpdate = Result_data_Table();
            dataToUpdate.Columns.Add("IMBChar", typeof(String));
            dataToUpdate.Columns.Add("IMBDig", typeof(String));
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
                    else if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,St,ZIP+4,De,Re,Intelligent Mail barcode,Intelligent Mail barcode DIG")
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

            if (dataToUpdate.Rows.Count == 0)
                line = "No Data ???? from BCC";


            return dataToUpdate;

        }

        public string updateTableSQL_Generic(DataTable inputdata, string to_DL, string ffName, string TableName)
        {
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from tmp_From_CASS_Generic");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                //bulkCopy.DestinationTableName =
                //    "[dbo].[Tempo_fsaData]";
                bulkCopy.DestinationTableName = "[dbo].[tmp_From_CASS_Generic]";

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(inputdata);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;
                    
                }
            }
            Connection.Close();
            try
            {
                SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@dataTable", TableName), new SqlParameter("@DLvalue", to_DL), new SqlParameter("@ffName", ffName) };

                DataSet ds2 = new DataSet();
             
                    ds2 = dbU.ExecuteDataSet("Update_From_CassGeneric", sqlParams2);

            }
            catch (Exception ex)
            {
                LogWriter logerror = new LogWriter();
                logerror.WriteLogToTable("Update From Cass", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Update", "TableName :  " + TableName + " filename " + ffName + " " + ex.Message, "email");
                errors = errors + ex.Message;
                
            }
            return errors;
        }
        public DataTable readcsvErrorGeneric(string fileName)
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
                    if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,St,ZIP+4,De,Re")

                        valueOk = 1;
                    else if (line.Replace("\"", "") == "RECNO,RETURN_FIELD_01,RETURN_FIELD_02,RETURN_FIELD_03,RETURN_FIELD_04,RETURN_FIELD_05,RETURN_FIELD_06,RETURN_FIELD_07,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,RETURN_FIELD_13,RETURN_FIELD_14,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,State,ZIP+4,Delivery Point,Return Code")
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
                    {//  WAS 5
                        if (xMatch == 1)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");
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
            //newt.Columns.Add("DeliveryPoint");
            //newt.Columns.Add("CassReturn");

            return newt;
        }
    }
}
