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

namespace Horizon_EOBS_Parse
{
    public class BackCASS
    {
        string errors = "";
        int updErrors = 0;
        DBUtility dbU;
        public string ProcessFiles(string files)
        {
            ProcessVars.serviceIsrunning = true;
            //autoEvent.WaitOne(1000 * 60 * 3, false);
            string result = FilestoProcess(files);
            ProcessVars.serviceIsrunning = false;
            SendEmailErrors();
            return result;
        }
        public string FilestoProcess(string files)
        {

            string test = "";
            if (Directory.Exists(ProcessVars.gODMPs))
            {
                DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPs);
                FileInfo[] FilesReady = processedFiles.GetFiles("*.csv");
                foreach (FileInfo file in FilesReady)
                {
                    if (file.Name.IndexOf("toBCC-OUTPUT.csv") > 0)   // && file.Name.Contains("SBC")
                    {
                        if (file.Name.Substring(0, 8) == "UCDSQ005")
                            test = "1";
                        try
                        {
                            string error = updateDbase(file.FullName, files);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }

                }


            }
            return errors;
        }
        public string updateDbase(string fileName, string files)
        {
           
            updErrors = 0;
            FileInfo fileInfo = new System.IO.FileInfo(fileName);

            bool GBill = (fileInfo.Name.Substring(0, 3) == "EPB") ? true : false;
            bool CBill = (fileInfo.Name.Substring(0, 5) == "EP0GH") ? true : false;
            bool CR2 = (fileInfo.Name.Substring(0, 3) == "CR_") ? true : false;
            bool MBA = (fileInfo.Name.Substring(0, 3) == "MBA") ? true : false;
            bool SBC = (fileInfo.Name.Substring(0, 3) == "SBC") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "SMN") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            string fName = fileInfo.Name.Substring(0, fileInfo.Name.IndexOf("-OUTPUT")) + ".csv";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + fName + "' and Processed is null"));
            if (FileCount == 1)
            {
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + fName + "' and Processed is null");
                DataRow dr;

                dr = ds.Tables[0].Rows[0];
                string FFName = dr[0].ToString();
                string TTName = dr[1].ToString();
                string TTDir = dr[2].ToString();
                DataTable QualifiedRecords = readcsv(fileName);
                string resultUPD = "";
                File.Copy(fileName, ProcessVars.InputDirectory + @"\fromBCC\\" + fileInfo.Name, true);
                if (QualifiedRecords.Rows.Count > 0)
                {
                    resultUPD = updateTableSQL(QualifiedRecords, "Y", FFName, TTName);

                }
                //READ NON DELIVERABLE
                string erroFile = fileInfo.DirectoryName + "\\" + fileInfo.Name.Substring(0, fileInfo.Name.IndexOf("-OUTPUT")) + "-NON-DELIVERABLE.csv";
                if (File.Exists(erroFile))
                {
                    FileInfo fileInfoError = new System.IO.FileInfo(erroFile);
                    File.Copy(erroFile, ProcessVars.InputDirectory + @"\fromBCC\\" + fileInfoError.Name, true);
                    DataTable NonD_Records = readcsvError(erroFile);

                    if (NonD_Records.Rows.Count > 0)
                    {
                        if (files != "")
                        {
                            resultUPD = updateTableSQL(NonD_Records, "Y", FFName, TTName);
                            string strsql = "update  HOR_parse_" + files + " set dl = 'N' where city = '' and state = ''  and FName = '" + FFName + "'";
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
                        if (FFName.IndexOf("CR_") == 0 || FFName.IndexOf("MBA") == 0 || FFName.IndexOf("SMN") == 0 || FFName.IndexOf("PNO") == 0)
                            ds2 = dbU.ExecuteDataSet("HOR_upd_tot_Parse_NonDeliverables_CR2", sqlParams2);  // because FName
                        else
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
                        if (ds2.Tables[0].Rows.Count > 0)
                        {
                            dr2 = ds2.Tables[0].Rows[0];
                            toND = dr2[1].ToString();
                            totOK = dr2[2].ToString();
                        }
                        else
                        {
                            var noRows = "no rows...";

                        }
                    }


                    //pritn file to SCI
                    SqlParameter[] sqlParams;
                    sqlParams = null;
                    sqlParams = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };
                    string spName = "";
                    if (GBill || CBill)
                        spName = "HOR_rpt_PARSE_cbILLSto_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";
                    else if (CR2)
                        spName = "HOR_rpt_PARSE_CR2to_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";
                    else if (MBA)
                        spName = "HOR_rpt_PARSE_CR2to_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";
                    else if (SBC)
                        spName = "HOR_rpt_PARSE_SBCto_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";
                    else
                        spName = "HOR_rpt_PARSE_to_SCI";
                    if (TTName == "HOR_Fraud")
                        spName = "HOR_rpt_PARSE_Fraud_to_SCI";

                    DataTable datato_SCI = dbU.ExecuteDataTable(spName, sqlParams);
                    if (datato_SCI.Rows.Count > 0)
                    {
                        createCSV createcsv = new createCSV();
                        //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                        string pName = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                        if (File.Exists(pName))
                            File.Delete(pName);
                        var fieldnames = new List<string>();
                        for (int index = 0; index < datato_SCI.Columns.Count; index++)
                        {
                            fieldnames.Add(datato_SCI.Columns[index].ColumnName);
                        }
                        bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                        foreach (DataRow row in datato_SCI.Rows)
                        {

                            var rowData = new List<string>();
                            for (int index = 0; index < datato_SCI.Columns.Count; index++)
                            {
                                rowData.Add(row[index].ToString());
                            }
                            resp = false;
                            resp = createcsv.addRecordsCSV(pName, rowData);
                            //if (UpdSQL != "")
                            //    dbU.ExecuteScalar(UpdSQL + row[0]);
                        }

                    }
                    if (CR2 || MBA || SBC)
                    {
                        string OutputpName = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed\" + FFName.ToUpper().Replace(".PDF", ".csv");
                        string pNameCass = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                        File.Copy(pNameCass, OutputpName);  // prevent user open before ready
                    }
                    if (MBA)
                    {
                        //make zip???
                        //string OutputpName = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed\" + FFName.ToUpper().Replace(".PDF", ".csv");
                        //string pNameCass = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                        //File.Copy(pNameCass, OutputpName);  // prevent user open before ready
                    }
                    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set processed = 'Y', CASSReceiveDate =  GETDATE() , " +
                                        " RecordsOK = " + totOK + ", " +
                                        " RecordsNonDeliverable = " + toND + " where FileNameCASS = '" + fName + "'");


                    //rename or move files  counts
                }
            }

            return "";
        }
        public string updateTableSQL(DataTable inputdata, string to_DL, string ffName, string TableName)
        {
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from tmp_From_CASS");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                //bulkCopy.DestinationTableName =
                //    "[dbo].[Tempo_fsaData]";
                bulkCopy.DestinationTableName = "[dbo].[tmp_From_CASS]";

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(inputdata);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;
                    updErrors++;
                }
            }
            Connection.Close();
            try
            {
                SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@dataTable", TableName), new SqlParameter("@DLvalue", to_DL), new SqlParameter("@ffName", ffName) };

                DataSet ds2 = new DataSet();
                if (ffName.IndexOf("CR_") == 0 || ffName.IndexOf("MBA") == 0 || ffName.IndexOf("SMN") == 0 || ffName.IndexOf("PNO") == 0)
                    ds2 = dbU.ExecuteDataSet("Update_From_CassCR", sqlParams2);
                else
                    ds2 = dbU.ExecuteDataSet("Update_From_Cass", sqlParams2);
            }
            catch (Exception ex)
            {
                LogWriter logerror = new LogWriter();
                logerror.WriteLogToTable("Update From Cass", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Update", "TableName :  " + TableName + " filename " + ffName + " " + ex.Message, "email");
                errors = errors + ex.Message;
                updErrors++;
            }
            return errors;
        }

        public string updateTableSQL_HNJH(DataTable inputdata, string to_DL, string ffName, string TableName)
        {
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from tmp_From_CASS_HNJH");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                //bulkCopy.DestinationTableName =
                //    "[dbo].[Tempo_fsaData]";
                bulkCopy.DestinationTableName = "[dbo].[tmp_From_CASS_HNJH]";

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(inputdata);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;
                    updErrors++;
                }
            }
            Connection.Close();
            try
            {
                SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@dataTable", TableName), new SqlParameter("@DLvalue", to_DL), new SqlParameter("@ffName", ffName) };

                DataSet ds2 = new DataSet();
                if (TableName == "HOR_parse_HNJH_Champion" || TableName == "HOR_parse_HNJH_WK" || TableName == "HOR_parse_AbilTO"
                    || TableName == "HOR_parse_Involuntary_Disenrollment")    //UpdCity
                    ds2 = dbU.ExecuteDataSet("Update_From_CassHNJH_Champion", sqlParams2);
                //if (TableName == "HOR_parse_HNJH_WK")
                //    ds2 = dbU.ExecuteDataSet("Update_From_CassHNJH_WK", sqlParams2);
                else if (TableName == "HOR_DirectMail")
                    ds2 = dbU.ExecuteDataSet("Update_From_CassHNJH_DirectMail", sqlParams2);
                else
                    ds2 = dbU.ExecuteDataSet("Update_From_CassHNJH", sqlParams2);

            }
            catch (Exception ex)
            {
                LogWriter logerror = new LogWriter();
                logerror.WriteLogToTable("Update From Cass", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Update", "TableName :  " + TableName + " filename " + ffName + " " + ex.Message, "email");
                errors = errors + ex.Message;
                updErrors++;
            }
            return errors;
        }

        public void SendEmailErrors()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();

            processedData = dbU.ExecuteDataTable("HOR_ZZ_rpt_errors_parse_daily");
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>File(s) Processed:</p>");
            if (processedData.Rows.Count > 0)
            {
                strHTMLBuilder.Append(" Parse Daily Errors Results: <br>");
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

                //SendMails sendmail = new SendMails();
                //sendmail.SendMail("Parse Daily Errors Results", "rchico@apps.cierant.com",
                //    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                //                            "noreply@apps.cierant.com", "\n\n" +
                //                             strHTMLBuilder);  //tkrompinger@apps.cierant.com

            }

            DataTable processedDataErrPDFs = new DataTable();

            processedDataErrPDFs = dbU.ExecuteDataTable("select FileName, ImportDate, TotalP, page_addrs, Addr, Addr0, Addr1, Recnum, errors from HOR_parse_HLGS where errors <> ''");
            

            
            if (processedDataErrPDFs.Rows.Count > 0)
            {
                strHTMLBuilder.Append("<p>");
                strHTMLBuilder.Append(" Parse Daily Errors HOR_parse_HLGS: <br>");
                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in processedDataErrPDFs.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in processedDataErrPDFs.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");
                }

              

            }
            if (strHTMLBuilder.Length > 10)
            {

                SendMails sendmail = new SendMails();
                sendmail.SendMail("Parse Daily Errors Results", "rchico@apps.cierant.com",
                    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);  //tkrompinger@apps.cierant.com

            }
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
                    if (line.Replace("\"", "") == "RETURN_FIELD_01,RETURN_FIELD_02,RETURN_FIELD_03,RETURN_FIELD_04,RETURN_FIELD_05,RETURN_FIELD_06,RETURN_FIELD_07,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,RETURN_FIELD_13,RETURN_FIELD_14,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,State,ZIP+4,Delivery Point,Return Code")
                        valueOk = 1;

                    else if (line.Replace("\"", "") == "RETURN_FIELD_01,RETURN_FIELD_02,RETURN_FIELD_03,RETURN_FIELD_04,RETURN_FIELD_05,RETURN_FIELD_06,RETURN_FIELD_07,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,Latitude,Longitude,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,State,ZIP+4,Delivery Point,Return Code")
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
                        //if (xMatch == 22)
                        //    row["De"] = match.Value.Replace("\"", "").Replace(",", "");
                        //if (xMatch == 23)
                        //    row["Re"] = match.Value.Replace("\"", "").Replace(",", "");
                        xMatch++;
                    }
                    dataToUpdate.Rows.Add(row);
                }

            }

            file.Close();
            int numberOfRecords = dataToUpdate.Select("Uaddr1 = ''").Length;
            if (numberOfRecords > (dataToUpdate.Rows.Count * .40))
            {
                SendMails sendmail = new SendMails();
                for (int i = 0; i < 10; i++)
                {
                    sendmail.SendMailFatalError("Horizon File many nulls in addr records " + dataToUpdate.Rows.Count.ToString(), "Error inProcess", "\n\n"  , "");
                }
                var error = "Waiting to check";
            }
            return dataToUpdate;

        }
        public DataTable readcsvError(string fileName)
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
                        if (xMatch == 0)
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

            }

            file.Close();
            return dataToUpdate;

        }

        public DataTable readcsvHNJH(string fileName)
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
            //newt.Columns.Add("DeliveryPoint");
            //newt.Columns.Add("CassReturn");

            return newt;
        }
    }
}
