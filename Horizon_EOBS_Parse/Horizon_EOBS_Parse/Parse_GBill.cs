using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{
    public class Parse_GBill
    {
        DataTable G_BILLS = EOBs_Table();
        List<string> addrs = new List<string>();
        int Recnum = 1;
        int C_Recnum = 1;
        int countinAddr = 0;
        string inaddr = "";
        string sysout, jobname, pDate, aDate, seqNum, scount, exception, type, JobID, Feed;
        string errors = "";
        int errorcount = 0;
        DBUtility dbU;

        public string ProcessFiles(string dateProcess)
        {
            ProcessVars.serviceIsrunning = true;
            //autoEvent.WaitOne(1000 * 60 * 3, false);
            string result = zipFilesinDir(dateProcess);
            ProcessVars.serviceIsrunning = false;

            return result;
        }
        public string zipFilesinDir(string dateProcess)
        {

            if (Directory.Exists(ProcessVars.GBInputDirectory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(ProcessVars.GBInputDirectory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("*.txt");
                foreach (FileInfo file in FilesPDF)
                {
                    //if (file.Name.IndexOf("EP0GHBLP") == -1)
                    //{
                    try
                    {
                        string error = evaluate_TXT(file.FullName, "","");
                        if (error != "")
                            errors = errors + error + "\n\n";
                    }
                    catch (Exception ez)
                    {
                        errors = errors + file + "  " + ez.Message + "\n\n";
                    }
                    //}
                }


            }
            return errors;
        }

        public string Process_Gbills(string filename, string dateFile, string newSYSID)
        {
            string error = evaluate_TXT(filename,dateFile,newSYSID);
            return error;
        }
        public string evaluate_TXT(string fileName, string dateFile, string newSYSID)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            C_Recnum = 1;


            Recnum = 1;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Recnum = 1;
            else
                Recnum = Convert.ToInt32(recnum.ToString()) + 1;


            countinAddr = 1;
            inaddr = "";

            int prevline = 0;

            bool fsys = false;
            bool fsCount = false;
            bool fStart = false;
            

            //string sys = "SYSOUT ID: ";
            string sys = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string seqNN = " 3  COMM";
            string seqNN1 = " 3  HMO";
            string seqNN2 = " 3  CORP";
            
            string add1 = " 4 ";
            string add2 = "1";
            string add3 = " 6";
            sysout = jobname = pDate = aDate = seqNum = scount = exception = type = JobID = Feed = string.Empty;

            string processCompleted = "";

            //JobID = "JOB09155";



            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int currLine = 0;
            int valueOk = 0;
            string line;
            G_BILLS.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (newSYSID.Length > 1 && !fsys)
                    {
                        string[] words = newSYSID.Replace("  ", " ").Trim().Split(' ');    //Previous Balance

                        sysout = words[0];
                        jobname = words[1];
                        //jobID = words[2];
                        pDate = words[3];
                        fsys = true;

                    }
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {
                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');    //Previous Balance

                        sysout = words[1];
                        jobname = words[2];
                        JobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;
                        fsys = true;
                        fsCount = false;
                    }
                    //if (line.IndexOf(sys) != -1 && !fsys)
                    //{
                    //    sysout = searchText(sys, line, currLine);
                    //    jobname = searchText(jobn, line, currLine);
                    //    pDate = searchText(pdat, line, currLine);
                    //    aDate = searchText(adat, line, currLine);
                    //    fsys = true;
                    //    fsCount = false;
                    //}
                    if (line.IndexOf("DJDE ") != -1 && fsys)
                    {
                        prevline = currLine;
                        //fsCount = true;
                        //inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4).Replace(",","");
                    }

                    if ((line.IndexOf(seqNN) == 0 && fsys) || (line.IndexOf(seqNN1) == 0 && fsys)
                            || (line.IndexOf(seqNN2) == 0 && fsys))
                    {
                        countinAddr = 0;
                        fsCount = true;
                        prevline = currLine;
                        exception = "";
                        type = line.Substring(3, line.Length - 3).Trim();
                    }
                    if (fsCount && currLine == prevline + 2 && (line.IndexOf(" 3 ") == 0))
                    {
                        string result  = searchText2(" 3 ", line, currLine).Trim();
                        if (result != "00")
                            //exception = result;
                            exception = "E";
                    }

                    if (fsCount && currLine > prevline + 2 && (line.IndexOf(" 8 ") == 0))
                    {
                        seqNum = line.Substring(line.Length - 9);
                        //seqNum = searchText2(seqNN, line, currLine);
                    }
                 
                    if (line.IndexOf(add1) == 0 && fsCount && countinAddr < 5)
                    {
                        countinAddr++;
                        addrs.Add(line.Substring(add1.Length, line.Length - add1.Length));
                    }


                    if (line.IndexOf(add3) == 0 && fsCount)
                    {
                        if (countinAddr < 5)
                        {
                            while (countinAddr < 5)
                            {
                                addrs.Add("");
                                countinAddr++;
                            }
                        }

                        if(line.IndexOf("DUPLICATE") != -1)
                            exception = exception + "D";
                        addToTable(currLine);
                        fsCount = false;
                    }
               
                    currLine++;
                    if (currLine == 57)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n";
                    errorcount++;
                }
            }
            file.Close();
            createCAS_CSV create_cas__csv = new createCAS_CSV();
            if (errorcount == 0)
            {
                if (sysout != "")
                {
                    if (G_BILLS.Rows.Count > 0)
                    {

                        string resultcsv = create_cas__csv.create_GBills_CAS_CSV(
                                            fileName, G_BILLS, "GBill_PastDueNotice", Recnum, G_BILLS.Rows.Count.ToString(), sysout, JobID, dateFile);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "GBill_PastDueNotice", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "GBill_PastDueNotice", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "GBill_PastDueNotice", "error count " + errorcount);
            }

            return processCompleted;


            #region old code
            //if (sysout != "")
            //{
            //    file.Close();

            //    foreach (DataRow row in G_BILLS.Rows)
            //    {
            //        for (int ii = 19; ii > 0; ii--)
            //        {
            //            if (row[ii].ToString() != "")
            //            {
            //                row[19] = row[ii];
            //                row[ii] = "";
            //                break;
            //            }
            //        }
            //    }
            //    DataTable working_G_BILLS = G_BILLS.Copy();
            //    working_G_BILLS.Columns.Remove("MED_Flag");
                
            //    createCSV createcsv = new createCSV();
            //    string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";  // +DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //    string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";
            //    string pName = ProcessVars.GBOutputDirectory + pNameToCASS;

            //    if (File.Exists(pName))
            //        File.Delete(pName);
            //    var fieldnames = new List<string>();
            //    for (int index = 0; index < working_G_BILLS.Columns.Count; index++)
            //    {
            //        fieldnames.Add(working_G_BILLS.Columns[index].ColumnName);
            //        //string colname = working_G_BILLS.Columns[index].ColumnName;
            //        //colnames = colnames + ", [" + colname + "]";
            //    }
            //    bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            //    foreach (DataRow row in working_G_BILLS.Rows)
            //    {

            //        var rowData = new List<string>();
            //        for (int index = 0; index < working_G_BILLS.Columns.Count; index++)
            //        {
            //            rowData.Add(row[index].ToString());
            //        }
            //        resp = false;
            //        resp = createcsv.addRecordsCSV(pName, rowData);
            //        //if (UpdSQL != "")
            //        //    dbU.ExecuteScalar(UpdSQL + row[0]);
            //    }
            //    //copy to CASS
            //    string cassFileName = ProcessVars.gDMPs + pNameToCASS;
            //    File.Move(pName, cassFileName);

            //    // add to dbase
            //    string colnames = "";
            //    for (int index = 0; index < G_BILLS.Columns.Count; index++)
            //    {
            //        string colname = G_BILLS.Columns[index].ColumnName;
            //        colnames = colnames + ", [" + colname + "]";
            //    }

            //    GlobalVar.dbaseName = "BCBS_Horizon";
            //    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //    dbU.ExecuteScalar("delete from HOR_parse_TMP_GBill_PastDueNotice");

            //    int errors = 0;
            //    string recnumError = "";
            //    string insertCommand1 = "Insert into HOR_parse_TMP_GBill_PastDueNotice([FileName],[ImportDate]" + colnames + ") VALUES ('";
            //    foreach (DataRow row in G_BILLS.Rows)
            //    {
            //        string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
            //        for (int index = 0; index < G_BILLS.Columns.Count; index++)
            //        {
            //            insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''") + "','";
            //        }
            //        try
            //        {
            //            recnumError = row[0].ToString();
            //            var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
            //        }
            //        catch (Exception ex)
            //        {
            //            errors++;
            //        }
            //    }
            //    if (errors == 0)
            //    {
            //        int totrecs = G_BILLS.Rows.Count;
            //        dbU.ExecuteScalar("Insert into HOR_parse_GBill_PastDueNotice select * from HOR_parse_TMP_GBill_PastDueNotice");
            //        dbU.ExecuteScalar("delete from HOR_parse_TMP_GBill_PastDueNotice");
            //        // create store proc to delete if exist
            //        int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
            //        if (FileCount == 0)
            //        {
            //            //dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, SentDate, TableName,DirectoryTo) values(" +
            //            //                   totrecs + ",'" + pNameToCASS + "','" + fileInfo.Name + "', GETDATE(),'HOR_parse_GBill_PastDueNotice','" + directoryAfterCass + "')");

            //            dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed ) values(" +
            //              totrecs + ",'" + totrecs + "','" + fileInfo.Name + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "','" + dateFile + "','HOR_parse_GBill_PastDueNotice" + "','" + directoryAfterCass + "','" + sysout + "','" + jobname + "','Receive','Y')");

            //        }
            //        else
            //        {
            //            dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set RecordsNum =" +
            //                               totrecs + ", SentDate = GETDATE(), TableName = 'HOR_parse_CBill_PastDueNotice', Processed = NULL " +
            //                               ",DirectoryTo = '" + directoryAfterCass + "' where FileNameCASS = '" + pNameToCASS + "'");

            //        }
            //        dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_GBill_PastDueNotice', GETDATE())");
            //    }
            //    outputSCI(fileInfo.Name, "HOR_parse_GBill_PastDueNotice", directoryAfterCass);
            //}
            //else
            //{
            //    processCompleted = "No SYSOUT ID file " + fileName;
            //    errorcount++;
            //}
            //if (errorcount != 0)
            //    processCompleted = processCompleted + " errors " + errorcount;
            //return processCompleted;
            #endregion
        }
        public void outputSCI(string FFName, string TTName, string directoryAfterCass)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //pritn file to SCI

            SqlParameter[] sqlParams;
            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };
            DataTable datato_SCI = dbU.ExecuteDataTable("HOR_rpt_PARSE_to_SCI", sqlParams);
            if (datato_SCI.Rows.Count > 0)
            {
                createCSV createcsv = new createCSV();
                //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                string pName = directoryAfterCass + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
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

        }
        public void addToTable(int currline)
        {
            var row = G_BILLS.NewRow();
            row["Recnum"] = Recnum;
            row["Sysout"] = sysout;
            row["DE_Flag"] = exception; // scount;   
            row["Jobname"] = jobname;
            row["PrintDate"] = pDate;
            row["ArchiveDate"] = aDate;
            row["C_Recnum"] = C_Recnum;
            row["Seq"] = seqNum;
            row["Field2"] = type;
            row["Field4"] = Feed;
            row["Addr1"] = addrs[0];
            row["Addr2"] = addrs[1];
            row["Addr3"] = addrs[2];
            row["Addr4"] = addrs[3];
            row["JOBID"] = JobID;
            row["MED_Flag"] = "N";
            //row["Addr5"] = addrs[4];
            //row["Addr6"] = addrs[5];
            G_BILLS.Rows.Add(row);
            addrs.Clear();
            countinAddr = 0;
            Recnum++;
            C_Recnum++;
            inaddr = "";
        }
        public string searchText(string valuetosearch, string line, int currline)
        {
            try
            {
                //int poscBlank = line.IndexOf(" ", line.IndexOf(valuetosearch) + valuetosearch.Length);
                string seg1 = line.Substring(line.IndexOf(valuetosearch) + valuetosearch.Length);
                string seg2 = seg1.Substring(0, seg1.IndexOf(" "));
                return seg2;
            }
            catch (Exception ex)
            {
                errorcount++;
                return "error line " + currline;
            }
        }
        public string searchText2(string valuetosearch, string line, int currline)
        {
            try
            {
                //int poscBlank = line.IndexOf(" ", line.IndexOf(valuetosearch) + valuetosearch.Length);
                string seg1 = line.Substring(line.IndexOf(valuetosearch) + valuetosearch.Length);
                //string seg2 = seg1.Substring(0, seg1.IndexOf(" "));
                return seg1;
            }
            catch (Exception ex)
            {
                errorcount++;
                return "error line " + currline;
            }
        }

        public string searchTextSTART(string valuetosearch, string line, int currline)
        {
            string seg1 = "";
            try
            {

                int poscID = line.IndexOf(valuetosearch);
                //if(line.IndexOf(" ",valuetosearch.Length

                if (poscID > 30)
                {
                    int poscLastBlank = line.LastIndexOf("  ", line.Length);
                    seg1 = (line.Substring(poscLastBlank, line.Length - poscLastBlank)).Trim();
                    //int poscBlank1 = line.IndexOf(" ", 1) + 1;
                    //int poscBlank2 = line.IndexOf(" ", poscBlank1) + 1;
                    //string seg1 = line.Substring(poscBlank1, poscBlank2 - 1 - poscBlank1);

                }
                else
                {
                    int poscBlank = line.IndexOf(" ", 1);
                    int poscBlank2 = line.IndexOf(" ", poscBlank + valuetosearch.Length + 1);

                    if (poscBlank2 == -1)
                    {
                        seg1 = line.Substring(poscBlank + 1, line.Length - poscBlank - 1);
                    }
                    else if (poscBlank > poscID)
                    {
                        seg1 = line.Substring(3, poscBlank2 - 3).Trim();
                    }
                    else
                    {
                        seg1 = line.Substring(poscBlank + 1, poscBlank2 - poscBlank - 1);
                    }



                }
            }
            catch (Exception ex)
            {
                errorcount++;
                return "error line START " + currline;
            }
            if (seg1.Trim().Length == 0)
            {
                errorcount++;
                return "error line START " + currline;
            }
            return seg1;
        }
        private static DataTable EOBs_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("Sysout");
            newt.Columns.Add("Jobname");
            newt.Columns.Add("PrintDate");
            newt.Columns.Add("ArchiveDate");
            newt.Columns.Add("C_Recnum");
            newt.Columns.Add("Seq");
            newt.Columns.Add("DE_Flag");
            newt.Columns.Add("JOBID");
            newt.Columns.Add("Field2");
            newt.Columns.Add("Field3");
            newt.Columns.Add("Field4");
            newt.Columns.Add("Field5");
            newt.Columns.Add("Field6");
            newt.Columns.Add("Addr1");
            newt.Columns.Add("Addr2");
            newt.Columns.Add("Addr3");
            newt.Columns.Add("Addr4");
            newt.Columns.Add("Addr5");
            newt.Columns.Add("Addr6");
            newt.Columns.Add("MED_Flag");
            //newt.Columns.Add("On-Hand", typeof(Double));
            return newt;
        }
    }
}
