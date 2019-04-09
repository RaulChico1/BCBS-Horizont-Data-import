using System;
using System.Collections.Generic;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Text;

namespace Horizon_EOBS_Parse
{
    public class Notice_Letter
    {
        DataTable NoticeLetter = NoticeLetter_Table();
        DataTable NoticeLetter5303 = NoticeLetter5303_Table();
        DataTable NoticeLetter6003 = NoticeLetter6003_Table();
        List<string> details = new List<string>();
        int Recnum = 1;
        int countinAddr = 0;
        string sysout, jobname, pDate, aDate, seqNum, scount, LetterProduced;
        string errors = "";
        int errorcount = 0;
        int GRecnum = 1;
        DBUtility dbU;

        public string ProcessFiles(string dateProcess)
        {
            ProcessVars.serviceIsrunning = true;
            //autoEvent.WaitOne(1000 * 60 * 3, false);
            string result = zipFilesinDir(dateProcess);
            ProcessVars.serviceIsrunning = false;
            if (result == "")
                result = "Done " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
            return result;
        }
        public string zipFilesinDir(string dateProcess)
        {

            if (Directory.Exists(ProcessVars.NoticeDirectory))
            {
                 DirectoryInfo original_noTxt = new DirectoryInfo(ProcessVars.NoticeDirectory);
                 FileInfo[] FilesnoTXT = original_noTxt.GetFiles("*");

                 foreach (FileInfo file in FilesnoTXT)
                {
                    if (file.Name.IndexOf("EP") == -1)
                    {
                        try
                        {
                            //check jobid  it has, check output
                            string error = evaluate_TXT_no_Header(file.FullName);

                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                }
                DirectoryInfo originalTxts = new DirectoryInfo(ProcessVars.NoticeDirectory);
                FileInfo[] FilesTxt = originalTxts.GetFiles("*.txt");
                foreach (FileInfo file in FilesTxt)
                {
                   
                        if (file.Name.IndexOf("EP005303") != 0)
                        {
                            try
                            {
                                //check jobid definition
                                string error = evaluate_TXT(file.FullName, "");
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                        if (file.Name.IndexOf("EP005303") == 0)
                        {
                            try
                            {
                                //check jobid definition
                                string error = evaluate_TXT503(file.FullName, "");
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                //summary
                produceSummary();
                //HOR_ZZ_rpt_errors_parse_PastDueNotice_daily

            }
            return errors;
        }
        public void processNotices(string filename, string dateFile)
        {
            FileInfo file = new FileInfo(filename);

            if (file.Name.IndexOf("EP005203") == 0
                    || file.Name.ToUpper().IndexOf("EP005204") == 0)
            {
                try
                {
                    //check jobid definition
                    //string error = evaluate_TXT(file.FullName, file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
                    string error = evaluate_TXT(file.FullName, dateFile);
                    if (error != "")
                        errors = errors + error + "\n\n";
                }
                catch (Exception ez)
                {
                    errors = errors + file + "  " + ez.Message + "\n\n";
                }
            }
            if (file.Name.IndexOf("EP005303") == 0)
            {
                try
                {
                    //check jobid definition
                    string error = evaluate_TXT503(file.FullName, dateFile);
                    if (error != "")
                        errors = errors + error + "\n\n";
                }
                catch (Exception ez)
                {
                    errors = errors + file + "  " + ez.Message + "\n\n";
                }
            }
            if (file.Name.IndexOf("EP006003") == 0)
            {
                try
                {
                    //check jobid definition
                    string error = evaluate_TXT603(file.FullName, dateFile);
                    if (error != "")
                        errors = errors + error + "\n\n";
                }
                catch (Exception ez)
                {
                    errors = errors + file + "  " + ez.Message + "\n\n";
                }
            }
            //produceSummary();
        }


        public string evaluate_TXT_no_Header(string fileName)
        {
            //string str = ConfigurationManager.ConnectionStrings["conStrProd"].ToString();
            errorcount = 0;

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            Recnum = 1;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            countinAddr = 1;

            int linesDue = 0;
            int startRecLine = 0;
            bool fsys = false;
            bool fsCount1 = false;
            bool fsCount2 = false;
            bool fStart = false;
            bool fSeq = false;
            bool not_group_number = false;
            string sys = "SYSOUT ID: ";
            string sysCierant = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string seqNN = "+                                    PAST DUE NOTICE";
            string seqNN2 = ", 2015";
            string add1 = "COMM     Grp#";
            string add2 = "HMO      Grp#";
            string add3 = "CORP     Grp#";
            sysout = jobname = pDate = aDate = seqNum = scount =  string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int currLine = 0;
            int valueOk = 0;
            bool isrecord = false;
            bool isduedates = false;
            bool finisfduedates = false;
            string line;
            bool lookingStatementdate = false;
            bool lookingTotal = false;
            bool lookingTotalFound = false;
            bool ismemebership = false;

            bool lookingAddr = false;


            NoticeLetter.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (!fsys)
                    {


                        string[] words = fileInfo.Name.Split('.');

                            sysout = words[2];
                            jobname = words[1];
                            pDate = words[0];
                            //sysout = searchText(sys, line, currLine);
                            //jobname = searchText(jobn, line, currLine);
                            //pDate = searchText(pdat, line, currLine);
                            aDate = "";
                            fsys = true;
                            fsCount1 = false;
                            details.Clear();
                       
                    }


                    if ((line.IndexOf(seqNN) != -1 && fsys))
                    {
                        //seqNum = searchText2(seqNN, line, currLine);   //  is past due notice line
                        startRecLine = currLine;
                        fsCount1 = true;
                        fSeq = true;
                        isrecord = false;
                        lookingAddr = false;
                        ismemebership = false;
                    }
                    if ((line.IndexOf(seqNN2) == 49 && fsys))
                    {
                        startRecLine = currLine;
                        details.Add(line.Trim());
                        isrecord = true;
                        startRecLine = currLine;
                        fsCount2 = false;
                        not_group_number = true;
                    }
                    if (fsCount1 && currLine == startRecLine + 2)
                    {
                        if (line.IndexOf("XXXXXXXX 99, 9999") == -1)
                        {
                            details.Add(line.Trim());
                            isrecord = true;
                            startRecLine = currLine;
                            fsCount1 = false;
                        }
                        else
                        {
                            isrecord = false;
                            fsCount1 = false;
                        }
                    }


                    if (isrecord && currLine == startRecLine + 2)
                    {
                        if (line.IndexOf("DEAR") != -1)
                        {
                            details.Add(line.Trim());
                            isrecord = true;
                            startRecLine = currLine;
                        }

                    }

                    if (isrecord && currLine > startRecLine && line.IndexOf("PAST DUE AMOUNT") != -1)
                    {
                        isduedates = true;
                        linesDue = 0;
                        startRecLine = currLine;
                        isrecord = false;
                    }

                    if (isduedates)
                    {
                        if (currLine != startRecLine)
                        {
                            if (line.Length > 1)
                            {
                                if (line.IndexOf("TOTAL AMOUNT PAST DUE") != -1)
                                {
                                    isduedates = false;
                                    finisfduedates = true;
                                    startRecLine = currLine;
                                    if (linesDue < 30)
                                    {
                                        do
                                        {
                                            details.Add("");
                                            linesDue++;
                                        } while (linesDue < 30);
                                    }
                                    string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                                    details.Add(total.Trim());

                                }
                                else
                                {
                                    if (line.IndexOf("_______________") == -1 && line.IndexOf("--------------") == -1)
                                    {
                                        string nline = line.Replace("Previous Balance", "PreviousBalance");
                                        string[] words = nline.Trim().Split(' ');    //Previous Balance
                                        int numberElements = 0;
                                        foreach (string word in words)
                                        {
                                            if (word.Length > 0)
                                            {

                                                numberElements++;
                                                if (word == "PreviousBalance")
                                                {
                                                    details.Add("Previous Balance");
                                                    linesDue++;
                                                }

                                                else
                                                {
                                                    details.Add(word.Trim());
                                                    linesDue++;
                                                }
                                                if (not_group_number && numberElements == 1)
                                                {
                                                    details.Add("");
                                                    linesDue++;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                    if (finisfduedates && currLine > startRecLine + 1 && line.IndexOf("TOTAL AMOUNT PAST DUE") != -1)
                    {
                        string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                        details.Add(total.Trim());
                    }
                    if (finisfduedates && currLine > startRecLine && line.IndexOf("was due on") != -1)
                    {
                        int poscBlank = line.IndexOf("was due on", 1) + 11;
                        int poscBlank2 = line.IndexOf(".", poscBlank + 11);


                        string date = line.Substring(poscBlank, poscBlank2 - poscBlank);
                        details.Add(date.Trim().Replace("  ", " "));
                        lookingTotal = true;
                    }

                    if (lookingTotal && line.IndexOf("Charges             Amount            Enclosed") != -1)
                    {
                        lookingTotalFound = true;
                        lookingTotal = false;
                        startRecLine = currLine;
                    }
                    if (lookingTotal && line.IndexOf("CONSUMER MEMBERSHIP") > 10)
                    {
                        lookingTotalFound = true;
                        lookingTotal = false;
                        startRecLine = currLine;
                        ismemebership = true;
                        details.Add("");
                        details.Add("");
                        details.Add("");
                        details.Add("");

                    }


                    if (lookingTotalFound && currLine > startRecLine && line.Length > 1)
                    {
                        string nline = line.Replace(" -", "-");
                        string[] words = nline.Trim().Split(' ');

                        foreach (string word in words)
                        {
                            if (word.Length > 0)
                            {
                                details.Add(word.Trim());
                            }
                        }
                        lookingTotalFound = false;
                        lookingTotal = false;
                        lookingStatementdate = true;
                        startRecLine = currLine;
                    }

                    if (!ismemebership)
                    {
                        if (lookingStatementdate && line.IndexOf("Statement Date:") != -1)
                        {
                            string result = searchText2("Statement Date:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf("Account Number:") != -1)
                        {
                            string result = searchText2("Account Number:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf("Invoice Number:") != -1)
                        {
                            string result = searchText2("Invoice Number:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf(add1) != -1)
                        {
                            string result = searchText2A(add1, line, currLine);
                            details.Add(add1.Replace("Grp#", "").Trim());
                            string result2 = result.Substring(add1.Length + 1);


                            details.Add(result2);

                            lookingAddr = true;
                            lookingStatementdate = false;
                            startRecLine = currLine;
                        }
                        if (lookingStatementdate && (line.IndexOf(add2) != -1))
                        {
                            string result = searchText2A(add2, line, currLine);
                            details.Add(add2.Replace("Grp#", "").Trim());
                            string result2 = result.Substring(add2.Length + 1);


                            details.Add(result2);

                            lookingAddr = true;
                            lookingStatementdate = false;
                            startRecLine = currLine;
                        }
                        if (lookingStatementdate && (line.IndexOf(add3) != -1))
                        {
                            string result = searchText2A(add3, line, currLine);
                            details.Add(add3.Replace("Grp#", "").Trim());
                            string result2 = result.Substring(add3.Length + 1);


                            details.Add(result2);

                            lookingAddr = true;
                            lookingStatementdate = false;
                            startRecLine = currLine;
                        }
                    }
                    else
                    {
                        if (lookingStatementdate && currLine > startRecLine && line.Length > 1)
                        {

                            details.Add(line.Trim());
                            details.Add("");
                            lookingAddr = true;
                            startRecLine = currLine;
                            lookingStatementdate = false;
                        }

                    }
                    if (lookingAddr && currLine > startRecLine)
                    {
                        string addLine = "";
                        if (line.Length > 1)
                        {

                            if (line.Length < 53)
                                addLine = line.Replace("  ", " ").Replace("-", "").Trim();
                            else
                                addLine = line.Substring(0, 52).Replace("  ", " ").Replace("-", "").Trim();

                            if (addLine.Length > 1)
                            {

                                double n;
                                bool isNumeric = double.TryParse(addLine.Replace(" ", ""), out n);
                                if (isNumeric && addLine.Trim().Length > 10)
                                {
                                    details.Add(line.Trim());
                                    lookingAddr = false;
                                    if (details.Count < 48)
                                        details.Add("");
                                    addToTable(currLine, "5204");
                                }
                                else
                                {
                                    details.Add(addLine.Trim());
                                }

                            }
                        }
                    }
                    currLine++;
                    //if (currLine == 56035)
                    //    valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n";
                    errorcount++;
                }
            }
            //wr.Flush();
            //wr.Close();
            if (sysout != "")
            {
                file.Close();
                if (NoticeLetter.Rows.Count > 0)
                {
                    foreach (DataRow row in NoticeLetter.Rows)
                    {
                        for (int ii = 57; ii > 0; ii--)
                        {
                            if (row[ii].ToString() != "")
                            {
                                row[57] = row[ii];
                                row[ii] = "";
                                break;
                            }
                        }
                    }
                    DataTable output_NoticeLetter;
                    output_NoticeLetter = NoticeLetter.Copy();


                    string colnames = "";

                    createCSV createcsv = new createCSV();
                    string pName = ProcessVars.oNoticeDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                    //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                    if (File.Exists(pName))
                        File.Delete(pName);
                    var fieldnames = new List<string>();
                    for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                    {
                        string nColname = "";
                        string colname = output_NoticeLetter.Columns[index].ColumnName;
                        colnames = colnames + ", [" + colname + "]";
                        if (ismemebership)
                        {
                            if (colname == "StatmDate")
                                nColname = "StatmCode";

                            else if (colname == "AcctN")
                                nColname = "StatmDate";

                            else if (colname == "InvoiceN")
                                nColname = "StatmTotal";
                            else
                                nColname = colname;

                            fieldnames.Add(nColname);

                        }
                        else
                        {
                            //fieldnames.Add(output_NoticeLetter.Columns[index].ColumnName);
                            fieldnames.Add(colname);
                        }
                    }
                    bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                    resp = createcsv.addRecordsCSV(pName, fieldnames);
                    foreach (DataRow row in output_NoticeLetter.Rows)
                    {

                        var rowData = new List<string>();
                        for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());
                        }
                        resp = false;
                        resp = createcsv.addRecordsCSV(pName, rowData);
                        //if (UpdSQL != "")
                        //    dbU.ExecuteScalar(UpdSQL + row[0]);
                    }



                    GlobalVar.dbaseName = "BCBS_Horizon";
                    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                    dbU.ExecuteScalar("delete from HOR_parse_TMP_NL_PastDueNotice");

                    int errors = 0;
                    string recnumError = "";
                    string insertCommand1 = "Insert into HOR_parse_TMP_NL_PastDueNotice([FileName],[ImportDate]" + colnames + ") VALUES ('";
                    foreach (DataRow row in output_NoticeLetter.Rows)
                    {
                        string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                        for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                        {
                            insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''") + "','";
                        }
                        try
                        {
                            recnumError = row[0].ToString();
                            var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                        }
                        catch (Exception ex)
                        {
                            errors++;
                        }
                    }
                    if (errors == 0)
                    {
                        dbU.ExecuteScalar("Insert into HOR_parse_NL_PastDueNotice select * from HOR_parse_TMP_NL_PastDueNotice");
                        dbU.ExecuteScalar("delete from HOR_parse_TMP_NL_PastDueNotice");
                        dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_NL_PastDueNotice', GETDATE())");
                    }
                    //check jobid
                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,Processed ) values(" +
                                           output_NoticeLetter.Rows.Count + ",'" + fileInfo.Name + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "', GETDATE(),'HOR_parse_NL_PastDueNotice" + "','No CASS',' ','" + sysout + "','" + jobname + "','Receive','" + GlobalVar.DateofProcess + "')");
                    
                }
                else
                {
                    processCompleted = "No records file " + fileName;
                    errorcount++;
                }
            }
            else
            {
                processCompleted = "No SYSOUT ID file " + fileName;
                errorcount++;
            }
            if (errorcount != 0)
                processCompleted = processCompleted + " errors " + errorcount;
            return processCompleted;
        }
        public string evaluate_TXT(string fileName, string dateFile)
        {
            //string str = ConfigurationManager.ConnectionStrings["conStrProd"].ToString();


            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            Recnum = 1;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
                int recordnumber = 0;
                if (recnum.ToString() == "")
                    GRecnum = 1;
                else
                    GRecnum = Convert.ToInt32(recnum.ToString()) + 1;
                
            countinAddr = 1;

            int linesDue = 0;
            int startRecLine = 0;
            bool fsys = false;
            bool fsCount1 = false;
            bool fsCount2 = false;
            bool fStart = false;
            bool fSeq = false;
            bool not_group_number = false;
            
            string sys = "SYSOUT ID: ";
            string sysCierant = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string seqNN = "+                                    PAST DUE NOTICE";
            string seqNN2 = ", 2015";
            string add1 = "COMM     Grp#";
            string add2 = "HMO      Grp#";
            string add3 = "CORP     Grp#";

            string final = "TOTAL PAGES PRINTED    -";
            sysout = jobname = pDate = aDate = seqNum = scount = LetterProduced = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int currLine = 0;
            int valueOk = 0;
            bool isrecord = false;
            bool isduedates = false;
            bool finisfduedates = false;
            string line;
            bool lookingStatementdate = false;
            bool lookingTotal = false;
            bool lookingTotalFound = false;
            bool ismemebership = false;
            
            bool lookingAddr = false;

            
            NoticeLetter.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if ((line.IndexOf(sys) != -1  || line.IndexOf(sysCierant) == 0) && !fsys)
                    {
                        if (line.IndexOf(sysCierant) == 0)
                        {
                            while (line.Contains("  ")) line = line.Replace("  ", " ");
                            string[] words = line.Replace("  ", " ").Trim().Split(' ');    

                            sysout = words[1];
                            jobname = words[3];
                            pDate = words[6];

                            //sysout = searchText(sys, line, currLine);
                            //jobname = searchText(jobn, line, currLine);
                            //pDate = searchText(pdat, line, currLine);
                            aDate = "";
                            fsys = true;
                            fsCount1 = false;
                            details.Clear();
                        }
                        else
                        {
                            sysout = searchText(sys, line, currLine);
                            jobname = searchText(jobn, line, currLine);
                            pDate = searchText(pdat, line, currLine);
                            aDate = searchText(adat, line, currLine);
                            fsys = true;
                            fsCount1 = false;
                            fsCount2 = false;
                        }
                    }


                    if ((line.IndexOf(seqNN) != -1 && fsys))
                    {
                        //seqNum = searchText2(seqNN, line, currLine);   //  is past due notice line
                        startRecLine = currLine;
                        fsCount1 = true;
                        fSeq = true;
                        isrecord = false;
                        lookingAddr = false;
                        ismemebership = false;
                    }
                    if ((line.IndexOf(seqNN2) == 49 && fsys))
                    {
                        startRecLine = currLine;
                        details.Add(line.Trim());
                        isrecord = true;
                        startRecLine = currLine;
                        fsCount2 = false;
                        not_group_number = true;
                    }
                    if (fsCount1 && currLine == startRecLine + 2)
                    {
                        if (line.IndexOf("XXXXXXXX 99, 9999") == -1)
                        {
                            details.Add(line.Trim());
                            isrecord = true;
                            startRecLine = currLine;
                            fsCount1 = false;
                        }
                        else
                        {
                            isrecord = false;
                            fsCount1 = false;
                        }
                    }
                  

                    if (isrecord && currLine == startRecLine + 2)
                    {
                        if (line.IndexOf("DEAR") != -1)
                        {
                            details.Add(line.Trim());
                            isrecord = true;
                            startRecLine = currLine;
                        }
                
                    }

                    if (isrecord && currLine > startRecLine && line.IndexOf("PAST DUE AMOUNT") != -1)
                    {
                        isduedates = true;
                        linesDue = 0;
                        startRecLine = currLine;
                        isrecord = false;
                    }

                    if (isduedates)
                    {
                        if (currLine != startRecLine)
                        {
                            if (line.Length > 1)
                            {
                                if (line.IndexOf("TOTAL AMOUNT PAST DUE") != -1)
                                {
                                    isduedates = false;
                                    finisfduedates = true;
                                    startRecLine = currLine;
                                    if (linesDue < 30)
                                    {
                                        do
                                        {
                                            details.Add("");
                                            linesDue++;
                                        } while (linesDue < 30);
                                    }
                                    string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                                    details.Add(total.Trim());

                                }
                                else
                                {
                                    if (line.IndexOf("_______________") == -1 && line.IndexOf("--------------") == -1)
                                    {
                                        string nline = line.Replace("Previous Balance", "PreviousBalance");
                                        string[] words = nline.Trim().Split(' ');    //Previous Balance
                                        int numberElements = 0;
                                        foreach (string word in words)
                                        {
                                            if (word.Length > 0)
                                            {

                                                numberElements++;
                                                if (word == "PreviousBalance")
                                                {
                                                    details.Add("Previous Balance");
                                                    linesDue++;
                                                }

                                                else
                                                {
                                                    details.Add(word.Trim());
                                                    linesDue++;
                                                }
                                                if (not_group_number && numberElements == 1)
                                                {
                                                    details.Add("");
                                                    linesDue++;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                    if (finisfduedates && currLine > startRecLine + 1 && line.IndexOf("TOTAL AMOUNT PAST DUE") != -1)
                    {
                        string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                        details.Add(total.Trim());
                    }
                    if (finisfduedates && currLine > startRecLine && line.IndexOf("was due on") != -1)
                    {
                        int poscBlank = line.IndexOf("was due on", 1) + 11;
                        int poscBlank2 = line.IndexOf(".", poscBlank + 11);


                        string date = line.Substring(poscBlank, poscBlank2 - poscBlank);
                        details.Add(date.Trim().Replace("  ", " "));
                        lookingTotal = true;
                    }

                    if (lookingTotal && line.IndexOf("Charges             Amount            Enclosed") != -1)
                    {
                        lookingTotalFound = true;
                        lookingTotal = false;
                        startRecLine = currLine;
                    }
                    if (lookingTotal && line.IndexOf("CONSUMER MEMBERSHIP") > 10)
                    {
                        lookingTotalFound = true;
                        lookingTotal = false;
                        startRecLine = currLine;
                        ismemebership = true;
                        details.Add("");
                        details.Add("");
                        details.Add("");
                        details.Add("");

                    }


                    if (lookingTotalFound && currLine > startRecLine && line.Length > 1)
                    {
                        string nline = line.Replace(" -", "-");
                        string[] words = nline.Trim().Split(' ');

                        foreach (string word in words)
                        {
                            if (word.Length > 0)
                            {
                                details.Add(word.Trim());
                            }
                        }
                        lookingTotalFound = false;
                        lookingTotal = false;
                        lookingStatementdate = true;
                        startRecLine = currLine;
                    }

                    if (!ismemebership)
                    {
                        if (lookingStatementdate && line.IndexOf("Statement Date:") != -1)
                        {
                            string result = searchText2("Statement Date:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf("Account Number:") != -1)
                        {
                            string result = searchText2("Account Number:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf("Invoice Number:") != -1)
                        {
                            string result = searchText2("Invoice Number:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf(add1) != -1)
                        {
                            string result = searchText2A(add1, line, currLine);
                            details.Add(add1.Replace("Grp#", "").Trim());
                            string result2 = result.Substring(add1.Length + 1);


                            details.Add(result2);

                            lookingAddr = true;
                            lookingStatementdate = false;
                            startRecLine = currLine;
                        }
                        if (lookingStatementdate && (line.IndexOf(add2) != -1))
                        {
                            string result = searchText2A(add2, line, currLine);
                            details.Add(add2.Replace("Grp#", "").Trim());
                            string result2 = result.Substring(add2.Length + 1);


                            details.Add(result2);

                            lookingAddr = true;
                            lookingStatementdate = false;
                            startRecLine = currLine;
                        }
                        if (lookingStatementdate && (line.IndexOf(add3) != -1))
                        {
                            string result = searchText2A(add3, line, currLine);
                            details.Add(add3.Replace("Grp#", "").Trim());
                            string result2 = result.Substring(add3.Length + 1);


                            details.Add(result2);

                            lookingAddr = true;
                            lookingStatementdate = false;
                            startRecLine = currLine;
                        }
                    }
                    else
                    {
                        if (lookingStatementdate && currLine > startRecLine && line.Length > 1)
                        {
                            
                            details.Add(line.Trim());
                            details.Add("");
                            lookingAddr = true;
                            startRecLine = currLine;
                            lookingStatementdate = false;
                        }

                    }
                    if (lookingAddr && currLine > startRecLine)
                    {
                        string addLine = "";
                        if (line.Length > 1)
                        {
                            
                            if (line.Length < 53)
                                addLine = line.Replace("  ", " ").Replace("-", "").Trim();
                            else
                                addLine = line.Substring(0, 52).Replace("  ", " ").Replace("-", "").Trim();

                            if (addLine.Length > 1)
                            {
                                
                                    double n;
                                    bool isNumeric = double.TryParse(addLine.Replace(" ", ""), out n);
                                    if (isNumeric && addLine.Trim().Length > 10)
                                    {
                                        details.Add(line.Trim());
                                        lookingAddr = false;
                                        if (details.Count < 48)
                                            details.Add("");
                                        addToTable(currLine, "5204");
                                    }
                                    else
                                    {
                                        details.Add(addLine.Trim());
                                    }
                                
                            }
                        }
                    }
                    if (line.IndexOf(final) > 0 && fsys)
                    {
                        int poscHad = line.IndexOf("-");
                        LetterProduced = line.Substring(poscHad + 1, line.Length - poscHad - 1).Replace("*","").Trim();
                    }
                    currLine++;
                    if (currLine == 265)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n";
                    errorcount++;
                }
            }
            //wr.Flush();
            //wr.Close();
            if (sysout != "")
            {
                file.Close();
                foreach (DataRow row in NoticeLetter.Rows)
                {
                    for (int ii = 57; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[57] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }
                DataTable output_NoticeLetter;
                output_NoticeLetter = NoticeLetter.Copy();


                string colnames = "";

                createCSV createcsv = new createCSV();
                string pName = ProcessVars.oNoticeDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                {
                    string nColname = "";
                    string colname = output_NoticeLetter.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                    if (ismemebership)
                    {
                        if (colname == "StatmDate")
                            nColname = "StatmCode";

                        else if (colname == "AcctN")
                            nColname = "StatmDate";

                        else if (colname == "InvoiceN")
                            nColname = "StatmTotal";
                        else
                            nColname = colname;

                        fieldnames.Add(nColname);

                    }
                    else
                    {
                    //fieldnames.Add(output_NoticeLetter.Columns[index].ColumnName);
                        fieldnames.Add(colname);
                    }
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in output_NoticeLetter.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }

               
                //output_NoticeLetter.Columns.Add("ImportDate", typeof(DateTime));
                //try
                //{
                //    for (int i = 0; i < output_NoticeLetter.Rows.Count; i++)
                //    {
                //        output_NoticeLetter.Rows[i]["ImportDate"] = DateTime.Now.ToString("yyyy-MM-dd");
                //    }
                //}
                //catch (Exception ex)
                //{
                //    string error = ex.Message + "\n\n";
                //}
                //add to sql
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_TMP_NL_PastDueNotice");

                int errors = 0;
                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_TMP_NL_PastDueNotice([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in output_NoticeLetter.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                    {
                        insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''") + "','";
                    }
                    try
                    {
                        recnumError = row[0].ToString();
                      var resultSql =  dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                    }
                    catch (Exception ex)
                    {
                        errors++;
                    }
                }
                if (errors == 0)
                {
                    dbU.ExecuteScalar("Insert into HOR_parse_NL_PastDueNotice select * from HOR_parse_TMP_NL_PastDueNotice");
                    dbU.ExecuteScalar("delete from HOR_parse_TMP_NL_PastDueNotice");
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_NL_PastDueNotice', GETDATE())");

                }

                dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess ) values(" +
                                           output_NoticeLetter.Rows.Count + ",'" + output_NoticeLetter.Rows.Count + "','" + fileInfo.Name + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "','" + dateFile + "','HOR_parse_NL_PastDueNotice" + "','No CASS','" + sysout + "','" + jobname + "','Receive','Y','" + GlobalVar.DateofProcess + "')");

            }
            else
            {
                processCompleted = "No SYSOUT ID file " + fileName;
                errorcount++;
            }
            if (errorcount != 0)
                processCompleted = processCompleted + " errors " + errorcount;
            return processCompleted;
        }

        public string evaluate_TXT503(string fileName, string dateFile)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            Recnum = 1;
           

            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            Recnum = 1;
            countinAddr = 1;

            int linesDue = 0;
            int startRecLine = 0;
            bool fsys = false;
            bool fsCount1 = false;
            string sys = "SYSOUT ID: ";
            string sysCierant = "  CIERANT";

            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";

            string final = "TOTAL PAGES PRINTED    -";

            sysout = jobname = pDate = aDate = seqNum = scount = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int currLine = 0;
            int valueOk = 0;
            bool isrecord = false;
            bool isduedates = false;
            bool finisfduedates = false;
            string line;
            bool lookingStatementdate = false;
            bool lookingTotal = false;
            bool lookingTotalFound = false;
            bool ismemebership = false;
            bool lookingSeq = false;
            bool lookingAddr = false;
            bool lookingDate = false;


            NoticeLetter5303.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if ((line.IndexOf(sys) != -1 || line.IndexOf(sysCierant) == 0) && !fsys)
                    {
                        if (line.IndexOf(sysCierant) == 0)
                        {
                            while (line.Contains("  ")) line = line.Replace("  ", " ");
                            string[] words = line.Replace("  ", " ").Trim().Split(' ');   

                            sysout = words[1];
                            jobname = words[3];
                            pDate = words[6];
                            
                            //sysout = searchText(sys, line, currLine);
                            //jobname = searchText(jobn, line, currLine);
                            //pDate = searchText(pdat, line, currLine);
                            aDate = "";
                            fsys = true;
                            fsCount1 = false;
                            details.Clear();
                        }
                        else
                        {
                            sysout = searchText(sys, line, currLine);
                            jobname = searchText(jobn, line, currLine);
                            pDate = searchText(pdat, line, currLine);
                            aDate = searchText(adat, line, currLine);
                            fsys = true;
                            fsCount1 = false;
                            details.Clear();
                        }
                    }


                    if ((line.IndexOf("1") == 0 && fsys))
                    {
                        startRecLine = currLine;
                        fsCount1 = true;
                        isrecord = false;
                        lookingAddr = false;
                        ismemebership = false;
                    }
                    if (fsCount1 && currLine == startRecLine + 3 && line.IndexOf("9999999999") == -1 && line.Length > 1)
                    {
                        string nline = line.Replace("  ", " ").Replace(" -", "-");
                        string[] words = nline.Trim().Split(' ');

                        foreach (string word in words)
                        {
                            if (word.Length > 0)
                            {
                                details.Add(word.Replace("-", " -").Trim());
                            }
                        }
                        fsCount1 = false;
                        lookingSeq = true;
                        startRecLine = currLine;
                    }
                    if (lookingSeq && currLine > startRecLine)
                    {
                        if (line.Length > 1)
                        {
                            string numLine = line.Trim();
                            double n;
                            bool isNumeric = double.TryParse(numLine, out n);
                            if (isNumeric)
                            {
                                details.Add(numLine);
                                lookingSeq = false;
                                startRecLine = currLine;
                                lookingAddr = true;
                            }
                        }
                    }
                    if (lookingAddr && currLine > startRecLine)
                    {
                        string addLine = "";
                        if (line.Length > 1)
                        {

                            if (line.Length < 53)
                                addLine = line.Replace("  ", " ").Replace("-", "").Trim();
                            else
                                addLine = line.Substring(0, 52).Replace("  ", " ").Replace("-", "").Trim();

                            if (addLine.Length > 1)
                            {

                                double n;
                                bool isNumeric = double.TryParse(addLine.Replace(" ", ""), out n);
                                if (isNumeric && addLine.Trim().Length > 10)
                                {
                                    while (details.Count < 10)
                                    {
                                        details.Add("");
                                    }

                                    details.Add(line.Trim());
                                    lookingAddr = false;
                                    lookingDate = true;
                                    startRecLine = currLine;
                                }
                                else
                                {
                                    details.Add(addLine.Trim());
                                }

                            }
                        }
                    }
                    if (lookingDate && currLine > startRecLine)
                    {
                        if (line.Length > 1)
                        {
                            details.Add(line.Replace("  ", " ").Trim());
                            isrecord = true;
                            startRecLine = currLine;
                            lookingDate = false;
                        }
                    }
                    if (isrecord && currLine > startRecLine)
                    {
                        if (line.IndexOf("DEAR") != -1)
                        {
                            details.Add(line.Trim());
                            isrecord = true;
                            startRecLine = currLine;
                        }
                        //else
                        //{
                        //    details.Add("");
                        //}
                    }

                    if (isrecord && currLine > startRecLine && line.IndexOf("PAST DUE AMOUNT") != -1)
                    {
                        isduedates = true;
                        linesDue = 0;
                        startRecLine = currLine;
                        isrecord = false;
                    }

                    if (isduedates)
                    {
                        if (currLine != startRecLine)
                        {
                            if (line.Length > 1)
                            {
                                if (line.IndexOf("TOTAL AMOUNT PAST DUE") != -1)
                                {
                                    isduedates = false;
                                    finisfduedates = true;
                                    startRecLine = currLine;
                                    if (linesDue < 20)
                                    {
                                        do
                                        {
                                            details.Add("");
                                            linesDue++;
                                        } while (linesDue < 20);
                                    }
                                    string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                                    details.Add(total.Trim());

                                }
                                else
                                {
                                    if (line.IndexOf("_______________") == -1 && line.IndexOf("--------------") == -1)
                                    {
                                        string nline = line.Replace("Previous Balance", "PreviousBalance");
                                        string[] words = nline.Trim().Split(' ');    //Previous Balance

                                        foreach (string word in words)
                                        {
                                            if (word.Length > 0)
                                            {
                                                if (word == "PreviousBalance")
                                                {
                                                    details.Add("Previous Balance");
                                                    linesDue++;
                                                }

                                                else
                                                {
                                                    details.Add(word.Trim());
                                                    linesDue++;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                    if (finisfduedates && currLine > startRecLine + 1 && line.IndexOf("TOTAL AMOUNT PAST DUE") != -1)
                    {
                        string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                        details.Add(total.Trim());
                    }
                    if (finisfduedates && currLine > startRecLine && line.IndexOf("was due on") != -1)
                    {
                        int poscBlank = line.IndexOf("was due on", 1) + 11;
                        int poscBlank2 = line.IndexOf(".", poscBlank + 11);


                        string date = line.Substring(poscBlank, poscBlank2 - poscBlank);
                        details.Add(date.Trim().Replace("  ", " "));
                        lookingTotal = true;
                    }

                    if (lookingTotal && line.IndexOf("Charges             Amount            Enclosed") != -1)
                    {
                        lookingTotalFound = true;
                        lookingTotal = false;
                        startRecLine = currLine;
                    }
                    if (lookingTotal && line.IndexOf("CONSUMER MEMBERSHIP") > 10)
                    {
                       
                        lookingTotal = false;
                        addToTable(currLine,"5303");
                    }
                    if (line.IndexOf(final) > 0 && fsys)
                    {
                        int poscHad = line.IndexOf("-");
                        LetterProduced = line.Substring(poscHad + 1, line.Length - poscHad - 1).Replace("*", "").Trim();
                    }
                    currLine++;
                    if (currLine == 107)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n";
                    errorcount++;
                }
            }
            //wr.Flush();
            //wr.Close();
            if (sysout != "")
            {
                file.Close();


                DataTable output_NoticeLetter5303;
                output_NoticeLetter5303 = NoticeLetter5303.Copy();

                createCSV createcsv = new createCSV();
                string pName = ProcessVars.oNoticeDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < output_NoticeLetter5303.Columns.Count; index++)
                {
                    string colname = output_NoticeLetter5303.Columns[index].ColumnName;

                    fieldnames.Add(colname);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);     // double title for XMPie only
                resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in output_NoticeLetter5303.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < output_NoticeLetter5303.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_TMP_NL_PastDueNotice");

                string colnames = "";
                for (int index = 0; index < output_NoticeLetter5303.Columns.Count; index++)
                {
                    string colname = output_NoticeLetter5303.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                int errors = 0;
                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_TMP_NL_PastDueNotice([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in output_NoticeLetter5303.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < output_NoticeLetter5303.Columns.Count; index++)
                    {
                        insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''") + "','";
                    }
                    try
                    {
                        recnumError = row[0].ToString();
                        var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                    }
                    catch (Exception ex)
                    {
                        errors++;
                    }
                }
                if (errors == 0)
                {
                    dbU.ExecuteScalar("Insert into HOR_parse_NL_PastDueNotice select * from HOR_parse_TMP_NL_PastDueNotice");
                    dbU.ExecuteScalar("delete from HOR_parse_TMP_NL_PastDueNotice");
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_NL_PastDueNotice', GETDATE())");

                }

                dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum,LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess ) values(" +
                                           output_NoticeLetter5303.Rows.Count + ",'" + output_NoticeLetter5303.Rows.Count.ToString() + "','" + fileInfo.Name + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "','" + dateFile + "','HOR_parse_NL_PastDueNotice" + "','No CASS','" + sysout + "','" + jobname + "','Receive','Y','" + GlobalVar.DateofProcess + "')");

            }
            else
            {
                processCompleted = "No SYSOUT ID file " + fileName;
                errorcount++;
            }
            if (errorcount != 0)
                processCompleted = processCompleted + " errors " + errorcount;
            return processCompleted;
        }

        public string evaluate_TXT603(string fileName, string dateFile)
        {
            //string str = ConfigurationManager.ConnectionStrings["conStrProd"].ToString();


            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            Recnum = 1;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            countinAddr = 1;

            int linesDue = 0;
            int startRecLine = 0;
            bool fsys = false;
            bool fsCount1 = false;
            bool fsCount2 = false;
            bool fStart = false;
            bool fSeq = false;
            bool not_group_number = false;
            bool lookingCode = false;

            string sys = "SYSOUT ID: ";
            string sysCierant = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string seqNN = "                             BOUNCE NOTIFICATION LETTER";
            string seqNN2 = ", 2016";
           

            string final = "TOTAL PAGES PRINTED    -";
            sysout = jobname = pDate = aDate = seqNum = scount = LetterProduced = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int currLine = 0;
            int valueOk = 0;
            bool isrecord = false;
            bool isduedates = false;
            bool isPrevduedates = false;
            bool finisfduedates = false;
            string line;
            bool lookingStatementdate = false;
            bool lookingTotal = false;
            bool lookingTotalFound = false;
            bool ismemebership = false;
            bool wasInvNumber = false;
            bool lookingAddr = false;


            NoticeLetter.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if ((line.IndexOf(sys) != -1 || line.IndexOf(sysCierant) == 0) && !fsys)
                    {
                        if (line.IndexOf(sysCierant) == 0)
                        {
                            while (line.Contains("  ")) line = line.Replace("  ", " ");
                            string[] words = line.Replace("  ", " ").Trim().Split(' ');

                            sysout = words[1];
                            jobname = words[3];
                            pDate = words[6];

                            //sysout = searchText(sys, line, currLine);
                            //jobname = searchText(jobn, line, currLine);
                            //pDate = searchText(pdat, line, currLine);
                            aDate = "";
                            fsys = true;
                            fsCount1 = false;
                            details.Clear();
                        }
                        else
                        {
                            sysout = searchText(sys, line, currLine);
                            jobname = searchText(jobn, line, currLine);
                            pDate = searchText(pdat, line, currLine);
                            aDate = searchText(adat, line, currLine);
                            fsys = true;
                            fsCount1 = false;
                            fsCount2 = false;
                        }
                    }


                    if ((line.IndexOf(seqNN) != -1 && fsys))
                    {
                        //seqNum = searchText2(seqNN, line, currLine);   //  is past due notice line
                        startRecLine = currLine;
                        fsCount1 = true;
                       
                        isrecord = false;
                        lookingAddr = false;
                        ismemebership = false;
                    }
                    if ((fsCount1 && line.IndexOf(seqNN2)>  10 && fsys))
                    {
                        startRecLine = currLine;
                        details.Add(line.Trim());
                        isrecord = true;
                       
                        fsCount2 = false;
                        not_group_number = true;
                    }
                    //if (fsCount1 && currLine == startRecLine + 2)
                    //{
                    //    if (line.IndexOf("XXXXXXXX 99, 9999") == -1)
                    //    {
                    //        details.Add(line.Trim());
                    //        isrecord = true;
                    //        startRecLine = currLine;
                    //        fsCount1 = false;
                    //    }
                    //    else
                    //    {
                    //        isrecord = false;
                    //        fsCount1 = false;
                    //    }
                    //}


                    if (isrecord && currLine > startRecLine )
                    {
                        if (line.IndexOf("DEAR") != -1)
                        {
                            details.Add(line.Trim());
                            isrecord = true;
                            startRecLine = currLine;
                        }

                    }

                    if (isrecord && currLine > startRecLine && line.IndexOf("Date Returned") != -1)
                    {
                        isPrevduedates = true;
                        linesDue = 0;
                        startRecLine = currLine;
                        
                    }
                    if (isrecord && isPrevduedates && currLine > startRecLine && line.IndexOf("____________") != -1)
                    {
                        isPrevduedates = false;
                        isduedates = true;
                        linesDue = 0;
                        startRecLine = currLine;
                        
                    }
                    if (isduedates && isrecord)
                    {
                        if (currLine != startRecLine)
                        {
                            if (line.Length > 1)
                            {
                                if (finisfduedates)
                                {
                                    isduedates = false;
                                    finisfduedates = false;
                                    details.Add(line.Trim());
                                    startRecLine = currLine;
                                }
                                if (line.IndexOf("________________") != -1)
                                {
                                    //isduedates = false;
                                    finisfduedates = true;
                                    lookingTotalFound = true;
                                    startRecLine = currLine;
                                    if (linesDue < 40)
                                    {
                                        do
                                        {
                                            details.Add("");
                                            linesDue++;
                                        } while (linesDue < 40);
                                    }
                                    //string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                                    //details.Add(total.Trim());

                                }
                                else
                                {
                                    if (line.IndexOf("_______________") == -1 && line.IndexOf("--------------") == -1 && linesDue < 40)
                                    {
                                        //string nline = line.Replace("Previous Balance", "PreviousBalance");
                                        string[] words = line.Trim().Split(' ');    //Previous Balance
                                        int numberElements = 0;
                                        foreach (string word in words)
                                        {
                                            if (word.Length > 0)
                                            {

                                                //numberElements++;
                                                //if (word == "PreviousBalance")
                                                //{
                                                //    details.Add("Previous Balance");
                                                //    linesDue++;
                                                //}

                                                //else
                                                //{
                                                    details.Add(word.Trim());
                                                    linesDue++;
                                                //}
                                                //if (not_group_number && numberElements == 1)
                                                //{
                                                //    details.Add("");
                                                //    linesDue++;
                                                //}
                                            }
                                        }
                                    }
                                }
                            }
                        }

                    }
                    //if (finisfduedates && currLine > startRecLine + 1 && line.IndexOf("TOTAL AMOUNT PAST DUE") != -1)
                    //{
                    //    string total = searchText2("TOTAL AMOUNT PAST DUE", line, currLine);
                    //    details.Add(total.Trim());
                    //}
                    //if (finisfduedates && currLine > startRecLine && line.IndexOf("was due on") != -1)
                    //{
                    //    int poscBlank = line.IndexOf("was due on", 1) + 11;
                    //    int poscBlank2 = line.IndexOf(".", poscBlank + 11);


                    //    string date = line.Substring(poscBlank, poscBlank2 - poscBlank);
                    //    details.Add(date.Trim().Replace("  ", " "));
                    //    lookingTotal = true;
                    //}

                    if (isrecord && lookingTotalFound && currLine > startRecLine && line.IndexOf("Pay This Amount        Amount Enclosed") != -1)
                    {
                        //lookingTotalFound = true;
                        lookingTotal = true;
                        startRecLine = currLine;
                    }
                    if ( isrecord && lookingTotal && currLine > startRecLine && line.IndexOf("$") != -1)
                    {
                        lookingTotalFound = true;
                        lookingTotal = false;
                        startRecLine = currLine;
                        wasInvNumber = false;
                        ismemebership = true;
                        details.Add(line.Trim());
                        startRecLine = currLine;
                        lookingStatementdate = true;
                    }



                    if (isrecord &&  ismemebership && currLine > startRecLine && line.Length > 1)
                    {
                        if (lookingStatementdate && line.IndexOf("Statement Date:") != -1)
                        {
                            string result = searchText2("Statement Date:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf("Account Number:") != -1)
                        {
                            string result = searchText2("Account Number:", line, currLine);
                            details.Add(result.Trim());
                        }
                        if (lookingStatementdate && line.IndexOf("Invoice Number:") != -1)
                        {
                            string result = searchText2("Invoice Number:", line, currLine);
                            details.Add(result.Trim());
                            lookingCode = true;
                            startRecLine = currLine;
                            ismemebership = false;
                        }
                    }
                    if (isrecord && lookingCode && currLine > startRecLine)
                    {
                        if (line.Length > 1)
                        {
                            details.Add(line.Trim());
                            lookingCode = false;
                            lookingAddr = true;
                            startRecLine = currLine;
                        }
                    }
                    if (isrecord && lookingAddr && currLine > startRecLine)
                    {
                        string addLine = "";
                        if (line.Length > 1)
                        {

                            if (line.Length < 53)
                                addLine = line.Replace("  ", " ").Replace("-", "").Trim();
                            else
                                addLine = line.Trim();

                            if (addLine.Length > 1)
                            {

                                double n;
                                bool isNumeric = double.TryParse(addLine.Replace(" ", ""), out n);
                                if (isNumeric && addLine.Trim().Length < 30)
                                {
                                    details.Add(line.Trim());
                                    //lookingAddr = false;

                                    //if (details.Count < 65)
                                    //    details.Add("");
                                    //addToTable(currLine, "6003");


                                    //isrecord = false;
                                }
                                else
                                {
                                    if (!isNumeric)
                                    {
                                        try
                                        {
                                            string addr = line.Substring(0, 52).Trim();
                                            details.Add(addr.Trim());
                                        }
                                        catch (Exception ex)
                                        {
                                            string addr = line.Trim();
                                            details.Add(addr.Trim());

                                        }
                                    }
                                }
                                if (isNumeric && addLine.Trim().Length > 30)
                                {
                                    details.Add(line.Trim());
                                    //do
                                    //{
                                    //    details.Add("");
                                    //    linesDue++;
                                    //} while (details.Count < 65);

                                    addToTable(currLine, "6003");
                                    isrecord = false;
                                    lookingAddr = false;

                                }
                            }
                        }
                    }
                    if (line.IndexOf(final) > 0 && fsys)
                    {
                        int poscHad = line.IndexOf("-");
                        LetterProduced = line.Substring(poscHad + 1, line.Length - poscHad - 1).Replace("*", "").Trim();
                    }
                    currLine++;
                    if (currLine == 114)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n";
                    errorcount++;
                }
            }
            //wr.Flush();
            //wr.Close();
            if (sysout != "")
            {
                file.Close();
                foreach (DataRow row in NoticeLetter6003.Rows)
                {
                    for (int ii = 63; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[63] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }
                DataTable output_NoticeLetter;
                output_NoticeLetter = NoticeLetter6003.Copy();


                string colnames = "";

                createCSV createcsv = new createCSV();
                string pName = ProcessVars.oNoticeDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                {
                    string nColname = "";
                    string colname = output_NoticeLetter.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                   
                        fieldnames.Add(colname);
                   
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in output_NoticeLetter.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }


                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_TMP_6003_PastDueNotice");

                int errors = 0;
                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_TMP_6003_PastDueNotice([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in output_NoticeLetter.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < output_NoticeLetter.Columns.Count; index++)
                    {
                        insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''") + "','";
                    }
                    try
                    {
                        recnumError = row[0].ToString();
                        var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                    }
                    catch (Exception ex)
                    {
                        errors++;
                    }
                }
                if (errors == 0)
                {
                    dbU.ExecuteScalar("Insert into HOR_parse_6003_PastDueNotice select * from HOR_parse_TMP_6003_PastDueNotice");
                    dbU.ExecuteScalar("delete from HOR_parse_TMP_6003_PastDueNotice");
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_NL_PastDueNotice', GETDATE())");

                }

                dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess ) values(" +
                                           output_NoticeLetter.Rows.Count + ",'" + output_NoticeLetter.Rows.Count + "','" + fileInfo.Name + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "','" + dateFile + "','HOR_parse_6003_PastDueNotice" + "','No CASS','" + sysout + "','" + jobname + "','Receive','Y','" + GlobalVar.DateofProcess + "')");

            }
            else
            {
                processCompleted = "No SYSOUT ID file " + fileName;
                errorcount++;
            }
            if (errorcount != 0)
                processCompleted = processCompleted + " errors " + errorcount;
            return processCompleted;
        }

        public void produceSummary( )
        {

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();


            processedData = dbU.ExecuteDataTable("HOR_ZZ_rpt_errors_parse_PastDueNotice_daily");
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>Files Imported for PastDue Notice</p>");
            if (processedData.Rows.Count > 0)
            {
                
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

                SendMails sendmail = new SendMails();
                sendmail.SendMail("PastDue Notice Upload", "tkrompinger@apps.cierant.com, kcarpenter@apps.cierant.com, alalla@apps.cierant.com,cgaytan@apps.cierant.com,rchico@apps.cierant.com",
                    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);  //tkrompinger@apps.cierant.com

            }


        }
        public void addToTable(int currline, string tabName)
        {
            string test = "";
            
            //if (tabName == "5303")
            //    row = NoticeLetter5303.NewRow();
            //else if (tabName == "6003")
            //    row = NoticeLetter6003.NewRow();
            //else
            //    NoticeLetter.NewRow();
            var row = tabName == "5303" ? NoticeLetter5303.NewRow() : NoticeLetter.NewRow();
            if (tabName == "6003")
                row = tabName == "6003" ? NoticeLetter6003.NewRow() : NoticeLetter.NewRow();
            try
            {
                row["Recnum"] = GRecnum;
                row["Sysout"] = sysout;
                row["line_exception"] = currline;
                row["Jobname"] = jobname;
                row["PrintDate"] = pDate;
                row["ArchiveDate"] = aDate;
                row["C_Recnum"] = Recnum;
                row["Seq"] = seqNum;


                if (tabName == "5303")
                {
                    for (int j = 8; j < NoticeLetter5303.Columns.Count; j++)
                    {
                        row[j] = details[j - 8].Trim();

                    }
                    NoticeLetter5303.Rows.Add(row);
                }
                else if (tabName == "6003")
                {
                    try
                    {
                        for (int j = 8; j < NoticeLetter6003.Columns.Count -2; j++)
                        {
                            row[j] = details[j - 8].Trim();
                            if (j == 59)
                                tabName = tabName;
                        }
                        NoticeLetter6003.Rows.Add(row);
                    }
                    catch (Exception ex)
                    {
                        tabName = tabName;
                    }
                }
                else
                {
                    for (int j = 8; j < NoticeLetter.Columns.Count - 2; j++)
                    {
                        row[j] = details[j - 8].Trim();
                        //if(j + 1 > details.Count)
                        //    break;
                    }

                    NoticeLetter.Rows.Add(row);
                }

                details.Clear();
                countinAddr = 0;
                Recnum++;
                GRecnum++;
            }
            catch (Exception ex)
            {
                string error = ex.Message + "\n\n";
            }
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
                string seg1 = line.Substring(line.IndexOf(valuetosearch) + valuetosearch.Length);
                return seg1;
            }
            catch (Exception ex)
            {
                errorcount++;
                return "error line " + currline;
            }
        }
        public string searchText2A(string valuetosearch, string line, int currline)
        {
            try
            {
                string seg1 = line.Substring(line.IndexOf(valuetosearch) );
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
        private static DataTable NoticeLetter_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("Sysout");
            newt.Columns.Add("line_exception");
            newt.Columns.Add("Jobname");
            newt.Columns.Add("PrintDate");
            newt.Columns.Add("ArchiveDate");
            newt.Columns.Add("C_Recnum");
            newt.Columns.Add("Seq");
            newt.Columns.Add("LetterDate");
            newt.Columns.Add("Dear");
            newt.Columns.Add("Due1");
            newt.Columns.Add("GroupNum1");
            newt.Columns.Add("PastAmount1");
            newt.Columns.Add("Due2");
            newt.Columns.Add("GroupNum2");
            newt.Columns.Add("PastAmount2");
            newt.Columns.Add("Due3");
            newt.Columns.Add("GroupNum3");
            newt.Columns.Add("PastAmount3");
            newt.Columns.Add("Due4");
            newt.Columns.Add("GroupNum4");
            newt.Columns.Add("PastAmount4");
            newt.Columns.Add("Due5");
            newt.Columns.Add("GroupNum5");
            newt.Columns.Add("PastAmount5");
            newt.Columns.Add("Due6");
            newt.Columns.Add("GroupNum6");
            newt.Columns.Add("PastAmount6");
            newt.Columns.Add("Due7");
            newt.Columns.Add("GroupNum7");
            newt.Columns.Add("PastAmount7");
            newt.Columns.Add("Due8");
            newt.Columns.Add("GroupNum8");
            newt.Columns.Add("PastAmount8");
            newt.Columns.Add("Due9");
            newt.Columns.Add("GroupNum9");
            newt.Columns.Add("PastAmount9");
            newt.Columns.Add("Due10");
            newt.Columns.Add("GroupNum10");
            newt.Columns.Add("PastAmount10");
            newt.Columns.Add("PayDue");
            newt.Columns.Add("PaymentDueOn");
            newt.Columns.Add("T_amtDue");
            newt.Columns.Add("T_MonthCharges");
            newt.Columns.Add("T_ThisMonth");
            newt.Columns.Add("T_DueDate");
            newt.Columns.Add("StatmDate");
            newt.Columns.Add("AcctN");
            newt.Columns.Add("InvoiceN");
            newt.Columns.Add("Group");
            newt.Columns.Add("GroupNo");
            //newt.Columns.Add("SeqEnvelope");
            newt.Columns.Add("Addr1");
            newt.Columns.Add("Addr2");
            newt.Columns.Add("Addr3");
            newt.Columns.Add("Addr4");
            newt.Columns.Add("Addr5");
            newt.Columns.Add("Addr6");
            newt.Columns.Add("OCR");
            return newt;
        }
       
        private static DataTable NoticeLetter6003_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("Sysout");
            newt.Columns.Add("line_exception");
            newt.Columns.Add("Jobname");
            newt.Columns.Add("PrintDate");
            newt.Columns.Add("ArchiveDate");
            newt.Columns.Add("C_Recnum");
            newt.Columns.Add("Seq");
            newt.Columns.Add("LetterDate");
            newt.Columns.Add("Dear");

            newt.Columns.Add("chk1");
            newt.Columns.Add("Dep1");
            newt.Columns.Add("Amt1");
            newt.Columns.Add("DateR1");
            newt.Columns.Add("chk2");
            newt.Columns.Add("Dep2");
            newt.Columns.Add("Amt2");
            newt.Columns.Add("DateR2");
            newt.Columns.Add("chk3");
            newt.Columns.Add("Dep3");
            newt.Columns.Add("Amt3");
            newt.Columns.Add("DateR3");
            newt.Columns.Add("chk4");
            newt.Columns.Add("Dep4");
            newt.Columns.Add("Amt4");
            newt.Columns.Add("DateR4");
            newt.Columns.Add("chk5");
            newt.Columns.Add("Dep5");
            newt.Columns.Add("Amt5");
            newt.Columns.Add("DateR5");
            newt.Columns.Add("chk6");
            newt.Columns.Add("Dep6");
            newt.Columns.Add("Amt6");
            newt.Columns.Add("DateR6");
            newt.Columns.Add("chk7");
            newt.Columns.Add("Dep7");
            newt.Columns.Add("Amt7");
            newt.Columns.Add("DateR7");
            newt.Columns.Add("chk8");
            newt.Columns.Add("Dep8");
            newt.Columns.Add("Amt8");
            newt.Columns.Add("DateR8");
            newt.Columns.Add("chk9");
            newt.Columns.Add("Dep9");
            newt.Columns.Add("Amt9");
            newt.Columns.Add("DateR9");
            newt.Columns.Add("chk10");
            newt.Columns.Add("Dep10");
            newt.Columns.Add("Amt10");
            newt.Columns.Add("DateR10");


            newt.Columns.Add("PayTot");
            newt.Columns.Add("PayThis");
            newt.Columns.Add("StatmDate");
            newt.Columns.Add("AcctN");
            newt.Columns.Add("InvoiceN");
            newt.Columns.Add("Code1");
            newt.Columns.Add("Code2");
            //newt.Columns.Add("SeqEnvelope");
            newt.Columns.Add("Addr1");
            newt.Columns.Add("Addr2");
            newt.Columns.Add("Addr3");
            newt.Columns.Add("Addr4");
            newt.Columns.Add("Addr5");
            newt.Columns.Add("Addr6");
            newt.Columns.Add("OCR");
            return newt;
        }

        private static DataTable NoticeLetter5303_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("Sysout");
            newt.Columns.Add("line_exception");
            newt.Columns.Add("Jobname");
            newt.Columns.Add("PrintDate");
            newt.Columns.Add("ArchiveDate");
            newt.Columns.Add("C_Recnum");
            newt.Columns.Add("Seq");
            newt.Columns.Add("StatmCode");
            newt.Columns.Add("StatmDate");
            newt.Columns.Add("StatmTotal");
            newt.Columns.Add("SeqEnvelope");
            newt.Columns.Add("Addr1");
            newt.Columns.Add("Addr2");
            newt.Columns.Add("Addr3");
            newt.Columns.Add("Addr4");
            newt.Columns.Add("Addr5");
            newt.Columns.Add("Addr6");
            newt.Columns.Add("OCR");
            newt.Columns.Add("LetterDate");
            newt.Columns.Add("Dear");
            newt.Columns.Add("Due1");
            newt.Columns.Add("PastAmount1");
            newt.Columns.Add("Due2");
            newt.Columns.Add("PastAmount2");
            newt.Columns.Add("Due3");
            newt.Columns.Add("PastAmount3");
            newt.Columns.Add("Due4");
            newt.Columns.Add("PastAmount4");
            newt.Columns.Add("Due5");
            newt.Columns.Add("PastAmount5");
            newt.Columns.Add("Due6");
            newt.Columns.Add("PastAmount6");
            newt.Columns.Add("Due7");
            newt.Columns.Add("PastAmount7");
            newt.Columns.Add("Due8");
            newt.Columns.Add("PastAmount8");
            newt.Columns.Add("Due9");
            newt.Columns.Add("PastAmount9");
            newt.Columns.Add("Due10");
            newt.Columns.Add("PastAmount10");
            newt.Columns.Add("PayDue");  //37
            newt.Columns.Add("PaymentDueOn");

            return newt;
        }
    }
}