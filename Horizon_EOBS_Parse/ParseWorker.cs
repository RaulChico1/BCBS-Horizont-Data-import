using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class ParseWorker
    {
        AutoResetEvent autoEvent = new AutoResetEvent(false);
        DataTable EOBs = EOBs_Table();
        List<string> addrs = new List<string>();
        int Recnum = 1;
        int C_Recnum = 1;
        int countinAddr = 0;
        string inaddr = "";
        string sysout, jobname, jobID, pDate, aDate, seqNum, scount, mailStop;
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
            
            if (Directory.Exists(ProcessVars.InputDirectory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(ProcessVars.InputDirectory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("*.txt");
                foreach (FileInfo file in FilesPDF)
                {
                    if (file.Name.IndexOf("DISPATCH") == -1)
                    {
                        try
                        {
                            string error = evaluate_TXT(file.FullName);
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
        public string evaluate_TXT(string fileName)
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

            string sys = "  CIERANT ";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string add1 = "111    ";
            string add2 = "118    ";
            string add3 = "119    ";
            sysout = jobname = jobID = pDate = aDate = seqNum = scount = mailStop = string.Empty;
            
            string processCompleted = "";
            
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int currLine = 0;
            int valueOk = 0;
            string line;
            EOBs.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {
                        string wLine = line.TrimStart();
                        while (wLine.Contains("  ")) wLine = wLine.Replace("  ", " ");
                        string[] words = wLine.Split(' ');

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        //sysout = searchText(sys, line, currLine);
                        //jobname = searchText(jobn, line, currLine);
                        //pDate = searchText(pdat, line, currLine);
                        //aDate = searchText(adat, line, currLine);
                        fsys = true;
                    }
                    if (line.IndexOf("DJDE ") != -1 && line.IndexOf("'START'") != -1 && !fStart)
                    {
                        prevline = currLine;
                        fsCount = true;
                    }
                    if (currLine - 1 == prevline && line.IndexOf(sysout) != -1 && fsCount)
                    {
                        scount = searchTextSTART(sysout, line, currLine);
                        fsCount = false;
                        prevline = 0;
                    }
                    if (line.IndexOf(sysout) != -1 && fsCount)
                    {
                        if (line.IndexOf(sysout) != -1 && fsCount)
                        {
                            scount = searchTextSTART(sysout, line, currLine);
                            fsCount = false;
                            prevline = 0;
                        }
                    }
                    //if (line.IndexOf("118 ") != -1 && !fsCount)
                    //{
                    //    scount = searchText("118 ", line);
                    //    fsCount = true;
                    //}
                    if (line.IndexOf(add1) == 0 || line.IndexOf(add2) == 0 || line.IndexOf(add3) == 0)
                    {
                        int n;
                        bool isNumeric = int.TryParse(line.Substring(line.Length - 6), out n);
                        if (isNumeric)
                        {
                            inaddr = line.Substring(0, 3);
                            mailStop = line.Substring(4, line.Length - 10);
                            seqNum = line.Substring(line.Length - 6);
                            countinAddr = 0;
                        }
                        else
                            inaddr = "";

                    }

                    if (inaddr != "" && countinAddr < 6)
                    {
                        if (inaddr == "111" && line.Substring(0, 3) == "  2")
                        {
                            if (line != "  2")
                            {
                                countinAddr++;
                                addrs.Add(line.Substring(3, line.Length - 3));

                                if (countinAddr == 6)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                            else if (line == "  2" && countinAddr > 1)
                            {
                                countinAddr++;
                                addrs.Add("");
                                if (countinAddr == 6)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                        }
                        //=====  3
                        if (inaddr == "111" && line.Substring(0, 3) == "  3")
                        {
                            if (line != "  3")
                            {
                                countinAddr++;
                                addrs.Add(line.Substring(3, line.Length - 3));

                                if (countinAddr == 5)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                            else if (line == "  3" && countinAddr > 1)
                            {
                                countinAddr++;
                                addrs.Add("");
                                if (countinAddr == 6)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                        }
                        //====
                        //=====  118  6
                        if (inaddr == "118" && line.Substring(0, 3) == "  8")
                        {
                            if (line != "  8")
                            {
                                countinAddr++;
                                addrs.Add(line.Substring(3, line.Length - 3));

                                if (countinAddr == 5)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                            else if (line == "  8" && countinAddr > 1)
                            {
                                countinAddr++;
                                addrs.Add("");
                                if (countinAddr == 6)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                        }
                        //====
                        //=====  119   3
                        if (inaddr == "119" && line.Substring(0, 3) == "  3")
                        {
                            if (line != "  3")
                            {
                                countinAddr++;
                                addrs.Add(line.Substring(3, line.Length - 3));

                                if (countinAddr == 5)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                            else if (line == "  3" && countinAddr > 1)
                            {
                                countinAddr++;
                                addrs.Add("");
                                if (countinAddr == 6)
                                {
                                    if (addrs.Count > 2)
                                    {
                                        addToTable();
                                    }
                                }
                            }
                        }
                        //====
                    }
                    currLine++;
                    if (currLine == 29816)
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

                foreach (DataRow row in EOBs.Rows)
                {
                    for (int ii = 21; ii > 0; ii-- )
                    {
                        if(row[ii].ToString() != "")
                        {
                            row[21] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }
                DataTable working_EOBs = EOBs.Copy();
                working_EOBs.Columns.Remove("Sheet_count");
                working_EOBs.Columns.Remove("mailStop");
                working_EOBs.Columns.Remove("MED_Flag");

                createCSV createcsv = new createCSV();
               // string pName = ProcessVars.OutputDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";  // +DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                string directoryAfterCass = ProcessVars.OutputDirectory + "FromCASS";
                string pName = ProcessVars.OutputDirectory + pNameToCASS;

                //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < working_EOBs.Columns.Count; index++)
                {
                    fieldnames.Add(working_EOBs.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in working_EOBs.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < working_EOBs.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }
                //copy to CASS
                string cassFileName = ProcessVars.gDMPs + pNameToCASS;
                File.Move(pName, cassFileName);

                // add to dbase
                string colnames = "";
                for (int index = 0; index < EOBs.Columns.Count; index++)
                {
                    string colname = EOBs.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_TMP_EOBS");

                int errors = 0;
                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_TMP_EOBS([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in EOBs.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < EOBs.Columns.Count; index++)
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
                    int totrecs = EOBs.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_EOBs select * from HOR_parse_TMP_EOBs");
                    dbU.ExecuteScalar("delete from HOR_parse_TMP_EOBs");
                    // create store proc to delete if exist
                    int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
                    if (FileCount == 0)
                    {
                        dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, SentDate, TableName,DirectoryTo) values(" +
                                           totrecs + ",'" + pNameToCASS + "','" + fileInfo.Name + "', GETDATE(),'HOR_parse_EOBs','" + directoryAfterCass + "')");
                    }
                    else
                    {
                        dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set RecordsNum =" +
                                           totrecs + ", SentDate = GETDATE(), TableName = 'HOR_parse_Checks', Processed = NULL " +
                                           ",DirectoryTo = '" + directoryAfterCass + "' where FileNameCASS = '" + pNameToCASS + "'");

                    }
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_EOBs', GETDATE())");
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
        public void addToTable()
        {
            var row = EOBs.NewRow();
            row["Recnum"] = Recnum;
            row["Sysout"] = sysout;
            row["Sheet_count"] = scount;
            row["Jobname"] = jobname;
            row["PrintDate"] = pDate;
            row["ArchiveDate"] = aDate;
            row["C_Recnum"] = C_Recnum;
            row["Seq"] = seqNum;
            row["mailStop"] = mailStop.Trim();
            row["JOBID"] = jobID;
            row["Addr1"] = addrs[0];
            row["Addr2"] = addrs[1];
            row["Addr3"] = addrs[2];
            row["Addr4"] = addrs[3];
            row["Addr5"] = addrs[4];
            row["Addr6"] = addrs[5];
            row["MED_Flag"] = "N";
            EOBs.Rows.Add(row);
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
            newt.Columns.Add("Sheet_count");
            newt.Columns.Add("Jobname");
            newt.Columns.Add("PrintDate");
            newt.Columns.Add("ArchiveDate");
            newt.Columns.Add("C_Recnum");
            newt.Columns.Add("Seq");
            newt.Columns.Add("mailStop");
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
            return newt;
        }
    }
}
