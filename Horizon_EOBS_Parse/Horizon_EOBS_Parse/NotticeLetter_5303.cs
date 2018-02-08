using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
   
     public class NotticeLetter_5303
    {
        DataTable NoticeLetter = EOBs_Table();
        List<string> details = new List<string>();
        int Recnum = 1;
        int GRecnum = 3197;
        int countinAddr = 0;
        string sysout, jobname, pDate, aDate, seqNum, scount;
        string errors = "";
        int errorcount = 0;


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

            if (Directory.Exists(ProcessVars.NoticeDirectory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(ProcessVars.NoticeDirectory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("*.txt");
                foreach (FileInfo file in FilesPDF)
                {
                    if (file.Name.IndexOf("EP005303") == 0)
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
            Recnum = 1;
            countinAddr = 1;

            int linesDue = 0;
            int startRecLine = 0;
            bool fsys = false;
            bool fsCount1 = false;
            string sys = "SYSOUT ID: ";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
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

            
            NoticeLetter.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {
                        sysout = searchText(sys, line, currLine);
                        jobname = searchText(jobn, line, currLine);
                        pDate = searchText(pdat, line, currLine);
                        aDate = searchText(adat, line, currLine);
                        fsys = true;
                        fsCount1 = false;
                        details.Clear();
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
                                details.Add(word.Replace("-"," -").Trim());
                            }
                        }
                        fsCount1 = false;
                        lookingSeq = true;
                        startRecLine = currLine;
                    }
                    if (lookingSeq && currLine > startRecLine )
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
                                    if (linesDue < 16)
                                    {
                                        do
                                        {
                                            details.Add("");
                                            linesDue++;
                                        } while (linesDue < 16);
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
                        addToTable(currLine);
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
            



                createCSV createcsv = new createCSV();
                string pName = ProcessVars.oNoticeDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < NoticeLetter.Columns.Count; index++)
                {
                    string colname = NoticeLetter.Columns[index].ColumnName;
                    
                        fieldnames.Add(colname);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in NoticeLetter.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < NoticeLetter.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
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
        public void addToTable(int currline)
        { 
            string ok = "";
            var row = NoticeLetter.NewRow();
            try
            {
                row["Recnum"] = GRecnum;
                row["Sysout"] = sysout;
                row["line"] = currline;
                row["Jobname"] = jobname;
                row["PrintDate"] = pDate;
                row["ArchiveDate"] = aDate;
                row["C_Recnum"] = Recnum;
                row["Seq"] = seqNum;

                for (int j = 8; j < NoticeLetter.Columns.Count  ; j++)
                {
                    row[j] = details[j - 8].Trim();
                    if (j == 37)
                        ok = "here";
                }


                NoticeLetter.Rows.Add(row);
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
        private static DataTable EOBs_Table()
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
            newt.Columns.Add("PayDue");  //37
            newt.Columns.Add("PaymentDueOn");

            return newt;
        }
    }
}