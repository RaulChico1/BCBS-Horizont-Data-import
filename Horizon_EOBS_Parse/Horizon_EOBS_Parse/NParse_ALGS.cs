using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class NParse_ALGS
    {
        DataTable DataTable = Data_Table();
        List<string> addrs = new List<string>();
        int Recnum = 1;
        int C_Recnum = 1;
        int countinAddr = 0;
        string inaddr = "";
        string FileClass = "";
        string sysout, psysout, form, jobname, jobID, pDate, aDate, seqNum, scount, mailStop, checknumber, Feed, docDate, LetterProduced, idnum;
        string errors = "";
        int errorcount = 0;
        int currLine = 0;
        DBUtility dbU;

        public string evaluate_TXT(string fileName, string secondValue, string LastWriteTime, string newSYSID)
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
            bool fDJDE = false;
            bool fDate = false;
            bool fSeqn = false;
            bool fsAddrs = false;
            bool fsCount = false;

            bool fDJDE2 = false;
            bool fDate2 = false;
            bool fSeqn2 = false;
            bool fsAddrs2 = false;


            string sys = "  CIERANT";
            string add1 = "1DJDE FORMAT=NASAR1";
            string add1v = "1DJDE FORMAT=FEHBP1";
            string addv2 = "1DJDE FORMAT=NASCO, FORM=NALGS2";
            string final = "TOTAL PAGES IN REPORT   -";

            sysout = form = psysout = jobname = pDate = jobID = aDate = seqNum = scount = mailStop = checknumber = Feed = docDate = LetterProduced = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            currLine = 0;
            int valueOk = 0;
            string line;
            DataTable.Clear();

            
            FileClass = fileInfo.Name.Substring(0 , fileInfo.Name.IndexOf("-"));
             
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
                        jobID = words[2];
                        pDate = words[3];
                        fsys = true;

                    }
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {
                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');    //Previous Balance

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;
                    }


                    if ((line.IndexOf(add1) == 0 || line.IndexOf(add1v) == 0 ) && fsys)
                    {
                        countinAddr = 0;
                        prevline = currLine;
                        fDJDE = true;
                        inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                       
                    }
                    if (currLine > prevline && line.IndexOf("11") == 0 && fDJDE)
                    {
                        prevline = currLine;
                        fDJDE = false;
                        fDate = true;
                        docDate = line.Substring(3, line.Length - 3);
                    }
                    if (currLine > prevline && line.IndexOf("12") == 0 && fDate)
                    {
                        prevline = currLine;
                        fSeqn = true;
                        fDate = false;
                        scount = line.Substring(3, 5);
                    }
                    if (currLine > prevline && line.IndexOf("11") == 0 && fSeqn)
                    {
                        prevline = currLine;
                        fSeqn = false;
                        fsAddrs = true;
                        countinAddr++;
                        addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    if (currLine > prevline && line.IndexOf(" 1") == 0 && fsAddrs)
                    {
                        prevline = currLine;
                        countinAddr++;
                        if(line.Length > 2)
                        addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    if (currLine > prevline && line.IndexOf("13") == 0 && fsAddrs)
                    {
                        while (countinAddr < 5)
                        {
                            countinAddr++;
                            addrs.Add("");
                        }

                        fsAddrs = false;
                        addToTable();
                    }
                    //=================
                    if (line.IndexOf(addv2) == 0 && fsys)
                    {
                        countinAddr = 0;
                        prevline = currLine;
                        fDJDE2 = true;
                        inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                    }
                    if (currLine > prevline && line.IndexOf("13") == 0 && fDJDE2)
                    {
                        prevline = currLine;
                        fDJDE2 = false;
                        fDate2 = true;
                        docDate = line.Substring(3, line.Length - 3);
                    }
                    if (currLine > prevline && line.IndexOf("13") == 0 && fDate2)
                    {
                        prevline = currLine;
                        fSeqn2 = true;
                        fDate2 = false;
                        scount = line.Substring(3, 5);
                    }
                    if (currLine > prevline && line.IndexOf("12") == 0 && fSeqn2)
                    {
                        prevline = currLine;
                        fSeqn2 = false;
                        fsAddrs2 = true;
                        countinAddr++;
                        addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    if (currLine > prevline && line.IndexOf(" 2") == 0 && fsAddrs2)
                    {
                        prevline = currLine;
                        countinAddr++;
                        addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    if (currLine > prevline && line.IndexOf("14") == 0 && fsAddrs2)
                    {
                        while (countinAddr < 5)
                        {
                            countinAddr++;
                            addrs.Add("");
                        }

                        fsAddrs2 = false;
                        addToTable();
                    }
                    //=================
                    if (line.IndexOf(final) > 0 && fsys)
                    {
                        int poscHad = line.IndexOf("-");
                        LetterProduced = line.Substring(poscHad + 1, line.Length -poscHad -1);
                    }
                  
                    
                    //====
                    currLine++;
                    if (currLine == 4118)
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
            file.Close();
            createCAS_CSV create_cas__csv = new createCAS_CSV();
            if (errorcount == 0)
            {
                if (sysout != "")
                {
                    if (DataTable.Rows.Count > 0)
                    {
                        
                        string resultcsv = create_cas__csv.create_Default_CAS_CSV(
                                            fileName, DataTable, "ALGS", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "ALGS", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "ALGS", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "ALGS", "error count " + errorcount);
            }
            
            return processCompleted;
        }
        public string evaluate_TXTAR06(string fileName, string secondValue, string LastWriteTime,string newSYSID)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            dbU.ExecuteNonQuery("delete from HOR_parse_AR06 where filename = '" + fileInfo.Name + "'");
            dbU.ExecuteNonQuery("delete from HOR_parse_files_to_CASS where filename = '" + fileInfo.Name + "'");

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
            bool fDJDE = false;
            bool fDate = false;
            bool fSeqn = false;
            bool fsAddrs = false;
            bool fsCount = false;

            bool fDJDE2 = false;
            bool fDate2 = false;
            bool fSeqn2 = false;
            bool fsAddrs2 = false;


            string sys = "  CIERANT";
            string add1 = "1DJDE FORMAT=NASAR1";
            string add1v = "1DJDE FORMAT=FEHBP1";
            string addv2 = "1DJDE FORMAT=NASAR2, FORM=NASAR2";
            string final = "TOTAL PAGES IN REPORT   -";

            sysout = psysout = jobname = pDate = jobID = aDate = seqNum = scount = mailStop = checknumber = Feed = docDate = LetterProduced = idnum = string.Empty;

            string processCompleted = "";

            
            currLine = 0;
            int valueOk = 0;
            string line;
            DataTable.Clear();


            FileClass = fileInfo.Name.Substring(0, fileInfo.Name.IndexOf("-"));

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
                        jobID = words[2];
                        pDate = words[3];
                        fsys = true;

                    }
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {
                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');    //Previous Balance

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;
                    }


                    if ((line.IndexOf(add1) == 0 || line.IndexOf(add1v) == 0) && fsys)
                    {
                        countinAddr = 0;
                        prevline = currLine;
                        fDJDE = true;
                        inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                        form = line.Substring(line.IndexOf("FORM=", 1) + 5, 6);
                    }
                    if (currLine > prevline && line.IndexOf("11") == 0 && fDJDE)
                    {
                        prevline = currLine;
                        fDJDE = false;
                        fDate = true;
                        docDate = line.Substring(3, line.Length - 3);
                    }
                    
                    if (currLine > prevline && line.IndexOf("12") == 0 && fDate)
                    {
                        prevline = currLine;
                        fSeqn = true;
                        fDate = false;
                        scount = line.Substring(3, 6);
                    }
                    if (currLine > prevline && line.IndexOf("11") == 0 && fSeqn)
                    {
                        prevline = currLine;
                        fSeqn = false;
                        fsAddrs = true;
                        countinAddr++;
                        addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    if (currLine > prevline && line.IndexOf(" 1") == 0 && fsAddrs)
                    {
                        prevline = currLine;
                        countinAddr++;
                        if (line.Length > 2)
                            addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    //if (currLine > prevline && line.IndexOf(" 5") == 0 && fsAddrs)
                    if (currLine > prevline && line.IndexOf("13") == 0 && fsAddrs)
                    {
                        if (line.Length > 2)
                            idnum = line.Substring(3, line.Length - 3);
                        while (addrs.Count < 5)
                        {
                            countinAddr++;
                            addrs.Add("");
                        }

                        fsAddrs = false;
                        addToTable_AR06();
                    }
                    //=================
                    if (line.IndexOf(addv2) == 0 && fsys)
                    {
                        countinAddr = 0;
                        prevline = currLine;
                        fDJDE2 = true;
                        inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                    }
                    if (currLine > prevline && line.IndexOf("13") == 0 && fDJDE2 && line.Length > 2)
                    {
                        prevline = currLine;
                        fDJDE2 = false;
                        fDate2 = true;
                        docDate = line.Substring(3, line.Length - 3);
                    }
                    if (currLine > prevline && line.IndexOf("13") == 0 && fDate2 && line.Length > 2)
                    {
                        prevline = currLine;
                        fSeqn2 = true;
                        fDate2 = false;
                        scount = line.Substring(3, 5);
                    }
                    if (currLine > prevline && line.IndexOf("12") == 0 && fSeqn2)
                    {
                        prevline = currLine;
                        fSeqn2 = false;
                        fsAddrs2 = true;
                        countinAddr++;
                        addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    if (currLine > prevline && line.IndexOf(" 2") == 0 && fsAddrs2)
                    {
                        prevline = currLine;
                        countinAddr++;
                        addrs.Add(line.Substring(3, line.Length - 3));
                    }
                    if (currLine > prevline && line.IndexOf("14") == 0 && fsAddrs2)
                    {
                        while (addrs.Count < 5)
                        {
                            countinAddr++;
                            addrs.Add("");
                        }

                        fsAddrs2 = false;
                        addToTable_AR06();
                    }
                    //=================
                    if (line.IndexOf(final) > 0 && fsys)
                    {
                        int poscHad = line.IndexOf("-");
                        LetterProduced = line.Substring(poscHad + 1, line.Length - poscHad - 1);
                    }


                    //====
                    currLine++;
                    if (currLine == 720 || currLine == 1404)
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
            file.Close();
            createCAS_CSV create_cas__csv = new createCAS_CSV();
            if (errorcount == 0)
            {
                if (sysout != "")
                {
                    if (DataTable.Rows.Count > 0)
                    {

                        string resultcsv = create_cas__csv.create_Default_CAS_F101(
                                            fileName, DataTable, "AR06", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "AR06", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "AR06", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "AR06", "error count " + errorcount);
            }

            return processCompleted;
        }
        public void addToTable()
        {
            var row = DataTable.NewRow();
            row["Recnum"] = Recnum;
            row["Sysout"] = sysout;
            //row["Sheet_count"] = scount;
            row["Jobname"] = jobname;
            row["PrintDate"] = pDate;
            row["ArchiveDate"] = aDate;
            row["C_Recnum"] = C_Recnum;
            row["Seq"] = scount; // seqNum;
            row["JOBID"] = jobID;
            //row["mailStop"] = mailStop.Trim();
            row["Field2"] = docDate;
            row["Field3"] = currLine;
            row["Field4"] = Feed;
            row["Addr1"] = addrs[0];
            row["Addr2"] = addrs[1];
            row["Addr3"] = addrs[2];
            row["Addr4"] = addrs[3];
            //row["Addr5"] = addrs[4];
            //row["Addr6"] = addrs[5];
            row["MED_Flag"] = "N";
            row["JobClass"] = FileClass;
            DataTable.Rows.Add(row);
            addrs.Clear();
            countinAddr = 0;
            Recnum++;
            C_Recnum++;
            inaddr = "";
            checknumber = "";

        }
        public void addToTable_AR06()
        {
            var row = DataTable.NewRow();
            row["Recnum"] = Recnum;
            row["Sysout"] = sysout;
            //row["Sheet_count"] = scount;
            row["Jobname"] = jobname;
            row["PrintDate"] = pDate;
            row["ArchiveDate"] = aDate;
            row["C_Recnum"] = C_Recnum;
            row["Seq"] = scount; // seqNum;
            row["JOBID"] = jobID;
            //row["mailStop"] = mailStop.Trim();
            row["Field2"] = idnum;
            row["Field3"] = currLine;
            row["Field4"] = Feed;
            row["Field5"] = scount;
            row["Field6"] = form;
            row["Addr1"] = addrs[0];
            row["Addr2"] = addrs[1];
            row["Addr3"] = addrs[2];
            row["Addr4"] = addrs[3];
            //row["Addr5"] = addrs[4]; 
            //row["Addr6"] = addrs[5];
            row["MED_Flag"] = "N";
            row["JobClass"] = FileClass;
            DataTable.Rows.Add(row);
            addrs.Clear();
            countinAddr = 0;
            Recnum++;
            C_Recnum++;
            inaddr = "";
            checknumber = "";

        }
        public string searchText(string valuetosearch, string line, int currline)
        {
            try
            {
                //int poscBlank = line.IndexOf(" ", line.IndexOf(valuetosearch) + valuetosearch.Length);
                string seg1 = line.Substring(line.IndexOf(valuetosearch) + valuetosearch.Length).TrimStart();
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
                int poscBlank = line.IndexOf(" ", line.IndexOf(valuetosearch) + valuetosearch.Length);
                string seg1 = line.Substring(line.IndexOf(valuetosearch));
                string seg2 = seg1.Substring(0, seg1.IndexOf("DATE")).TrimEnd();
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
        private static DataTable Data_Table()
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
            newt.Columns.Add("JobClass");
            return newt;
        }
    }
}
