using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class NParse_UCDS
    {
     DataTable DataTable = Data_Table();
        List<string> addrs = new List<string>();
        int Recnum = 1;
        int C_Recnum = 1;
        int countinAddr = 0;
        bool isMEDADV = false;
        bool UCDSAR = false;
        string inaddr = "";
        string FileClass = "";
        string sysout, psysout, jobname, jobID, pDate, aDate, seqNum, scount, mailStop, checknumber, Feed, docDate, LetterProduced, Type;
        string errors = "";
        int errorcount = 0;
        int currLine = 0;
        DBUtility dbU;

        public string evaluate_TXT(string fileName, string secondValue,string LastWriteTime)
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
            bool fJobid = false;
            bool fsAddrs = false;
            bool specialScount = false;
            bool isUCDSI = false;
            bool inaddr111_3 = false;
            bool looking111_3 = false;
            string sys = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string add1 = "111  ";
            string add1a = "113  ";
            string add2 = "118  ";
            string add3 = "119  ";
            sysout = psysout = jobname = pDate = jobID = aDate = seqNum = scount = mailStop = checknumber = Feed = Type = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            Type = fileInfo.Name.ToUpper().ToString().Substring(4, 1);
            currLine = 0;
            int valueOk = 0;
            string line;
            DataTable.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            if (fileInfo.Name == "UCDSQ001-20150618011004.txt")
                inaddr = "";
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {

                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;

                       
                    }
                  
                    if (line.IndexOf("DJDE ") != -1 && line.IndexOf("'START'") != -1 && !fStart)
                    {
                        prevline = currLine;
                        fsCount = true;
                        isUCDSI = false;
                        inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                    }
                    if (currLine > prevline && line.IndexOf(sysout) != -1 && fsCount)    //psysout  10 20 2015
                    {

                     
                        if ((line.IndexOf("CHECK NO") != -1 || line.IndexOf("SEQUENCE NO") != -1 || line.IndexOf("SEQ NO") != -1)
                            && line.IndexOf("DJDE") == -1 && line.IndexOf("JDE") == -1)
                        {
                            checknumber = searchText2(" NO", line, currLine);
                            scount = line.Substring(3, line.IndexOf(" ", 5) - 3).Trim();
                        }
                        else
                        {
                            if (line.Length > 4 && line.IndexOf("DJDE") == -1 && line.IndexOf("JDE") == -1)
                            {
                                checknumber = "";
                                scount = line.Substring(4, line.Length - 4).TrimStart();
                            }
                            else
                            {
                                scount = "";
                                specialScount = true;
                            }
                        }
                        fsCount = false;
                        prevline = 0;
                        fsAddrs = true;
                    }
                    if (specialScount && line.IndexOf("  1") == 0 && line.Length > 3 )
                    {
                        scount = line.Substring(4, line.Length - 4).TrimStart();
                        specialScount = false;
                    }

                    if ((line.IndexOf(add1) == 0 || line.IndexOf(add2) == 0 || line.IndexOf(add3) == 0) && fsAddrs)
                    {
                        inaddr = line.Substring(0, 3);
                        //addrs.Clear();
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
                    //=====
                    if (fsAddrs)
                    {
                        if (inaddr != "" && countinAddr < 6)
                        {
                            if (inaddr == "111" && line.Substring(0, 3) == "  2")
                            {
                                if (line != "  2")
                                {
                                    countinAddr++;
                                    addrs.Add(line.Substring(3, line.Length - 3));

                                    if (countinAddr == 5)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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

                                    if (countinAddr == 6)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
                                        }
                                    }
                                }
                            }

                            if (inaddr == "111" && line.Substring(0, 3) == "113")
                            {
                                isUCDSI = true;
                                prevline = currLine;
                                inaddr111_3 = false;
                                fsAddrs = false;
                                looking111_3 = true;
                            }
                            //if (inaddr == "111" && line.IndexOf(sysout) != -1)
                            //{
                            //    int posc1 = line.IndexOf(sysout);
                            //    scount = line.Substring(posc1 + sysout.Length, 7);
                            //}
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
                                        }
                                    }
                                }
                            }
                            //====
                        }
                    }
                    if (line.Length > 2)
                    {
                        if (inaddr == "111" && isUCDSI && currLine > prevline && line.Substring(0, 3) == "111" && looking111_3)
                        {
                            while (line.Contains("  ")) line = line.Replace("  ", " ");
                            string[] words = line.Replace("  ", " ").Trim().Split(' ');
                            looking111_3 = false;
                            mailStop = words[1];
                            addrs.Add(mailStop);
                            seqNum = words[2];
                            inaddr111_3 = true;
                            prevline = currLine;
                            countinAddr = 1;
                        }
                        if (line.Substring(0, 3) == "  3" & inaddr111_3)
                        {
                            addrs.Add(line.Substring(3, line.Length - 3));
                            countinAddr++;

                        }
                        if (line.Substring(0, 3) == "112" & inaddr111_3)
                        {
                            addToTable();
                            inaddr111_3 = false;

                        }
                    }
                    //====
                    currLine++;
                    if (currLine == 230)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n" + fileName;
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
                                            fileName, DataTable, "UCDS", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "UCDS", "error count " + errorcount);
            }

            return processCompleted;
        }
        public string evaluate_EOB_TXT(string fileName, string secondValue, string LastWriteTime)
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
            bool fJobid = false;
            bool fsAddrs = false;
            bool specialScount = false;
            

            string sys = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string add1 = "111  ";
            string add2 = "118  ";
            string add3 = "119  ";
            sysout = psysout = jobname = pDate = jobID = aDate = seqNum = scount = mailStop = checknumber = Feed = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            currLine = 0;
            int valueOk = 0;
            string line;
            DataTable.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            if (fileInfo.Name == "UCDSQ001-20150618011004.txt")
                inaddr = "";
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {

                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;

                        //sysout = searchText(sys, line, currLine);
                        //psysout = sysout.Substring(4, 4);
                        //jobname = searchText(jobn, line, currLine);

                        //pDate = searchText(pdat, line, currLine);
                        //aDate = searchText(adat, line, currLine);
                        //prevline = currLine;
                        //fsys = true;
                        //fJobid = true;
                    }
                    //if (currLine - 1 == prevline && fJobid)
                    //{
                    //    jobID = searchText("JOBID:", line, currLine);
                    //    fJobid = false;
                    //}
                    if (line.IndexOf("DJDE ") != -1 && line.IndexOf("'START'") != -1 && !fStart)
                    {
                        prevline = currLine;
                        fsCount = true;
                        inaddr = "";
                        isMEDADV = false;
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                    }
                    if (currLine > prevline && line.IndexOf(psysout) != -1 && fsCount)
                    {

                        //scount = line.Substring(line.IndexOf(psysout) + 4, line.Length - line.IndexOf(psysout)-4);
                         //scount = line.Substring(line.IndexOf(psysout) + 4, 7);
                         if ((line.IndexOf("CHECK NO") != -1 || line.IndexOf("SEQUENCE NO") != -1 || line.IndexOf("SEQ NO") != -1)
                              && line.IndexOf("DJDE") == -1 && line.IndexOf("JDE") == -1)
                        {
                            checknumber = searchText2(" NO", line, currLine);
                            scount = line.Substring(3, line.IndexOf(" ", 5) - 3).Trim();
                        }
                        else
                        {
                            if (line.Length > 4 && line.IndexOf("DJDE") == -1 && line.IndexOf("JDE") == -1)
                            {
                                checknumber = "";
                                scount = line.Substring(4, line.Length - 4).TrimStart();
                            }
                            else
                            {
                                scount = "";
                                specialScount = true;
                            }
                        }
                        fsCount = false;
                        prevline = 0;
                        fsAddrs = true;
                    }
                    if (specialScount && line.IndexOf("  1") == 0 && line.Length > 3)
                    {
                        scount = line.Substring(4, line.Length - 4).TrimStart();
                        specialScount = false;
                    }

                    if ((line.IndexOf(add1) == 0 || line.IndexOf(add2) == 0 || line.IndexOf(add3) == 0) && fsAddrs)
                    {
                        if (line.IndexOf("MED ADV ") != -1)
                            isMEDADV = true;
                        inaddr = line.Substring(0, 3);
                        //addrs.Clear();
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
                    //=====
                    if (fsAddrs)
                    {
                        if (inaddr != "" && countinAddr < 6)
                        {
                            if (inaddr == "111" && line.Substring(0, 3) == "  2")
                            {
                                if (line != "  2")
                                {
                                    countinAddr++;
                                    addrs.Add(line.Substring(3, line.Length - 3));

                                    if (countinAddr == 5)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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

                                    if (countinAddr == 6)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
                                        }
                                    }
                                }
                            }
                            //====
                        }
                    }
                    //====
                    currLine++;
                    if (currLine == 399356)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n" + fileName;
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
                                            fileName, DataTable, "UCDS", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "UCDS", "error count " + errorcount);
            }

            return processCompleted;
        }
        public string evaluate_EOB_A_TXT(string fileName, string secondValue, string LastWriteTime)
        {
            UCDSAR = true;
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
            bool fsAddrs = false;


            string sys = "  CIERANT";
            sysout = psysout = jobname = pDate = jobID = aDate = seqNum = scount = mailStop = checknumber = Feed = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            currLine = 0;
            int valueOk = 0;
            string line;
            DataTable.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            if (fileInfo.Name == "UCDSQ001-20150618011004.txt")
                inaddr = "";
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {

                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;
                    }
                    if (line.IndexOf("DJDE ") != -1 && line.IndexOf("'START'") != -1 && !fStart)
                    {
                        fsAddrs = false;
                        prevline = currLine;
                        fsCount = true;
                        inaddr = "";
                        isMEDADV = false;
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                    }
                    if (currLine > prevline && line.IndexOf(sysout) != -1 && fsCount)
                    {
                        scount =  searchText(sysout, line, currLine);
                        //checknumber = searchText2(sysout, line, currLine);
                            //scount = line.Substring(3, line.IndexOf(" ", 5) - 3).Trim();
                        prevline = currLine;
                        fsCount = false;
                        prevline = 0;
                        fsAddrs = true;
                    }
                    if (currLine > prevline && line.IndexOf("  4") == 0 && fsAddrs)
                    {
                        if(line.Length > 5)
                        {
                            if (line.Length > 70)
                            {
                                countinAddr++;
                                addrs.Add(line.Substring(3, 67).TrimEnd().TrimStart());
                            }
                            else
                            {
                                double n;
                                bool isNumeric = double.TryParse(line.Substring(3, line.Length - 3).TrimEnd().TrimStart().Replace(" ", ""), out n);
                                if (isNumeric)
                                { }
                                else
                                {
                                    countinAddr++;
                                    addrs.Add(line.Substring(3, line.Length - 3).TrimEnd().TrimStart());
                                }
                            }
                        }
                        prevline = currLine;
                    }
                    if (currLine > prevline && line.IndexOf("006") == 0 && fsAddrs)
                    {
                        //close addrs
                        fsAddrs = false;
                        prevline = currLine;

                        while (countinAddr < 7)
                        {
                            addrs.Add("");
                            countinAddr++;
                        }
                        addToTable();
                        countinAddr = 0;
                    }


                    //====
                    currLine++;
                    if (currLine == 52306)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n" + fileName;
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
                                            fileName, DataTable, "UCDS", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "UCDS", "error count " + errorcount);
            }
            UCDSAR = false;
            return processCompleted;
        }
       
        public string evaluate_EOB_O_TXT(string fileName, string secondValue, string LastWriteTime)
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
            bool fJobid = false;
            bool fsAddrs = false;
            bool specialScount = false;

            string sys = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string add1 = "111  ";
            string add2 = "118  ";
            string add3 = "119  ";
            
            sysout = psysout = jobname = pDate = jobID = aDate = seqNum = scount = mailStop = checknumber = Feed = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            string FileType = fileInfo.Name.Substring(0, 5);
            currLine = 0;
            int valueOk = 0;
            string line;
            DataTable.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            if (fileInfo.Name == "UCDSQ001-20150618011004.txt")
                inaddr = "";
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {

                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;

                     
                    }
                   
                    if (line.IndexOf("DJDE ") != -1 && line.IndexOf("'START'") != -1 && !fStart)
                    {
                        prevline = currLine;
                        fsCount = true;
                        inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                    }
                    if (currLine > prevline && line.IndexOf(FileType) != -1  && fsCount && line.Substring(0,3) == "111")
                    {
                       
                            scount = line.Substring(3, 24).Trim();
                       
                        fsCount = false;
                        prevline = 0;
                        fsAddrs = true;
                        isMEDADV = false;
                    }
                    if (specialScount && line.IndexOf("  1") == 0 && line.Length > 3)
                    {
                        scount = line.Substring(4, line.Length - 4).TrimStart();
                        specialScount = false;
                    }

                    if ((line.IndexOf(add1) == 0 || line.IndexOf(add2) == 0 || line.IndexOf(add3) == 0) && fsAddrs)
                    {
                        if (line.IndexOf("MED ADV ") != -1)
                            isMEDADV = true;
                        inaddr = line.Substring(0, 3);
                        //addrs.Clear();
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
                    //=====
                    if (fsAddrs)
                    {
                        if (inaddr != "" && countinAddr < 6)
                        {
                            if (inaddr == "111" && line.Substring(0, 3) == "  2")
                            {
                                if (line != "  2")
                                {
                                    countinAddr++;
                                    addrs.Add(line.Substring(3, line.Length - 3));

                                    if (countinAddr == 5)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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

                                    if (countinAddr == 6)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
                                        }
                                    }
                                }
                            }
                            //====
                        }
                    }
                    //====
                    currLine++;
                    if (currLine == 229)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n" + fileName;
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
                                            fileName, DataTable, "UCDS", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "UCDS", "error count " + errorcount);
            }

            return processCompleted;
        }
        public string evaluate_EOB_C_TXT(string fileName, string secondValue, string LastWriteTime)
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
            bool fJobid = false;
            bool fsAddrs = false;
            bool specialScount = false;

            string sys = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string add1 = "111  ";
            string add2 = "118  ";
            string add3 = "119  ";

            sysout = psysout = jobname = pDate = jobID = aDate = seqNum = scount = mailStop = checknumber = Feed = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            string FileType = fileInfo.Name.Substring(0, 5);
            currLine = 0;
            int valueOk = 0;
            string line;
            DataTable.Clear();
            //string Nfilename = GlobalVar.directoryConverted + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4).ToUpper().Replace("-NASH", "") + "_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            if (fileInfo.Name == "UCDSQ001-20150618011004.txt")
                inaddr = "";
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(sys) != -1 && !fsys)
                    {

                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');

                        sysout = words[1];
                        jobname = words[2];
                        jobID = words[3];
                        pDate = words[6];
                        fsys = true;
                        prevline = currLine;


                    }

                    if (line.IndexOf("DJDE ") != -1 && line.IndexOf("'START'") != -1 && !fStart)
                    {
                        prevline = currLine;
                        fsCount = true;
                        inaddr = "";
                        Feed = line.Substring(line.IndexOf("FEED", 1) + 5, 4);
                    }
                    if (currLine > prevline && line.IndexOf(FileType) != -1 && fsCount && 
                            (line.Substring(0, 3) == "111" || fsCount && line.Substring(0, 3) == "118"))
                    {

                        scount = line.Substring(3, 24).Trim();
                        prevline = currLine;
                        fsCount = false;
                        //prevline = 0;
                        fsAddrs = true;
                    }


                    if (currLine > prevline && (line.IndexOf(add1) == 0 || line.IndexOf(add2) == 0 || line.IndexOf(add3) == 0) && fsAddrs)
                    {
                        inaddr = line.Substring(0, 3);
                        //addrs.Clear();
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
                    //=====
                    if (fsAddrs)
                    {
                        if (inaddr != "" && countinAddr < 6)
                        {
                            if (inaddr == "111" && line.Substring(0, 3) == "  2")
                            {
                                if (line != "  2")
                                {
                                    countinAddr++;
                                    addrs.Add(line.Substring(3, line.Length - 3));

                                    if (countinAddr == 5)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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

                                    if (countinAddr == 6)
                                    {
                                        if (addrs.Count > 2)
                                        {
                                            addToTable();
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
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
                                            fsAddrs = false;
                                        }
                                    }
                                }
                            }
                            //====
                        }
                    }
                    //====
                    currLine++;
                    if (currLine == 229)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    processCompleted = processCompleted + ex.Message + "\n\n" + fileName;
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
                                            fileName, DataTable, "UCDS", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "UCDS", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "UCDS", "error count " + errorcount);
            }

            return processCompleted;
        }
        public void addToTable()
        {
            var row = DataTable.NewRow();
            row["Recnum"] = Recnum;
            row["Sysout"] = sysout;
            row["Sheet_count"] = scount;
            row["Jobname"] = jobname;
            row["PrintDate"] = pDate;
            row["ArchiveDate"] = aDate;
            row["C_Recnum"] = C_Recnum;
            row["Seq"] =  seqNum; //scount;
            row["JOBID"] = jobID;
            if (Type == "Y")
                row["DE_Flag"] = "E";
            //row["mailStop"] = mailStop.Trim();
            row["Field2"] = docDate;
            row["Field3"] = currLine;
            row["Field4"] = Feed;
            row["Addr1"] = addrs[0];
            row["Addr2"] = addrs[1];
            row["Addr3"] = addrs[2];
            row["Addr4"] = addrs[3];
            if (UCDSAR)
            {
                row["Addr5"] = addrs[4];
                row["Addr6"] = addrs[5];
            }
            if (isMEDADV)
            row["MED_Flag"] = "Y";
            else
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
