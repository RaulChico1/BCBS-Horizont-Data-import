using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class NParse_IM
    {
        DataTable DataTable = Data_Table();
        List<string> addrs = new List<string>();
        int Recnum = 1;
        int C_Recnum = 1;
        int countinAddr = 0;
        string inaddr = "";
        string FileClass = "";
        string sysout, psysout, jobname, jobID, pDate, aDate, seqNum, scount, mailStop, LetterDate, checknumber, Feed, docDate, LetterProduced;
        string errors = "";
        int errorcount = 0;
        int currLine = 0;
        DBUtility dbU;
        public string evaluate_TXT(string fileName, string secondValue, string LastWriteTime)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            C_Recnum = 1;
            Recnum = 1;

            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");

            if (recnum.ToString() == "")
                Recnum = 1;
            else
                Recnum = Convert.ToInt32(recnum.ToString()) + 1;


            countinAddr = 1;
            inaddr = "";
            int prevline = 0;
            string prevValLine = "";

            bool fsys = false;
            bool inaddrBlock = false;


            string sys = "  CIERANT";
            //string final = "TOTAL PAGES IN REPORT   -";
            string final = "TOTAL NUMBER OF LETTERS PRINTED:";

            sysout = psysout = jobname = pDate = aDate = seqNum = scount = mailStop = LetterDate = LetterProduced = string.Empty;

            string processCompleted = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int currLine = 0;
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
                    if (line.IndexOf(" 2") == 0 && fsys)
                    {
                        scount = line.Substring(2, line.Length - 2);
                        prevline = currLine;
                        countinAddr = 0;
                    }

                    if (line.IndexOf(" 4") == 0 && fsys)
                    {

                        inaddrBlock = true;

                        countinAddr++;
                        addrs.Add(line.Substring(2, line.Length - 2));


                    }

                    if (line.IndexOf(" 1") == 0 && inaddrBlock)
                    {
                        //end of addr block
                        if (countinAddr < 6)
                        {
                            while (addrs.Count < 6)
                            {
                                addrs.Add("");
                            }

                            if (addrs.Count > 2)
                            {
                                addToTable();
                            }
                        }
                        inaddrBlock = false;
                    }


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
                    if (line.IndexOf(final) > 0 && fsys)
                    {
                        int poscHad = line.IndexOf(":");
                        LetterProduced = line.Substring(poscHad + 1, line.Length - poscHad - 1);
                    }
                    currLine++;
                    if (fsys)
                        prevValLine = line;

                    if (currLine == 1221)
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
                    if (DataTable.Rows.Count > 0)
                    {
                        string resultcsv = create_cas__csv.create_Default_CAS_CSV(
                                            fileName, DataTable, "IM", Recnum, DataTable.Rows.Count.ToString(), sysout, jobID, LastWriteTime);
                        if (resultcsv != "")
                            processCompleted = resultcsv + "\n\n";
                    }
                    else
                    {
                        processCompleted = "No recods in file " + fileName;
                        //save with zero
                        create_cas__csv.update_w_errors_zero(fileName, "IM", "No recods in file");
                    }

                }
                else
                {
                    processCompleted = "No SYSOUT ID file " + fileName;
                    errorcount++;
                    //save with error
                    create_cas__csv.update_w_errors_zero(fileName, "IM", "No SYSOUT ID in file");
                }
            }
            else
            {
                processCompleted = processCompleted + " errors " + errorcount;
                //save with error
                create_cas__csv.update_w_errors_zero(fileName, "IM", "error count " + errorcount);
            }

            return processCompleted;
        }

        public void addToTable()
        {
            var row = DataTable.NewRow();
            row["Recnum"] = Recnum;
            row["Sysout"] = sysout;
            row["Jobname"] = jobname;
            row["PrintDate"] = pDate;
            row["ArchiveDate"] = aDate;
            row["C_Recnum"] = C_Recnum;
            row["Seq"] = scount;
            row["JOBID"] = jobID;
            row["mailStop"] = mailStop.Trim();
            row["Field2"] = LetterDate;
            row["Field3"] = currLine;
            row["Field4"] = Feed;
            row["Addr1"] = addrs[0];
            row["Addr2"] = addrs[1];
            row["Addr3"] = addrs[2];
            row["Addr4"] = addrs[3];
            row["Addr5"] = addrs[4];
            row["Addr6"] = addrs[5];
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
