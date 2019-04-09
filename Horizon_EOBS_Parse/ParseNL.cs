using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using System.Xml;
using System.Xml.Linq;


namespace Horizon_EOBS_Parse
{
    public class ParseNL
    {

        AutoResetEvent autoEvent = new AutoResetEvent(false);
        DataTable EOBs = EOBs_Table();
        DataTable summary = summary_Table();
        List<string> addrs = new List<string>();
        int Recnum = 1;
        int countinAddr = 0;
        string inaddr = "";
        string sysout, psysout, jobname, pDate, aDate, seqNum, scount, mailStop, LetterDate;
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

            if (Directory.Exists(ProcessVars.NLDirectory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(ProcessVars.NLDirectory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("*.txt");
                foreach (FileInfo file in FilesPDF)
                {
                    if (file.Name.IndexOf("IM") == 0)
                    {
                        try
                        {
                            string error = evaluate_RejectedLetters(file.FullName);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.IndexOf("EP") == 0)
                    {
                        try
                        {
                            string error = evaluate_EP(file.FullName);
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
        public string evaluate_EP(string fileName)
        {
            Recnum = 1;
            countinAddr = 1;
            inaddr = "";

            int prevline = 0;
            string prevValLine = "";

            bool fsys = false;
            bool inaddrBlock = false;
            bool LookingEndAddrBlock = false;

            string sys = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string add1 = "                  000";

            string final = "LETTERS PRODUCED";
            sysout = psysout = jobname = pDate = aDate = seqNum = scount = mailStop = LetterDate =string.Empty;

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
                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');    //Previous Balance

                        sysout = words[1];
                        jobname = words[3];
                        pDate = words[6];
                        fsys = true;
                        inaddrBlock = false;
                    }
                    if (line.IndexOf(add1) > 0 && fsys)
                    {
                        //prev line must date
                        while (prevValLine.Contains("  ")) prevValLine = prevValLine.Replace("  ", " ");
                        LetterDate = prevValLine;
                        inaddrBlock = true;

                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        seqNum = line;
                        prevline = currLine;
                    }
                    if (line.IndexOf("+") == 0 && inaddrBlock && !LookingEndAddrBlock)
                    {
                        prevline = currLine;

                    }
                    if (currLine > prevline && inaddrBlock && line.IndexOf("+") != 0)
                    {
                        if (line.Length > 1)
                        {
                            while (line.Contains("  ")) line = line.Replace("  ", " ");
                            countinAddr++;
                            addrs.Add(line);
                            LookingEndAddrBlock = true;
                        }
                        
                    }
                    if (currLine > prevline && inaddrBlock && line.IndexOf("+") == 0)
                    {
                        //end of addr block
                        if (countinAddr < 6)
                        {
                            while (addrs.Count < 6)
                            {
                                addrs.Add("");
                            }

                            //if (addrs.Count > 2)
                            //{
                            //    addToTable();
                            //}
                        }
                        inaddrBlock = false;
                        LookingEndAddrBlock = false;
                        addToTable();
                    }
                    if (line.IndexOf(final) > 0 && fsys)
                    {
                        int poscHad = line.IndexOf("HAD");
                        string LetterProduced = line.Substring(poscHad + 4, 9);
                        //DataRow dr = EOBs.Select("C_Recnum=" + (Recnum - 1)).FirstOrDefault(); 
                        //dr["Sheet_count"] = LetterProduced;
                        int lastRecnum = Recnum - 1;
                        foreach (DataRow dr in EOBs.Rows) 
                        {
                            if (dr["C_Recnum"].ToString() == lastRecnum.ToString()) 
                            {
                                dr["Sheet_count"] = LetterProduced;
                            }
                        }

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
            //wr.Flush();
            //wr.Close();
            if (sysout != "")
            {
                file.Close();

                foreach (DataRow row in EOBs.Rows)
                {
                    for (int ii = 19; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[19] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }

                createCSV createcsv = new createCSV();
                string pName = ProcessVars.oNLDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < EOBs.Columns.Count; index++)
                {
                    fieldnames.Add(EOBs.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in EOBs.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < EOBs.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }


                //sumarize(EOBs, fileInfo.Name.Substring(0, fileInfo.Name.Length - 4));

                //CreateXML.ToXml(summary);

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
        public string evaluate_RejectedLetters(string fileName)
        {
            Recnum = 1;
            countinAddr = 1;
            inaddr = "";

            int prevline = 0;

            bool fsys = false;
            bool inaddrBlock = false;
            bool fStart = false;

            string sys = "  CIERANT";
            string jobn = "JOBNAME: ";
            string pdat = "PRINT DATE: ";
            string adat = "ARCHIVE DATE: ";
            string add1 = "111    ";
            string add2 = "118    ";
            string add3 = "119    ";
            sysout = psysout = jobname = pDate = aDate = seqNum = scount = mailStop = LetterDate = string.Empty;

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
                        while (line.Contains("  ")) line = line.Replace("  ", " ");
                        string[] words = line.Replace("  ", " ").Trim().Split(' ');    //Previous Balance

                        sysout = words[1];
                        jobname = words[3];
                        pDate = words[6];
                        fsys = true;
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
                    for (int ii = 19; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[19] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }

                createCSV createcsv = new createCSV();
                string pName = ProcessVars.oNLDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < EOBs.Columns.Count; index++)
                {
                    fieldnames.Add(EOBs.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in EOBs.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < EOBs.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }


                sumarize(EOBs, fileInfo.Name.Substring(0, fileInfo.Name.Length - 4));

               //CreateXML.ToXml(summary);

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
            row["Sysout"] = sysout;
            row["Sheet_count"] = scount;
            row["Jobname"] = jobname;
            row["PrintDate"] = pDate;
            row["ArchiveDate"] = aDate;
            row["C_Recnum"] = Recnum;
            row["Seq"] = seqNum;
            row["mailStop"] = mailStop.Trim();
            row["LetterDate"] = LetterDate;
            row["Addr1"] = addrs[0];
            row["Addr2"] = addrs[1];
            row["Addr3"] = addrs[2];
            row["Addr4"] = addrs[3];
            row["Addr5"] = addrs[4];
            row["Addr6"] = addrs[5];
            EOBs.Rows.Add(row);
            addrs.Clear();
            countinAddr = 0;
            Recnum++;
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
            newt.Columns.Add("Sysout");
            newt.Columns.Add("Sheet_count");
            newt.Columns.Add("Jobname");
            newt.Columns.Add("PrintDate");
            newt.Columns.Add("ArchiveDate");
            newt.Columns.Add("C_Recnum");
            newt.Columns.Add("Seq");
            newt.Columns.Add("mailStop");
            newt.Columns.Add("LetterDate");
            newt.Columns.Add("Field1");
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
            //newt.Columns.Add("On-Hand", typeof(Double));
            return newt;
        }
        private static DataTable summary_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Sysout");
            newt.Columns.Add("Jobname");
            newt.Columns.Add("PrintDate");
            newt.Columns.Add("ArchiveDate");
            newt.Columns.Add("RecordsCount");
            newt.Columns.Add("Sheet_count");
            newt.Columns.Add("BadAddrs");
            //newt.Columns.Add("On-Hand", typeof(Double));
            return newt;
        }
        public void sumarize(DataTable table, string fname)
        {
          

            var _result = table
                    .AsEnumerable()
                    .GroupBy(r1 => new
                    {
                        Sysout = r1.Field<string>("Sysout"),
                        Jobname = r1.Field<string>("Jobname"),
                        PrintDate = r1.Field<string>("PrintDate"),
                        ArchiveDate = r1.Field<string>("ArchiveDate")
                    }).Select(g => new
                    {
                        Sysout = g.Key.Sysout,
                        Jobname = g.Key.Jobname,
                        PrintDate = g.Key.PrintDate,
                        ArchiveDate = g.Key.ArchiveDate,
                        RecordsCount = g.Count(),
                        Sheet_count = "",
                        BadAddrs = ""
                    });



            var row = summary.NewRow();
            foreach (var element in _result)
            {
                string ok = "";
                row["Sysout"] = element.Sysout;
                row["Jobname"] = element.Jobname;
                row["PrintDate"] = element.PrintDate;
                row["ArchiveDate"] = element.ArchiveDate;
                row["RecordsCount"] = element.RecordsCount;
                //row["Sheet_count"] = seqNum;
                //row["BadAddrs"] = mailStop.Trim();
            }
            summary.Rows.Add(row);

            summary.TableName = "Summary";
            string xmlName = ProcessVars.oNLDirectory + fname + ".xml";
            //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            if (File.Exists(xmlName))
                File.Delete(xmlName);



            summary.WriteXml(xmlName, true);
           


            }
    }
}

