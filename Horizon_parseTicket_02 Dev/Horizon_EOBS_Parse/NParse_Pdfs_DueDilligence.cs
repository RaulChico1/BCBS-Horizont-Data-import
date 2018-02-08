﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;

namespace Horizon_EOBS_Parse
{
    public class NParse_Pdfs_DueDilligence
    {
        DataTable NLPdfs = pdfs_Table();
        List<string> addrs = new List<string>();
        int Recnum = 1;
        int initialRecnum = 0;
        int C_Recnum = 1;
        int page_addrs = 1;
        int totP = 0;
        string errors = "";
        int errorcount = 0;
        //int rowCount = 0;
        string errorMSG = "";
        string dateHLGS = "";
        DBUtility dbU;

        public string ProcessFiles(string dateProcess)
        {
            ProcessVars.serviceIsrunning = true;
            //autoEvent.WaitOne(1000 * 60 * 3, false);
            string result = zipFilesinDir(dateProcess);
            ProcessVars.serviceIsrunning = false;

            return "DueDilligence Done at" + DateTime.Now.ToString("yyyy_MM_dd   HH_mm"); ;
        }
        public string zipFilesinDir(string dateProcess)
        {
        string dirname = @"\\Cierant-taper\clients\Horizon BCBS\DUE DILIGENCE LETTERS 20180130";
        if (Directory.Exists(dirname))
            {
            DirectoryInfo originalPDFs = new DirectoryInfo(dirname);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("*.pdf");
                if (FilesPDF.Count() > 0)
                {
                    GlobalVar.dbaseName = "BCBS_Horizon";
                    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                    C_Recnum = 1;
                    Recnum = 1;
                    string test = "";

                    var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
                    if (recnum.ToString() == "")
                        Recnum = 1;
                    else
                        Recnum = Convert.ToInt32(recnum.ToString()) + 1;

                    initialRecnum = Recnum;


                    foreach (FileInfo file in FilesPDF)
                    {
                        if (file.Name.IndexOf("DISPATCH") == -1)
                        {
                            try
                            {
                                string error = evaluate_pdf_ver2(file.FullName, "");
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
                if (NLPdfs.Rows.Count > 0)
                    finalprocess(originalPDFs.FullName, "");   //dateHLGS
            }
            return errors;
        }
        public string zipFilesinDirService(string dateProcess, string direcTory)
        {

            if (Directory.Exists(direcTory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(direcTory);
                //DirectoryInfo gparent = new DirectoryInfo(Directory.GetParent((direcTory).ToString()).ToString());
                FileInfo[] filesZ = originalPDFs.GetFiles("*.zip");
                //if (direcTory.IndexOf(filesZ[0].ToString().Replace(".zip", "")) != -1)
                //{
                //    GlobalVar.dbaseName = "BCBS_Horizon";
                //    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                //    var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + filesZ[0].ToString() + "'");
                //    dateHLGS = fileDate.ToString();
                //}
                FileInfo[] FilesPDF = originalPDFs.GetFiles("*.pdf", SearchOption.AllDirectories);
                if (FilesPDF.Count() > 0)
                {
                    GlobalVar.dbaseName = "BCBS_Horizon";
                    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                    C_Recnum = 1;
                    Recnum = 1;
                    string test = "";

                    var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
                    int recordnumber = 0;
                    if (recnum.ToString() == "")
                        Recnum = 1;
                    else
                        Recnum = Convert.ToInt32(recnum.ToString()) + 1;

                    initialRecnum = Recnum;


                    foreach (FileInfo file in FilesPDF)
                    {
                        if (file.Name.IndexOf("Summary") == -1)
                        {
                            try
                            {
                                string error = evaluate_pdf(file.FullName, "");
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
                if (NLPdfs.Rows.Count > 0)
                    finalprocess(direcTory, dateHLGS);
            }
            return errors;
        }
        public string evaluate_pdf(string fileName, string dest)
        {
            int ppCount = 0;
            errorMSG = "";
            bool doc_NO_addr = false;
            int LineStart = 5;

            bool addrFound = false;

            bool found_RE_Dear = false;

            bool isNotification = false;

            int index_date = 0;
            int index_re = 0;



            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            if (fileInfo.Name == "Advance PCPChange_0.25132698475280624.PDF" ||
                 fileInfo.Name == ".PDF")
                errorMSG = "";
            //====================
            string strText = string.Empty;
            try
            {
                PdfReader reader = new PdfReader(fileName);
                totP = reader.NumberOfPages;
                for (int page = 1; page <= reader.NumberOfPages; page++)
                {
                    ITextExtractionStrategy its = new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy();
                    string s = PdfTextExtractor.GetTextFromPage(reader, page, its);

                    s = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(s)));
                    ppCount++;
                    if (ppCount == 33)
                        ppCount = ppCount;
                    string[] words = s.Split('\n');
                    //Text.Append(currentText);
                    int n;
                    if (page == 1)
                    {
                        for (int i = 0; i < words.Length; i++)
                        {

                            string[] importString = new string[] { "RE: UNCLAIMED" };
                            foreach (string sS in importString)
                            {
                                switch (words[i].ToUpper().Contains(sS))
                                {
                                    case true:
                                        if (sS == "THIS NOTIFICATION WAS ISSUED")
                                            isNotification = true;
                                        else
                                            isNotification = false;
                                        index_re = i;
                                        break;
                                    default:
                                        //transform.gameObject.AddComponent("Backup_ValveMove");
                                        break;
                                }
                            }

                        }
                        for (int i = 0; i < words.Length; i++)
                        {
                            if (isNotification)
                            {
                                if (words[i].Contains("Page"))
                                {
                                    index_date = i;
                                    break;
                                }
                            }
                            else
                            {
                                if (words[i].Contains(", 2018") || words[i].Contains(", 2016") || words[i].Contains(", 2017"))
                                {
                                    index_date = i;
                                    break;
                                }
                                else
                                {
                                    if (words[i].Contains(", 2018") || words[i].Contains(", 2016") || words[i].Contains(", 2017"))
                                    {
                                        index_date = i;
                                        break;
                                    }
                                    else
                                    {
                                        if (words[i].Contains(" 2018") || words[i].Contains(" 2016") || words[i].Contains(" 2017"))
                                        {
                                            index_date = i;
                                            break;
                                        }

                                    }

                                }

                            }


                        }
                    }
                    if (addrFound)
                    {
                        if (index_re > words.Count())
                        {
                            page_addrs++;
                        }
                        else
                        {
                            try
                            {
                                if (words[index_re].ToUpper().Contains("RE: UNCLAIMED") )
                                {
                                    //other addrs
                                    NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = page_addrs;
                                    page_addrs = 1;
                                    addrFound = false;

                                    addrFound = true;
                                    for (int ii = index_date; ii < index_re; ii++)
                                    {
                                        string wtext = "";
                                        if (words[ii].Contains("Return this"))
                                            wtext = words[ii].ToString().Substring(0, words[ii].ToString().IndexOf("Return this"));
                                        else
                                            wtext = words[ii].ToString();
                                        addrs.Add(wtext);
                                    }
                                    if (addrs.Count < 9)
                                    {
                                        while (addrs.Count < 9)
                                        {
                                            addrs.Add("");
                                        }
                                    }

                                    addToTable(1, fileInfo.Name);
                                    //rowCount++;


                                }
                                else if (words[index_re + 1].ToUpper().Contains("RE: UNCLAIMED"))
                                {
                                    //other addrs
                                    NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = page_addrs;
                                    page_addrs = 1;
                                    addrFound = false;

                                    addrFound = true;
                                    for (int ii = index_date; ii < index_re + 1; ii++)
                                    {
                                        string wtext = "";
                                        if (words[ii].Contains("Return this"))
                                            wtext = words[ii].ToString().Substring(0, words[ii].ToString().IndexOf("Return this"));
                                        else
                                            wtext = words[ii].ToString();
                                        addrs.Add(wtext);
                                    }
                                    if (addrs.Count < 9)
                                    {
                                        while (addrs.Count < 9)
                                        {
                                            addrs.Add("");
                                        }
                                    }

                                    addToTable(1, fileInfo.Name);
                                    //rowCount++;


                                }
                                else if (words[index_re + 2].ToUpper().Contains("RE: UNCLAIMED"))
                                {
                                    //other addrs
                                    NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = page_addrs;
                                    page_addrs = 1;
                                    addrFound = false;

                                    addrFound = true;
                                    for (int ii = index_date; ii < index_re + 2; ii++)
                                    {
                                        string wtext = "";
                                        if (words[ii].Contains("Return this"))
                                            wtext = words[ii].ToString().Substring(0, words[ii].ToString().IndexOf("Return this"));
                                        else
                                            wtext = words[ii].ToString();
                                        addrs.Add(wtext);
                                    }
                                    if (addrs.Count < 9)
                                    {
                                        while (addrs.Count < 9)
                                        {
                                            addrs.Add("");
                                        }
                                    }

                                    addToTable(1, fileInfo.Name);
                                    //rowCount++;


                                }
                                else
                                    page_addrs++;
                            }
                            catch (Exception outIndex)
                            {
                                page_addrs++;
                            }
                        }
                    }
                    else
                    {
                        if (index_re == 0)
                        {
                            // out file name with no addrs info
                            //NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = 0;
                            errorMSG = "No addrs in file detected";
                            while (addrs.Count < 9)
                            {
                                addrs.Add("");
                            }

                            addToTable(1, fileInfo.Name);
                            //rowCount++;
                        }
                    }
                    if (page == 1)
                    {
                        if (index_re == 0)
                            doc_NO_addr = true;
                        else
                        {
                            doc_NO_addr = false;
                            index_date++;
                            //index_re--;
                        }
                        if (!doc_NO_addr)
                        {
                            addrFound = true;
                            for (int ii = index_date; ii < index_re; ii++)
                            {
                                string wtext = "";
                                if (words[ii].Contains("Return this"))
                                    wtext = words[ii].ToString().Substring(0, words[ii].ToString().IndexOf("Return this"));
                                else
                                    wtext = words[ii].ToString();
                                addrs.Add(wtext);
                                if (addrs.Count == 9)
                                    break;
                            }
                            if (addrs.Count < 9)
                            {
                                while (addrs.Count < 9)
                                {
                                    addrs.Add("");
                                }
                            }

                            addToTable(1, fileInfo.Name);
                            //rowCount++;
                        }
                    }


                }
                if (page_addrs > 1)
                {
                    NLPdfs.Rows[NLPdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
                    page_addrs = 1;
                }

                reader.Close();



            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = ex.Message;
                addToTable(1, fileInfo.Name);

                //MessageBox.Show(ex.Message);
            }
            return "";
        }
        public string evaluate_pdf_ver2(string fileName, string dest)
            {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            string errorMSG = "";
            string m_IDNumber = "";
            page_addrs = 0;
            int Tpage_addrs = 0;
            int index_re = 0;
            string strText = string.Empty;
            try
                {
                string[] fInfo = fileInfo.Name.Split('_');
                //m_JulianDate = fInfo[1].ToString();
                //m_BatchID = fInfo[2].ToString().ToUpper().Replace(".PDF", "");
                //m_importDate = fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss");
                PdfReader reader = new PdfReader(fileName);
                totP = reader.NumberOfPages;
                for (int page = 1; page <= reader.NumberOfPages; page++)
                    {
                    ITextExtractionStrategy its = new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy();
                    string s = PdfTextExtractor.GetTextFromPage(reader, page, its);

                    s = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(s)));
                    page_addrs++;
                    Tpage_addrs++;
                    string[] words = s.Split('\n');
                    //Text.Append(currentText);
                    int n;
                    if (words[0].ToString().IndexOf("$$METADATA$$") != -1)
                        {
                        //m_IDNumber = "";
                        //if (MBApdfs.Rows.Count > 0)
                        //{
                        //    if (page_addrs > 1)
                        //    {
                        //        MBApdfs.Rows[MBApdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
                        //        page_addrs = 1;
                        //    }
                        //}
                        //page_addrs = 1;

                        //m_transID = m_Insert = m_TOD = m_csz = m_metadata = string.Empty;

                        //string[] metaData = words[0].ToString().Split('|');
                        //m_transID = metaData[1].ToString();
                        ////m_Insert = metaData[5].ToString(); // (3, 4 5)  ~
                        //if (metaData[3].ToString().Length > 0)
                        //    m_Insert = metaData[3].ToString();
                        //if (metaData[4].ToString().Length > 0)
                        //{
                        //    if (m_Insert.Length > 0)
                        //        m_Insert = m_Insert + "~" + metaData[4].ToString();
                        //    else
                        //        m_Insert = metaData[4].ToString();
                        //}
                        //if (metaData[5].ToString().Length > 0)
                        //{
                        //    if (m_Insert.Length > 0)
                        //        m_Insert = m_Insert + "~" + metaData[5].ToString();
                        //    else
                        //        m_Insert = metaData[5].ToString();
                        //}

                        }
                    //page_addrs = 1;
                    bool boolYearFiled = false;
                    bool endAddr = false;
                    string[] limitString = new string[] { "RE:", "DEAR ", "IMPORTANT:" };   //  , "RE:" 
                   string  m_TOD = page.ToString();
                   string m_metadata = ""; // words[0].ToString();
                    index_re = 0;
                    for (int i = 1; i < words.Length; i++)
                        {
                        string tmp = words[i].ToString().ToUpper();
                        if (tmp.ToLower() == "nd" || tmp.ToLower() == "1st" || tmp.ToLower() == "th")
                            tmp = "";
                        if (m_IDNumber == "")
                            {
                            //if (tmp.Contains("3HZ"))
                            //{
                            //    int posc1 = tmp.IndexOf("3HZ");
                            //    m_IDNumber = tmp.Substring(posc1, tmp.Length - tmp.IndexOf("3HZ"));
                            //    tmp = "";
                            //}
                            }

                        if (!endAddr)
                            {

                            if ((tmp.Contains("/2017") || tmp.Contains("/2018") ||
                                tmp.Contains(", 2017") || tmp.Contains(", 2018") ||
                                tmp.Contains(",2017") || tmp.Contains(",2018")) && !boolYearFiled)
                                {
                                if (i < 15 && tmp.ToUpper().IndexOf("COVERAGE") == -1 && tmp.ToUpper().IndexOf(".COM") == -1)
                                    {
                                    boolYearFiled = true;

                                    addrs.Clear();
                                    if (page_addrs > 1)
                                        NLPdfs.Rows[NLPdfs.Rows.Count - 1]["TotalP"] = page_addrs;

                                    page_addrs = 1;
                                    }
                                }
                            else
                                {
                                if (boolYearFiled)
                                    {
                                    bool b = limitString.Any(tmp.Contains);

                                    if (b)
                                        {
                                        if (tmp.Contains("3HZ"))
                                            {
                                            int posc1 = tmp.IndexOf("3HZ");
                                            m_IDNumber = tmp.Substring(posc1, 12);
                                            tmp = ""; //tmp.Replace(m_IDNumber, "");
                                            }

                                        endAddr = true;
                                        while (addrs.Count < 8)
                                            {
                                            addrs.Add("");
                                            }

                                        addToTable(1, fileInfo.Name);//, "Commercial", subdir, zipname);
                                        boolYearFiled = false;
                                        }
                                    else
                                        {
                                        if (tmp.Contains("3HZ"))
                                            {
                                            int posc1 = tmp.IndexOf("3HZ");
                                            m_IDNumber = tmp.Substring(posc1, 12);
                                            tmp = ""; //tmp.Replace(m_IDNumber, "");
                                            }
                                        else
                                            if (words[i].ToString().TrimStart().TrimEnd().Length > 50)
                                                errorMSG = "";
                                        if (tmp.Length > 1)
                                            {
                                            if (tmp.TrimStart().TrimEnd().Length > 1)
                                                addrs.Add(words[i].ToString().TrimStart().TrimEnd());
                                            }
                                        }

                                    }
                                }
                            }
                        else
                            {
                            if (!fPNO)
                                {
                                if (m_IDNumber == "")
                                    {
                                    if (tmp.Contains("3HZ"))
                                        {
                                        int posc1 = tmp.IndexOf("3HZ");
                                        m_IDNumber = tmp.Substring(posc1, 12);
                                        NLPdfs.Rows[NLPdfs.Rows.Count - 1]["artifactId"] = m_IDNumber;
                                        }
                                    }
                                }
                            }
                        }
                    //}
                    //else

                    }
                NLPdfs.Rows[NLPdfs.Rows.Count - 1]["Pages"] = page_addrs;
                reader.Close();
                }

            catch (Exception ex)
                {
                //errorcount++;
                errorMSG = errorMSG + ex.Message;
                //addToTableSBC(1, fileInfo.Name, "SBC");

                //MessageBox.Show(ex.Message);
                }
            return errorMSG;
          
            }

        public string finalprocess(string direcTory, string dateHLGS)
        {
            dateHLGS = DateTime.Now.ToString("yyyy-MM-dd");
            string processCompleted = "";
            DataView dv = NLPdfs.DefaultView;
            dv.Sort = "FileName";
            DataTable sortedPDFs = dv.ToTable();
            string prevFile = "";
            int totDoc = 0;
            int totFile = 0;
            int backupRowNumber = 0;
            for (int i = 0; i < sortedPDFs.Rows.Count; i++)
            {
                if (prevFile != sortedPDFs.Rows[i][1].ToString())
                {
                    if (prevFile != "")
                    {
                        if (totDoc != totFile)
                            sortedPDFs.Rows[backupRowNumber][13] = "Counts not in balance";

                        prevFile = sortedPDFs.Rows[i][1].ToString();
                        totDoc = Convert.ToInt32(sortedPDFs.Rows[i][3].ToString());
                        totFile = Convert.ToInt32(sortedPDFs.Rows[i][2].ToString());
                        backupRowNumber = i;
                    }
                    else
                    {
                        prevFile = sortedPDFs.Rows[i][1].ToString();
                        totDoc = totDoc + Convert.ToInt32(sortedPDFs.Rows[i][3].ToString());
                        totFile = Convert.ToInt32(sortedPDFs.Rows[i][2].ToString());
                        backupRowNumber = i;
                    }
                }
                else
                {
                    totDoc = totDoc + Convert.ToInt32(sortedPDFs.Rows[i][3].ToString());
                    totFile = Convert.ToInt32(sortedPDFs.Rows[i][2].ToString());
                    backupRowNumber = i;
                }


            }


            //upload to sql
            createCAS_CSV create_cas__csv = new createCAS_CSV();
            sortedPDFs.Columns["Errors"].SetOrdinal(2);
            if (sortedPDFs.Rows.Count > 0)
            {
                string resultcsv = create_cas__csv.create_DueDilligence_CSV(
                                    "", sortedPDFs, "PDFs_DueDilligence", Recnum, "", "", "", dateHLGS);
                if (resultcsv != "")
                    processCompleted = resultcsv + "\n\n";
            }

            //DataTable working_NLPdfs = NLPdfs.Copy();
            sortedPDFs.Columns.Remove("MED_Flag");

            createCSV createcsv = new createCSV();
            //string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";  // +DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //string pNameToCASS = direcTory + "HLGS_Pdfs.csv";
            //string directoryAfterCass = ProcessVars.oNLpdfsDirectory + "FromCASS";
            string pName = direcTory + @"\DueDilligences_Pdfs.csv";

            if (File.Exists(pName))
                File.Delete(pName);
            var fieldnames = new List<string>();
            for (int index = 0; index < sortedPDFs.Columns.Count; index++)
            {
                fieldnames.Add(sortedPDFs.Columns[index].ColumnName);
                //string colname = working_G_BILLS.Columns[index].ColumnName;
                //colnames = colnames + ", [" + colname + "]";
            }
            bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            foreach (DataRow row in sortedPDFs.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < sortedPDFs.Columns.Count; index++)
                {
                    rowData.Add(row[index].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsCSV(pName, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            return processCompleted;
        }
        public class SBTextRenderer : IRenderListener
        {

            private StringBuilder _builder;
            public SBTextRenderer(StringBuilder builder)
            {
                _builder = builder;
            }
            #region IRenderListener Members

            public void BeginTextBlock()
            {
            }

            public void EndTextBlock()
            {
            }

            public void RenderImage(ImageRenderInfo renderInfo)
            {
            }

            public void RenderText(TextRenderInfo renderInfo)
            {
                _builder.Append(renderInfo.GetText());
            }

            #endregion
        }
        public void addToTable(int currline, string fname)
        {
            var row = NLPdfs.NewRow();
            row["Recnum"] = Recnum;
            row["FileName"] = fname;
            row["TotalP"] = totP;
            row["page_addrs"] = page_addrs;
            if (errorMSG != "")
                row["Addr"] = errorMSG;
            else
            {
                row["Addr"] = addrs[0];
                row["Addr0"] = addrs[1];
                row["Addr1"] = addrs[2];
                row["Addr2"] = addrs[3];
                row["Addr3"] = addrs[4];
                row["Addr4"] = addrs[5];
                row["Addr5"] = addrs[6];
                row["Addr6"] = addrs[7];
            }
            //row["JOBID"] = JobID;
            row["MED_Flag"] = "N";
            row["JobClass"] = "HLGS";

            NLPdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            C_Recnum++;
        }
        private static DataTable pdfs_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("FileName");
            newt.Columns.Add("TotalP");
            newt.Columns.Add("page_addrs");
            newt.Columns.Add("Addr");
            newt.Columns.Add("Addr0");
            newt.Columns.Add("Addr1");
            newt.Columns.Add("Addr2");
            newt.Columns.Add("Addr3");
            newt.Columns.Add("Addr4");
            newt.Columns.Add("Addr5");
            newt.Columns.Add("Addr6");
            newt.Columns.Add("MED_Flag");
            newt.Columns.Add("Errors");
            newt.Columns.Add("JobClass");

            //newt.Columns.Add("On-Hand", typeof(Double));
            return newt;
        }
    }
}
