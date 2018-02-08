using System;
using System.Collections;
using Tamir.SharpSsh;
using System.Threading;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using WinSCP;
using System.Threading.Tasks;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Configuration;
using System.Text;


namespace Horizon_EOBS_Parse
{
    public class N_Parse_CommercialCorrespondence
    {
        DBUtility dbU;
        DataTable CommPdfs = data_Table();
        int totP = 0;
        int Recnum = 1;
        int page_addrs  = 0;
        int Tpage_addrs = 0;
        int pagesperAddrs = 0;
        string m_transID, m_Insert, m_TOD, m_csz, m_metadata, m_JulianDate, m_BatchID, m_importDate, m_IDNumber;
        List<string> addrs = new List<string>();
        private static DataTable data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            
            newt.Columns.Add("Sourcename");
            newt.Columns.Add("dir_in_zip");
            newt.Columns.Add("Importdate");
            newt.Columns.Add("Seq");
            newt.Columns.Add("Filename");
            newt.Columns.Add("Pages", typeof(Int32));
            newt.Columns.Add("TOD", typeof(Int32));
            newt.Columns.Add("Recnum", typeof(Int32));
            newt.Columns.Add("artifactId");
            newt.Columns.Add("coverPageAddress1");
            newt.Columns.Add("coverPageAddress2");
            newt.Columns.Add("coverPageAddress3");
            newt.Columns.Add("coverPageAddress4");
            newt.Columns.Add("coverPageAddress5");
            newt.Columns.Add("UpdAddr1");
            newt.Columns.Add("UpdAddr2");
            newt.Columns.Add("UpdAddr3");
            newt.Columns.Add("UpdAddr4");
            newt.Columns.Add("UpdAddr5");
            newt.Columns.Add("City");
            newt.Columns.Add("State");
            newt.Columns.Add("Zip");
            newt.Columns.Add("DL");
            newt.Columns.Add("Metadata");

            return newt;
        }

        public int expand_zips()
        {
        int results = 0;
            string errors = "";
            string DirLocal = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\Commercial_Correspondence\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");

            string tempo = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\from_FTP\tmp2";
            if (Directory.Exists(tempo))
                Directory.Delete(tempo, true);
            Directory.CreateDirectory(tempo);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            //DirectoryInfo CRNJLTRZIPs = new DirectoryInfo(DirLocal);
            string unzipDirName = "";
            string[] files = GetFiles(DirLocal, "Commercial Correspondence*.zip|CommercialCorrespondence*.zip", SearchOption.TopDirectoryOnly);

            //foreach (FileInfo f in CRNJLTRZIPs.GetFiles("CRN*.zip"))
            Recnum = 1;
            DataTable datafromPdfs = data_Table();

            foreach (string fi in files)
            {
                FileInfo f = new FileInfo(fi);
                int totf = 0; int linenum = 0;
                string xmlName = "";

                var fileProcessed = dbU.ExecuteScalar("select distinct sourcename from HOR_parse_Commercial_Correspondence_Detail_pdfs where SourceName ='" + f.Name.Replace("__", "") + "'");
                string fileWasProcessed = "No";
                if (fileProcessed != null)
                    fileWasProcessed = "Yes";
                if (f.Name.IndexOf("__") == -1 && fileWasProcessed == "No")
                {
                    try
                    {
                        //CommPdfs.Clear();
                        using (var archive = ZipFile.OpenRead(f.FullName))
                        {
                            foreach (var s in archive.Entries)
                            {

                                String subdirZip = s.FullName.Substring(0, s.FullName.ToString().Length - s.Name.ToString().Length - 1);
                                string path = Path.Combine(tempo + "\\" + subdirZip, s.Name);

                                if (!Directory.Exists(path))
                                    Directory.CreateDirectory(Path.GetDirectoryName(path));

                                s.ExtractToFile(path);
                               
                                string error = evaluate_Comm_C(path, "", subdirZip, f.Name);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        totf = 0;
                    }

                    results++;
                }
            }

            //int GRecnum = 0;
            string result = "";
            foreach (DataRow row in CommPdfs.Rows)
            {
                for (int ii = 13; ii > 0; ii--)
                {
                    if (row[ii].ToString() != "")
                    {
                        //if (ii < 21)
                        //    erros = "";

                        row[13] = row[ii];
                        row[ii] = "";
                        break;

                    }
                }
            }


            string[] selectedColumns = new[] { "Recnum", "coverPageAddress1","coverPageAddress2","coverPageAddress3","coverPageAddress4","coverPageAddress5" };

            DataTable dt = new DataView(CommPdfs).ToTable(false, selectedColumns);
            dt.Columns.Add("coverPageAddress4A", typeof(string)).SetOrdinal(5);
            dt.Columns.Add("F14", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F13", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F12", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F11", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F10", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F9", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F8", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F7", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F6", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F5", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F4", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F3", typeof(string)).SetOrdinal(1);
            dt.Columns.Add("F2", typeof(string)).SetOrdinal(1);

            string nameBCC = tempo + "\\Parse_Commercial_Correspondence_Detail_pdfs_" + DateTime.Now.ToString("yyyy_MM_dd") + "_toBCC.csv";
            string BCCname = "Parse_Commercial_Correspondence_Detail_pdfs_" + DateTime.Now.ToString("yyyy_MM_dd");
            string bccready = @"\\Cierant-taper\dmps\BCC_JM_PROCESSED_FOLDER_HORIZON-NOTICE-LETTERS\" + BCCname + "_toBCC-OUTPUT.csv";
            string bccreadyNG = @"\\Cierant-taper\dmps\BCC_JM_PROCESSED_FOLDER_HORIZON-NOTICE-LETTERS\" + BCCname + "_toBCC-NON-DELIVERABLE.csv";
            if (dt.Rows.Count > 1)
            {
                //_toBCC.csv


                if (File.Exists(bccready))
                    File.Delete(bccready);
                if (File.Exists(ProcessVars.gDMPs + BCCname + "_toBCC.csv"))
                    File.Delete(ProcessVars.gDMPs + BCCname + "_toBCC.csv");
                if (File.Exists(ProcessVars.gDMPs + BCCname + "_toBCC-NON-DELIVERABLE.csv"))
                    File.Delete(ProcessVars.gDMPs + BCCname + "_toBCC-NON-DELIVERABLE.csv");
                createCSV createdataBCC = new createCSV();
                
                if (File.Exists(nameBCC))
                    File.Delete(nameBCC );
                createdataBCC.printCSV_fullProcess(nameBCC , dt, "", "");

                File.Copy(nameBCC , ProcessVars.gDMPs + BCCname + "_toBCC.csv");
            }
            //string bccready = @"\\CIERANT-TAPER\DMPS\BCC_JM_PROCESSED_FOLDER_HORIZON-ID-AND-NOTICE-W-IMB\" + BCCname + "_toBCC-OUTPUT.csv";
            
            FileInfo infoBCCreadfy = new FileInfo(bccready);

            ReturnFromBCC procReturns = new ReturnFromBCC();

            string res = procReturns.process_HORIZ_ReturnBCC_and_upd_Sql(CommPdfs, infoBCCreadfy, BCCname, "HOR_parse_Commercial_Correspondence_Detail_pdfs", bccreadyNG);

            //DataTable filesProc = dbU.ExecuteDataTable("select distinct sourcename from HOR_parse_Commercial_Correspondence_Detail_pdfs where convert(date,importdate) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
            DataTable filesProc = dbU.ExecuteDataTable("select distinct sourcename from HOR_parse_Commercial_Correspondence_Detail_pdfs where convert(date,importdate) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
            foreach (DataRow row in filesProc.Rows)
            {
                SqlParameter[] sqlParams3;
                sqlParams3 = null;
                sqlParams3 = new SqlParameter[] { new SqlParameter("@Ofilename", row["sourcename"].ToString()),new SqlParameter("@Idate", DateTime.Now.ToString("yyyy-MM-dd")) };
                DataTable toSCI = dbU.ExecuteDataTable("HOR_rpt_Commercial_Correspondence_SCI", sqlParams3);
                string filenameSCI = DirLocal + "\\" + row["sourcename"].ToString().Replace(".zip",".csv");
                createCSV createdata = new createCSV();
                createdata.printCSV_fullProcess(filenameSCI, toSCI, "", "");
            }

            return results;
            //SqlParameter[] sqlParams4;
            //sqlParams4 = null;
            //sqlParams4 = new SqlParameter[] { new SqlParameter("@Ofilename", "aa"), new SqlParameter("@Idate", DateTime.Now.ToString("yyyy-MM-dd")) };
            //DataTable FSAtoSCI = dbU.ExecuteDataTable("HOR_rpt_FSA_SCI", sqlParams4);
            //string filenameFSAtoSCI = DirLocal + "FSA_MERCK.csv";
            //createCSV createdata2 = new createCSV();
            //createdata2.printCSV_fullProcess(filenameFSAtoSCI, FSAtoSCI, "", "");
        }

        public void reParse_FSA()
        {
            string errors = "";
            //string DirLocal = @"\\freenas\Internal_Production\Horizon_Production_Mngmt\SECURE\PROD_OUTBOUND\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            string DirLocal = @"\\FREENAS\Internal_Production\Horizon_Production_Mngmt\SECURE\PROD_WORKING\FSA";
            string tempo = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\from_FTP\tmp2";
            if (Directory.Exists(tempo))
                Directory.Delete(tempo, true);
            Directory.CreateDirectory(tempo);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            //DirectoryInfo CRNJLTRZIPs = new DirectoryInfo(DirLocal);
            string unzipDirName = "";
            string[] files = GetFiles(DirLocal, "*.pdf", SearchOption.TopDirectoryOnly);

            //foreach (FileInfo f in CRNJLTRZIPs.GetFiles("CRN*.zip"))
            Recnum = 1;
            DataTable datafromPdfs = data_Table();

            foreach (string fi in files)
            {
                FileInfo f = new FileInfo(fi);
                int totf = 0; int linenum = 0;
                string xmlName = "";

                var fileProcessed = dbU.ExecuteScalar("select distinct sourcename from HOR_ReParse_FSAControl_pdfs where SourceName ='" + f.Name.Replace("__", "") + "'");
                string fileWasProcessed = "No";
                if (fileProcessed != null)
                    fileWasProcessed = "Yes";
                if (f.Name.IndexOf("__") == -1 && fileWasProcessed == "No")
                {
                    try
                    {
                        DirectoryInfo parentDir = Directory.GetParent(Path.GetDirectoryName(f.FullName));
                        string parent = parentDir.FullName;
                        string lastDir = (f.DirectoryName.ToString().Substring(parent.Length, f.DirectoryName.ToString().Length - parent.Length));

                        string error = evaluate_FSA(f.FullName,"", lastDir.Replace(@"\", ""),  f.Name);
                    }
                    catch (Exception ex)
                    {
                        totf = 0;
                    }


                }
            }

            //int GRecnum = 0;
            string result = "";
            foreach (DataRow row in CommPdfs.Rows)
            {
                for (int ii = 13; ii > 0; ii--)
                {
                    if (row[ii].ToString() != "")
                    {
                        //if (ii < 21)
                        //    erros = "";
                        if (ii != 13)
                        {
                            row[13] = row[ii];
                            row[ii] = "";
                        }
                        break;

                    }
                    else
                    {

                    }
                }
            }


            foreach (DataRow row in CommPdfs.Rows)
            {
                for (int ii = 12; ii > 0; ii--)
                {
                    if (row[ii].ToString() != "")
                    {
                        //if (ii < 21)
                        //    erros = "";
                        if (ii != 12)
                        {
                            row[12] = row[ii];
                            row[ii] = "";
                        }
                        break;

                    }
                    else
                    {

                    }
                }
            }

            string errorss = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from HOR_ReParse_FSAControl_pdfs_tmp");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                //bulkCopy.DestinationTableName =
                //    "[dbo].[Tempo_fsaData]";
                bulkCopy.DestinationTableName = "[dbo].[HOR_ReParse_FSAControl_pdfs_tmp]";

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(CommPdfs);
                }
                catch (Exception ex)
                {
                    errorss = errorss + ex.Message;
                   
                }
            }

            if(errorss == "")
                dbU.ExecuteScalar("Insert into HOR_ReParse_FSAControl_pdfs select * from HOR_ReParse_FSAControl_pdfs_tmp");
            DataTable filesProc = dbU.ExecuteDataTable("select distinct sourcename from HOR_ReParse_FSAControl_pdfs where convert(date,importdate) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
            foreach (DataRow row in filesProc.Rows)
            {
                SqlParameter[] sqlParams3;
                sqlParams3 = null;
                sqlParams3 = new SqlParameter[] { new SqlParameter("@Ofilename", row["sourcename"].ToString()), new SqlParameter("@Idate", DateTime.Now.ToString("yyyy-MM-dd")) };
                DataTable toSCI = dbU.ExecuteDataTable("HOR_rpt_ReParse_FSAControl_pdfs_SCI", sqlParams3);
                string filenameSCI = DirLocal + "\\" +  row["sourcename"].ToString().Replace(".pdf", "_to_SCI.csv");
                createCSV createdata = new createCSV();
                createdata.printCSV_fullProcess(filenameSCI, toSCI, "", "");
            }
           
        }
    
        public static string[] GetFiles(string path, string searchPattern, SearchOption searchOption)
        {
            string[] searchPatterns = searchPattern.Split('|');
            List<string> files = new List<string>();
            foreach (string sp in searchPatterns)
                files.AddRange(System.IO.Directory.GetFiles(path, sp, searchOption));
            files.Sort();
            return files.ToArray();
        }
        
        public string evaluate_Comm_C(string fileName, string dest, string subdir, string zipname)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            string errorMSG = "";
            m_IDNumber = "";
            page_addrs = 0;
            Tpage_addrs = 0;
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
                    m_TOD = page.ToString();
                    m_metadata = ""; // words[0].ToString();
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
                                        CommPdfs.Rows[CommPdfs.Rows.Count - 1]["Pages"] = page_addrs;

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
                                        while (addrs.Count < 5)
                                        {
                                            addrs.Add("");
                                        }

                                        addToTableSBC(1, fileInfo.Name, "Commercial",subdir,zipname);
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
                                        if (tmp.Length > 1 )
                                        {
                                           if(tmp.TrimStart().TrimEnd().Length > 1)
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
                                        CommPdfs.Rows[CommPdfs.Rows.Count - 1]["artifactId"] = m_IDNumber;
                                    }
                                }
                            }
                        }
                    }
                    //}
                    //else
                    
                }
                CommPdfs.Rows[CommPdfs.Rows.Count - 1]["Pages"] = page_addrs;
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

        public string evaluate_FSA(string fileName, string dest, string subdir, string zipname)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            string errorMSG = "";
            m_IDNumber = "";
            page_addrs = 0;
            Tpage_addrs = 1;
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
                    pagesperAddrs++;
                    string[] words = s.Split('\n');
                    //Text.Append(currentText);
                    int n;
                    if (words.Length < 7 && words.Length > 1)
                    {
                        if (addrs.Count > 2)
                        {
                            while (addrs.Count < 5)
                            {
                                addrs.Add("");
                            }
                            pagesperAddrs--;
                            addToTableFSA(1, fileInfo.Name, "FSA", subdir, zipname);
                            Tpage_addrs = page_addrs + 1;
                            addrs.Clear();
                            pagesperAddrs = 1;
                        }

                        //page_addrs = 1;
                        bool boolYearFiled = false;
                        bool endAddr = false;
                        string[] limitString = new string[] { "RE:", "DEAR ", "IMPORTANT:" };   //  , "RE:" 
                        m_TOD = page.ToString();
                        m_metadata = ""; // words[0].ToString();
                        index_re = 0;
                        for (int i = 0; i < words.Length; i++)
                        {
                            string tmp = words[i].ToString().ToUpper();
                            if (tmp.Length > 1)
                            {
                                addrs.Add(words[i].ToString().TrimStart().TrimEnd());
                            }

                        }
                       

                    }
                    page_addrs++;
                    //Tpage_addrs++;
                }
                if (addrs.Count > 2)
                {
                    while (addrs.Count < 5)
                    {
                        addrs.Add("");
                    }

                    addToTableFSA(1, fileInfo.Name, "FSA", subdir, zipname);
                    addrs.Clear();
                    page_addrs = 1;
                }


                //CommPdfs.Rows[CommPdfs.Rows.Count - 1]["Pages"] = page_addrs;
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

      

        public void addToTableSBC(int currline, string fname, string jobClass,string subdir,string zipname)
        {
            string test = "";
            //if (m_csz.ToString().Length > 100)
            //    m_csz = m_csz.ToString().Substring(0, 100);
            for (int x = 0; x < 5; x++)
            {
                if (addrs[x].ToString().Length > 200)
                    addrs[x] = addrs[x].ToString().Substring(0, 200);
            }



            var row = CommPdfs.NewRow();
            row["Sourcename"] = zipname;
            row["dir_in_zip"] = subdir;
            row["Importdate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            row["Seq"] = CommPdfs.Rows.Count + 1;
            row["Filename"] = fname;
            row["Pages"] = page_addrs ;
            //row[""] = ;
            row["Recnum"] = Recnum;
            row["artifactId"] = m_IDNumber;
            row["TOD"] = Tpage_addrs;
                row["coverPageAddress1"] = addrs[0];
                row["coverPageAddress2"] = addrs[1];
                row["coverPageAddress3"] = addrs[2];
                row["coverPageAddress4"] = addrs[3];
                row["coverPageAddress5"] = addrs[4];

            row["Metadata"] = m_metadata;


            CommPdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
           // C_Recnum++;

        }
        public void addToTableFSA(int currline, string fname, string jobClass, string subdir, string zipname)
        {
            string test = "";
            //if (m_csz.ToString().Length > 100)
            //    m_csz = m_csz.ToString().Substring(0, 100);
            for (int x = 0; x < 5; x++)
            {
                if (addrs[x].ToString().Length > 200)
                    addrs[x] = addrs[x].ToString().Substring(0, 200);
            }



            var row = CommPdfs.NewRow();
            row["Sourcename"] = zipname;
            row["dir_in_zip"] = subdir;
            row["Importdate"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            row["Seq"] = CommPdfs.Rows.Count + 1;
            row["Filename"] = fname;
            row["Pages"] = pagesperAddrs;  // page_addrs;
            //row[""] = ;
            row["Recnum"] = Recnum;
            row["artifactId"] = m_IDNumber;
            row["TOD"] = Tpage_addrs;
            row["coverPageAddress1"] = addrs[0];
            row["coverPageAddress2"] = addrs[1];
            row["coverPageAddress3"] = addrs[2];
            row["coverPageAddress4"] = addrs[3];
            row["coverPageAddress5"] = addrs[4];

            row["Metadata"] = m_metadata;


            CommPdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            // C_Recnum++;

        }


    }
}
