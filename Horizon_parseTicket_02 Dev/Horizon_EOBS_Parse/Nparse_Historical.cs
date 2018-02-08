using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Text.RegularExpressions;
using System.Configuration;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{
    public class Nparse_Historical
    {
        //string zipDirs1 = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\HNJH_Facet_Letters";
        string zipDirs1 = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\DSNP_NJ_HEALTH";
        string zipDirs2 = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\DSNP_Letters";
        DBUtility dbU;
        int Recnum = 1;
        int initialRecnum = 0;
        int C_Recnum = 1;
        string errors = "";
        int errorcount = 0;
        int totP = 0;
        int page_addrs = 1;
        string errorMSG = "";
        string m_transID, m_Insert, m_TOD, m_csz, m_metadata, m_JulianDate, m_BatchID, m_importDate, m_IDNumber, doc_date;
        List<string> addrs = new List<string>();
        List<string> Numerics = new List<string>();
        List<string> Alphas = new List<string>();
        DataTable Histpdfs = pdfs_Table_Hist();

        public void proc_Hist()
        {
            int totprocessed = 0;
            appSets appsets = new appSets();
            appsets.setVars();
            appSets checkD = new appSets();
            string drivesOk = checkD.checkDrives();
            if (drivesOk == "")
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


                var files = from fileName in
                                Directory.EnumerateFiles(zipDirs2)
                            where fileName.ToLower().Contains(".pdf")
                            select fileName;
                foreach (var fileName in files)
                {
                    FileInfo fileInfo = new System.IO.FileInfo(fileName);
                    if (fileInfo.Name.IndexOf("__") == -1)  // && fileInfo.Name.IndexOf("SML") != -1)
                    {
                        int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(*) from HOR_parse_HNJH_DSNP_Letters where filename = '" + fileInfo.Name + "'"));
                        if (FileCount == 0)
                        {
                            if (fileInfo.Name == "003323~1.PDF")
                                test = "";
                            string result = parse_pdfsH(fileName);
                            if (result == "")
                            {

                                totprocessed++;
                            }
                        }
                    }
                }
                if (Histpdfs.Rows.Count > 0)
                {
                    foreach (DataRow row in Histpdfs.Rows)
                    {
                        for (int ii = 26; ii > 0; ii--)   //15
                        {
                            if (row[ii].ToString() != "")
                            {
                                row[26] = row[ii];
                                row[ii] = "";
                                break;
                            }
                        }
                    }
                }
                foreach (DataRow row in Histpdfs.Rows)
                {
                    if (row["Importdate"].ToString().Length < 16)
                        row["Importdate"] = "02/07/2018 15:000";
                }




                createCSV createcsv = new createCSV();
                createcsv.printCSV_fullProcess(@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\test_Histpdfs_2018_02_07_.csv", Histpdfs, "", "N");
                int updErrors = 0;
                string errors = "";
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_HNJH_DSNP_Letters_tmp");


                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    //bulkCopy.DestinationTableName =
                    //    "[dbo].[Tempo_fsaData]";
                    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_HNJH_DSNP_Letters_tmp]";

                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(Histpdfs);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;
                        updErrors++;
                    }
                }
                Connection.Close();

                if (updErrors == 0)
                {
                    //replace nulls
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", "HOR_parse_HNJH_DSNP_Letters_tmp") };

                    dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);
                    dbU.ExecuteScalar("Insert into HOR_parse_HNJH_DSNP_Letters select * from HOR_parse_HNJH_DSNP_Letters_tmp");

                }

                dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + Recnum + ",'HOR_parse_HNJH_DSNP_Letters', GETDATE())");
                
            }

        }
        public string parse_pdfsH(string fileName)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            if(fileInfo.Name.IndexOf("000447_Con") == 0)
                errorMSG = "";
            errorMSG = "";

            m_transID = m_Insert = m_TOD = m_csz = m_metadata = m_JulianDate = m_BatchID = m_importDate = m_IDNumber = doc_date = "";
            int index_re = 0;
            string strText = string.Empty;
            try
            {
                string[] fInfo = fileInfo.Name.Split('_');
                if (fInfo.Count() > 1)
                {
                    m_JulianDate = fInfo[1].ToString();
                    m_BatchID = fInfo[2].ToString().ToUpper().Replace(".PDF", "");
                    m_importDate = fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss");
                }
                PdfReader reader = new PdfReader(fileName);
                totP = reader.NumberOfPages;
                for (int page = 1; page <= reader.NumberOfPages; page++)
                {
                    ITextExtractionStrategy its = new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy();
                    string s = PdfTextExtractor.GetTextFromPage(reader, page, its);

                    s = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(s)));
                    if (page == 1178)
                        index_re = index_re;
                    string[] words = s.Split('\n');
                    //Text.Append(currentText);
                    int n;
                    if (words[0].ToString().IndexOf("$$METADATA$$") != -1)
                    {
                        m_IDNumber = "";
                        if (Histpdfs.Rows.Count > 0)
                        {
                            if (page_addrs > 1)
                            {
                                Histpdfs.Rows[Histpdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
                                page_addrs = 1;
                            }
                        }


                        m_transID = m_Insert = m_TOD = m_csz = m_metadata = string.Empty;

                        string[] metaData = words[0].ToString().Split('|');
                        m_transID = metaData[1].ToString();
                        //m_Insert = metaData[5].ToString(); // (3, 4 5)  ~
                        if (metaData[3].ToString().Length > 0)
                            m_Insert = metaData[3].ToString();
                        if (metaData[4].ToString().Length > 0)
                        {
                            if (m_Insert.Length > 0)
                                m_Insert = m_Insert + "~" + metaData[4].ToString();
                            else
                                m_Insert = metaData[4].ToString();
                        }
                        if (metaData[5].ToString().Length > 0)
                        {
                            if (m_Insert.Length > 0)
                                m_Insert = m_Insert + "~" + metaData[5].ToString();
                            else
                                m_Insert = metaData[5].ToString();
                        }
                    }

                    page_addrs = 1;
                    bool boolYearFiled = false;
                    bool endAddr = false;
                    string[] limitString = new string[] { "EVIDENCE OF COVERAGE", "DEAR ", "IMPORTANT:" };   //  , "RE:" 
                    m_TOD = page.ToString();
                    m_metadata = words[0].ToString();
                    index_re = 0;
                    for (int i = 0; i < words.Length; i++)
                    {
                        string tmp = words[i].ToString().ToUpper();
                        if (m_IDNumber == "")
                        {
                            if (tmp.Contains("3HZ"))
                            {
                                int posc1 = tmp.IndexOf("3HZ");
                                m_IDNumber = tmp.Substring(posc1, tmp.Length - tmp.IndexOf("3HZ"));

                            }
                        }

                        if (!endAddr)
                        {

                            if ((tmp.Contains("/2018") || tmp.Contains("/2016") || 
                                tmp.Contains(", 2018") || tmp.Contains(", 2016") || 
                                tmp.Contains("/2017") || tmp.Contains(", 2017")) && !boolYearFiled)
                            {
                                boolYearFiled = true;
                                addrs.Clear();
                                doc_date = tmp.Trim();
                                Numerics.Clear();
                                Alphas.Clear();

                            }
                            else
                            {
                                if (boolYearFiled)
                                {
                                    bool b = limitString.Any(tmp.Contains);
                                    bool b1 = Microsoft.VisualBasic.Information.IsNumeric(tmp);
                                    if (b1)
                                    {
                                        Numerics.Add(tmp);
                                    }
                                    else if (tmp.IndexOf("RXH") == 0 || tmp.IndexOf("DSN") == 0)
                                    {
                                        Alphas.Add(tmp);
                                    }
                                    else
                                    {
                                        if (b)
                                        {
                                            endAddr = true;
                                            while (addrs.Count < 10)
                                            {
                                                addrs.Add("");
                                            }
                                            addToTableHist(1, fileInfo.Name, "Hist");
                                            boolYearFiled = false;
                                        }
                                        else
                                        {
                                            if (tmp.Contains("3HZ"))
                                            {
                                            }
                                            else
                                                if(tmp.Trim().Length > 0)
                                                addrs.Add(words[i].ToString().TrimStart().TrimEnd());
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            
                                if (m_IDNumber == "")
                                {
                                    if (tmp.Contains("3HZ"))
                                    {
                                        int posc1 = tmp.IndexOf("3HZ");
                                        m_IDNumber = tmp.Substring(posc1, 12);
                                        Histpdfs.Rows[Histpdfs.Rows.Count - 1]["artifactId"] = m_IDNumber;
                                    }
                                }
                            
                        }
                    }
                    //}
                    //else
                    page_addrs++;
                }

                Histpdfs.Rows[Histpdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

                reader.Close();

            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = ex.Message;
                addToTableHist(1, fileInfo.Name, "Hist");

                //MessageBox.Show(ex.Message);
            }
            return "";
        }
        public void addToTableHist(int currline, string fname, string jobClass)
        {
            string test = "";
            if (m_csz.ToString().Length > 100)
                m_csz = m_csz.ToString().Substring(0, 100);
            if (errorMSG == "")
            {
                for (int x = 0; x < 9; x++)
                {
                    if (addrs[x].ToString().Length > 200)
                        addrs[x] = addrs[x].ToString().Substring(0, 200);
                }
            }


            var row = Histpdfs.NewRow();
            row["Recnum"] = Recnum;
            row["FileName"] = fname;
            row["ImportDate"] = m_importDate;
            row["TotalP"] = totP;
            row["page_addrs"] = page_addrs;
            row["JulianDate"] = m_JulianDate;
            row["BatchID"] = m_BatchID;
            row["TransactionID"] = m_transID;
            row["artifactId"] = m_IDNumber;
            row["letterName"] = "";
            if (Numerics.Count == 1)
                row["CodNum1"] = Numerics[0];
            if (Numerics.Count == 2)
            {
                row["CodNum1"] = Numerics[0];
                row["CodNum2"] = Numerics[1];
            }
            if (Numerics.Count == 3)
            {
                row["CodNum1"] = Numerics[0];
                row["CodNum2"] = Numerics[1];
                row["CodNum3"] = Numerics[2];
            }
            if (Alphas.Count == 1)
                row["CodeAlp1"] = Alphas[0];
            if (Alphas.Count == 2)
            {
                row["CodeAlp1"] = Alphas[0];
                row["CodeAlp2"] = Alphas[1];
            }
            if (Alphas.Count == 3)
            {
                row["CodeAlp1"] = Alphas[0];
                row["CodeAlp2"] = Alphas[1];
                row["CodeAlp3"] = Alphas[2];
            }
            if (errorMSG != "")
                row["Errors"] = errorMSG;

            else
            {
                row["coverPageName"] = addrs[0];
                row["coverPageAddress1"] = addrs[1];
                row["coverPageAddress2"] = addrs[2];
                row["coverPageAddress3"] = addrs[3];
                row["coverPageAddress4"] = addrs[4];
                row["coverPageAddress5"] = addrs[5];
                row["coverPageAddress6"] = addrs[6];
                row["coverPageAddress7"] = addrs[7];
                row["coverPageAddress8"] = addrs[8];

                row["coverPageCityStateZip"] = m_csz;

                row["BRE"] = "";   // nO INSERT
            }

            row["DocDate"] = doc_date;            
            //row["JOBID"] = JobID;
            row["MED_Flag"] = "N";
            row["JobClass"] = jobClass; // "HLGS";
            row["TOD"] = m_TOD;
            row["Metadata"] = m_metadata;
            //row["CycleDate"] = DateTime.Now.ToString("yyyy-MM-dd");
            //if (addrs[0].ToString().IndexOf("JESSICA YOUNG") != -1)
            //    test = "here";

            Histpdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            C_Recnum++;

        }
      
        public void OutputHis()
        {

        }
        private static DataTable pdfs_Table_Hist()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("FileName");
            newt.Columns.Add("ImportDate");
            newt.Columns.Add("TotalP");
            newt.Columns.Add("page_addrs");
            newt.Columns.Add("JulianDate");
            newt.Columns.Add("BatchID");
            newt.Columns.Add("TransactionID");
            newt.Columns.Add("artifactId");
            newt.Columns.Add("letterName");
            newt.Columns.Add("CodNum1");
            newt.Columns.Add("CodNum2");
            newt.Columns.Add("CodNum3");
            newt.Columns.Add("CodeAlp1");
            newt.Columns.Add("CodeAlp2");
            newt.Columns.Add("CodeAlp3");
            newt.Columns.Add("DocDate");
            newt.Columns.Add("coverPageName");
            newt.Columns.Add("coverPageAddress1");
            newt.Columns.Add("coverPageAddress2");
            newt.Columns.Add("coverPageAddress3");
            newt.Columns.Add("coverPageAddress4");
            newt.Columns.Add("coverPageAddress5");
            newt.Columns.Add("coverPageAddress6");
            newt.Columns.Add("coverPageAddress7");
            newt.Columns.Add("coverPageAddress8");

            newt.Columns.Add("coverPageCityStateZip");
            newt.Columns.Add("BRE");
            newt.Columns.Add("MED_Flag");
            newt.Columns.Add("Errors");
            newt.Columns.Add("FileStatus");
            newt.Columns.Add("JobClass");

            newt.Columns.Add("TOD");
            newt.Columns.Add("UpdAddr1");
            newt.Columns.Add("UpdAddr2");
            newt.Columns.Add("UpdAddr3");
            newt.Columns.Add("UpdAddr4");
            newt.Columns.Add("UpdAddr5");
            newt.Columns.Add("UpdCity");
            newt.Columns.Add("UpdState");
            newt.Columns.Add("UpdZip");
            newt.Columns.Add("UpdCounty");
            newt.Columns.Add("UpdLat");
            newt.Columns.Add("UpdLong");
            newt.Columns.Add("IMBChar");
            newt.Columns.Add("IMBDigit");
            newt.Columns.Add("DL");
            newt.Columns.Add("Metadata");
            return newt;
        }
    }
}
