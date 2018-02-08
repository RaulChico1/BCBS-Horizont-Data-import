using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Xml;
using System.Xml.Linq;
using System.Configuration;

namespace Horizon_EOBS_Parse
{
    public class Nparse_MAEOB_cards_pdfs
    {
        DBUtility dbU;
        DataTable MAEOBpdfs = pdfs_Table_CR2();
        public void parse_all_MAEOB_cards()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string results = "";
            string DirLocal = @"\\CIERANT-TAPER\Clients\Horizon BCBS\TEST FILES\SECURE DATA\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            //DirectoryInfo originalZIPs = new DirectoryInfo(DirLocal + @"from_FTP");
            DirectoryInfo originalXLs = new DirectoryInfo(DirLocal);
            FileInfo[] FilesXLS = originalXLs.GetFiles("MAEOB*.PDF");
            if (FilesXLS.Count() > 0)
            {
                foreach (FileInfo file in FilesXLS)
                {
                    if (file.Name.IndexOf("__MAEOB") == 0)
                    { }
                    else
                    {
                        results = parse_MAEOBpdf(file.FullName.ToString(), DirLocal, file.Name);
                        if (results == "")
                        {
                            File.Move(file.FullName, ProcessVars.OtherProcessed + file.Name);
                            File.Copy(file.FullName.Replace(".pdf", ".csv"), ProcessVars.OtherProcessed + file.Name.Replace(".pdf", ".csv"));
                        }
                    }

                }
            }
        }
        public string parse_MAEOBpdf(string filename, string DirLocal, string JustFname)
        {
            string result = "";

            //dbU.ExecuteScalar("delete from HOR_parse_MBA_SMN where FName = '" + filEE.Name + "'");
            //dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + filEE.Name + "'");

            MAEOBpdfs.Clear();

            //string error = evaluate_NoDate_pdf(filEE.FullName, "");

            return result;
        }
        //public string evaluate_NoDate_pdf(string fileName, string dest)
        //{
        //    FileInfo fileInfo = new System.IO.FileInfo(fileName);
        //    bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
        //    errorMSG = "";
        //    bool isSMNJDL = false;
        //    if (fileInfo.Name.IndexOf("SMNJAL_17002") == 0)
        //    {
        //        errorMSG = "";
        //    }
        //    int index_re = 0;
        //    string strText = string.Empty;
        //    try
        //    {
        //        string[] fInfo = fileInfo.Name.Split('_');
        //        m_JulianDate = fInfo[1].ToString();
        //        m_BatchID = fInfo[2].ToString().ToUpper().Replace(".PDF", "");
        //        m_importDate = fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss");
        //        PdfReader reader = new PdfReader(fileName);
        //        totP = reader.NumberOfPages;
        //        for (int page = 1; page <= reader.NumberOfPages; page++)
        //        {
        //            ITextExtractionStrategy its = new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy();
        //            string s = PdfTextExtractor.GetTextFromPage(reader, page, its);

        //            s = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(s)));
        //            if (page == 1178)
        //                index_re = index_re;
        //            string[] words = s.Split('\n');
        //            //Text.Append(currentText);
        //            int n;
        //            if (words[0].ToString().IndexOf("$$METADATA$$") != -1)
        //            {
        //                isSMNJDL = false;
        //                m_IDNumber = "";
        //                if (MBApdfs.Rows.Count > 0)
        //                {
        //                    if (page_addrs > 1)
        //                    {
        //                        MBApdfs.Rows[MBApdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
        //                        page_addrs = 1;
        //                    }
        //                }
        //                page_addrs = 1;

        //                m_transID = m_Insert = m_TOD = m_csz = m_metadata = string.Empty;

        //                string[] metaData = words[0].ToString().Split('|');
        //                m_transID = metaData[1].ToString();
        //                //m_Insert = metaData[5].ToString(); // (3, 4 5)  ~
        //                if (metaData[3].ToString().Length > 0)
        //                    m_Insert = metaData[3].ToString();
        //                if (metaData[4].ToString().Length > 0)
        //                {
        //                    if (m_Insert.Length > 0)
        //                        m_Insert = m_Insert + "~" + metaData[4].ToString();
        //                    else
        //                        m_Insert = metaData[4].ToString();
        //                }
        //                if (metaData[5].ToString().Length > 0)
        //                {
        //                    if (m_Insert.Length > 0)
        //                        m_Insert = m_Insert + "~" + metaData[5].ToString();
        //                    else
        //                        m_Insert = metaData[5].ToString();
        //                }
        //                bool boolYearFiled = false;
        //                bool endAddr = false;
        //                string[] limitString = new string[] { "RE:", "DEAR", "IDENTIFICATION" };
        //                m_TOD = page.ToString();
        //                m_metadata = words[0].ToString();
        //                index_re = 0;
        //                for (int i = 1; i < words.Length; i++)
        //                {
        //                    string tmp = words[i].ToString().ToUpper();
        //                    if (!endAddr)
        //                    {

        //                        if (tmp.Contains(".COM"))
        //                        {
        //                            boolYearFiled = true;
        //                            addrs.Clear();
        //                        }
        //                        else
        //                        {
        //                            if (fileInfo.Name.StartsWith("SMNJDL") && !isSMNJDL)
        //                            {
        //                                boolYearFiled = true;
        //                                addrs.Clear();
        //                                isSMNJDL = true;
        //                            }
        //                            if (boolYearFiled)
        //                            {
        //                                bool b = limitString.Any(tmp.Contains);

        //                                if (b)
        //                                {
        //                                    endAddr = true;
        //                                    while (addrs.Count < 5)
        //                                    {
        //                                        addrs.Add("");
        //                                    }
        //                                    addToTableMBA(1, fileInfo.Name, "MBA_SMN");
        //                                    boolYearFiled = false;
        //                                }
        //                                else
        //                                {
        //                                    addrs.Add(words[i].ToString().TrimStart().TrimEnd());
        //                                }

        //                            }
        //                        }
        //                    }
        //                    else
        //                    {
        //                        if (!fPNO)
        //                        {
        //                            if (m_IDNumber == "")
        //                            {
        //                                if (tmp.Contains("3HZ"))
        //                                {
        //                                    int posc1 = tmp.IndexOf("3HZ");
        //                                    m_IDNumber = tmp.Substring(posc1, 12);
        //                                    MBApdfs.Rows[MBApdfs.Rows.Count - 1]["artifactId"] = m_IDNumber;
        //                                }
        //                            }
        //                        }
        //                    }
        //                }
        //            }
        //            else
        //                page_addrs++;
        //        }

        //        MBApdfs.Rows[MBApdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

        //        reader.Close();

        //    }
        //    catch (Exception ex)
        //    {
        //        errorcount++;
        //        errorMSG = ex.Message;
        //        addToTableMBA(1, fileInfo.Name, "MBA_SMN");

        //        //MessageBox.Show(ex.Message);
        //    }
        //    return "";
        //}

        private static DataTable pdfs_Table_CR2()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("FName");
            newt.Columns.Add("ImportDate");
            newt.Columns.Add("TotalP");
            newt.Columns.Add("page_addrs");
            newt.Columns.Add("JulianDate");
            newt.Columns.Add("BatchID");
            newt.Columns.Add("TransactionID");
            newt.Columns.Add("artifactId");
            newt.Columns.Add("letterName");
            newt.Columns.Add("coverPageName");
            newt.Columns.Add("coverPageAddress1");
            newt.Columns.Add("coverPageAddress2");
            newt.Columns.Add("coverPageAddress3");
            newt.Columns.Add("coverPageAddress4");
            newt.Columns.Add("coverPageCityStateZip");
            newt.Columns.Add("BRE");
            newt.Columns.Add("MED_Flag");
            newt.Columns.Add("Errors");
            newt.Columns.Add("JobClass");
            newt.Columns.Add("TOD");
            newt.Columns.Add("Metadata");
            return newt;
        }
    }
}
