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
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{


    public class Parse_Inv_pdf
    {
        DataTable invPdfs = pdfs_Table_INV();
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
        string cycleDate = "";
        string m_transID, m_Insert, m_TOD, m_Acct, m_Inv, m_metadata, m_JulianDate, m_BatchID, m_importDate, m_IDNumber;
        DBUtility dbU;

       
        public string zipFilesinDir_INV(string dateProcess, string directory)
        {

            if (Directory.Exists(directory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(directory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("INVCON*.pdf");
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
                        if (file.Name.Substring(0,1) != "_")
                        {
                            try
                            {
                                //HOR_parse_CareRadius_2
                                var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_HIX_Inv where FileName = '" + file.Name + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                {
                                    dbU.ExecuteScalar("delete from HOR_parse_HIX_Inv where FileName = '" + file.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + file.Name + "'");
                                }

                                invPdfs.Clear();
                                string error = evaluate_inv_pdf(file.FullName, "");
                                if (error != "")
                                    errors = errors + error + "\n\n";
                                else
                                    if (invPdfs.Rows.Count > 0)
                                    {
                                        DateTime dateUpload;
                                        string strsql = "select importdate_start from HOR_parse_files_downloaded where filename = '" + file.Name + "'";
                                        //DateTime dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                                        var fileDate = dbU.ExecuteScalar(strsql);
                                        if (fileDate == null)
                                        {
                                            int DSeqnum = 0;
                                            var Drecnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");

                                            if (Drecnum.ToString() == "")
                                                DSeqnum = 1;
                                            else
                                                DSeqnum = Convert.ToInt32(Drecnum.ToString()) + 1;

                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                   DSeqnum + ",'" + file.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + file.Extension.Replace(".", "") + "',1,'" + directory + "','" +
                                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                                        }
                                        else
                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);


                                        //string dateUpload = (string)dbU.ExecuteScalar(strsql);
                                        // DateTime DateUpload = Convert.ToDateTime(dateUpload);
                                        //file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss")
                                        finalprocessInv(directory.Replace("\\from_FTP", ""), dateProcess, file.Name, dateProcess, "HIX_Inv", dateUpload);   //    finalprocess(direcTory, dateHLGS, "Coba", cycleDate, "HLGS");
                                        File.Move(file.FullName, file.FullName.Replace("INVCON", "__INVCON"));

                                    }
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                }

            }
            return errors;
        }


        public string evaluate_inv_pdf(string fileName, string dest)
        {

            errorMSG = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int index_re = 0;
            string strText = string.Empty;
            try
            {
                string[] fInfo = fileInfo.Name.Split('_');
                m_JulianDate = fInfo[1].ToString();
                m_BatchID = fInfo[2].ToString().ToUpper().Replace(".PDF", "");
                m_importDate = fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss");
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
                        if (invPdfs.Rows.Count > 0)
                        {
                            if (page_addrs > 1)
                            {
                                invPdfs.Rows[invPdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
                                page_addrs = 1;
                            }
                        }
                        page_addrs = 1;

                        m_transID = m_Insert = m_TOD = m_Acct = m_Inv = m_metadata = string.Empty;

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


                        m_TOD = page.ToString();
                        m_metadata = words[0].ToString();
                        index_re = 0;
                        bool boolYearFiled = false;
                        bool endAddr = false;
                        for (int i = 1; i < words.Length; i++)
                        {
                            string tmp = words[i].ToString().ToUpper();
                            if (m_IDNumber == "")
                            {
                                if (tmp.Contains("3HZ"))
                                {
                                    int posc1 = tmp.IndexOf("3HZ");
                                    m_IDNumber = tmp.Substring(posc1, 12);
                                }
                            }
                            if (m_Acct == "")
                            {
                                if (tmp.Contains("ACCOUNT #:"))
                                {
                                    int posc1 = tmp.IndexOf("ACCOUNT #:") + 10;
                                    m_Acct = tmp.Substring(posc1, 10);
                                }
                            }
                            if (m_Inv == "")
                            {
                                if (tmp.Contains("INVOICE #:"))
                                {
                                    int posc1 = tmp.IndexOf("ACCOUNT #:") + 11;
                                    m_Inv = tmp.Substring(posc1, 10);
                                }
                            }





                            if (tmp.Contains("SEQ #"))
                            {
                                boolYearFiled = true;
                                addrs.Clear();
                                int posc = tmp.IndexOf("SEQ #") + 6;
                                m_transID = tmp.Substring(posc, 6);
                            }

                            if (boolYearFiled )
                            {
                                if (!tmp.Contains("SEQ #"))
                                {
                                   // bool digitsOnly = IsDigitsOnly(words[i].ToString().Replace(" ", ""));
                                   //if (digitsOnly)
                                    if (tmp.Contains("ATTN:"))
                                    {
                                        addrs.Add(words[i].ToString().TrimStart().TrimEnd());
                                        while (addrs.Count < 5)
                                        {
                                            addrs.Add("");
                                        }
                                        addToTableInv(1, fileInfo.Name, "HIX_Inv");
                                        boolYearFiled = false;
                                        endAddr = true;
                                    }
                                    else
                                    {
                                       
                                        addrs.Add(words[i].ToString().TrimStart().TrimEnd());
                                    }
                                }
                            }

                        }

                    }
                    else
                        page_addrs++;
                }

                invPdfs.Rows[invPdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

                reader.Close();

            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = errorMSG  + ex.Message;
                addToTableInv(1, fileInfo.Name, "HIX_Inv");

                //MessageBox.Show(ex.Message);
            }
            return errorMSG;
        }
        public static bool IsDigitsOnly(string str)
        {
            for (int index = 0; index < str.Length; index++)
            {
                char c = str[index];
                if (c < '0' || c > '9')
                    return false;
            }

            return true;
        }

        public void addToTableInv(int currline, string filename, string jobClass)
        {
            string test = "";
           
            for (int x = 0; x < 5; x++)
            {
                if (addrs[x].ToString().Length > 200)
                    addrs[x] = addrs[x].ToString().Substring(0, 200);
            }



            var row = invPdfs.NewRow();
            row["Recnum"] = Recnum;
            row["FileName"] = filename;
            row["ImportDate"] = m_importDate;
            row["TotalP"] = totP;
            row["page_addrs"] = page_addrs;
            row["JulianDate"] = m_JulianDate;
            row["BatchID"] = m_BatchID;
            row["TransactionID"] = m_transID;
            row["letterName"] = "";
            row["AccountNo"] = m_Acct;
            row["InvoiceNo"] = m_Inv;
            if (errorMSG != "")
                row["Errors"] = errorMSG;
            else
            {
                row["coverPageName"] = addrs[0];
                row["coverPageAddress1"] = addrs[1];
                row["coverPageAddress2"] = addrs[2];
                row["coverPageAddress3"] = addrs[3];
                row["coverPageAddress4"] = addrs[4];
                row["coverPageCityStateZip"] = "";
                if (m_Insert.Length > 0)
                    row["BRE"] = "3701";
                //row["BRE"] = m_Insert;
                else
                    row["BRE"] = "";
            }
            //row["JOBID"] = JobID;
            row["MED_Flag"] = "N";
            row["JobClass"] = jobClass; // "HLGS";
            row["TOD"] = m_TOD;
            row["Metadata"] = m_metadata;

            if (addrs[0].ToString().IndexOf("JESSICA YOUNG") != -1)
                test = "here";

            invPdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            C_Recnum++;

        }
        public string finalprocessInv(string direcTory, string dateHLGS, string unzipName, string cycleDate, string FileType, DateTime lastW)
        {
            string processCompleted = "";
            DataView dv = invPdfs.DefaultView;
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
                        totDoc = totDoc + Convert.ToInt32(sortedPDFs.Rows[i][4].ToString());
                        totFile = Convert.ToInt32(sortedPDFs.Rows[i][3].ToString());
                        backupRowNumber = i;
                    }
                }
                else
                {
                    totDoc = totDoc + Convert.ToInt32(sortedPDFs.Rows[i][4].ToString());
                    totFile = Convert.ToInt32(sortedPDFs.Rows[i][3].ToString());
                    backupRowNumber = i;
                }


            }


            //upload to sql
            createCAS_CSV create_cas__csv = new createCAS_CSV();
            string pName = direcTory + @"\FromCass\" + unzipName.Replace(".pdf", "") + ".csv";
            //string BCCname = unzipName.Replace(".pdf", "") + "_toBCC.csv";
            if (sortedPDFs.Rows.Count > 0)
            {
                string resultcsv = create_cas__csv.create_INV_CSV(
                                    unzipName,  sortedPDFs, FileType, Recnum, "", "", m_BatchID, dateHLGS, cycleDate);
                if (resultcsv != "")
                    processCompleted = resultcsv + "\n\n";
            }

            //DataTable working_NLPdfs = NLPdfs.Copy();
            //
            if (File.Exists(pName))
                File.Delete(pName);

            SqlParameter[] sqlParams;
            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@FileName", unzipName), new SqlParameter("@table", "HOR_parse_HIX_Inv") };

            string spName = "HOR_rpt_PARSE_Inv_noBCC_to_SCI";
            DataTable datato_SCI = dbU.ExecuteDataTable(spName, sqlParams);
            int totrecs = 0;
            string fileNameCass = unzipName.Replace(".csv", ".pdf  File not to CASS");
            if (datato_SCI.Rows.Count > 0)
            {
                totrecs = datato_SCI.Rows.Count;
                createCSV createcsv = new createCSV();
                //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                //string pName = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < datato_SCI.Columns.Count; index++)
                {
                    fieldnames.Add(datato_SCI.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in datato_SCI.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < datato_SCI.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }

            }
            string OutputDataPath = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed";
            File.Copy(pName, OutputDataPath + "\\" + unzipName.Replace(".pdf", "") + ".csv");
          
            return processCompleted;
        }

        private static DataTable pdfs_Table_INV()
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
            newt.Columns.Add("AccountNo");
            newt.Columns.Add("InvoiceNo");
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
