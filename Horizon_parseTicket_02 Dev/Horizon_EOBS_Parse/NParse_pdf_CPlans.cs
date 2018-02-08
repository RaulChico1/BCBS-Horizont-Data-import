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
    public class NParse_pdf_CPlans
    {
        DataTable Cplanspdfs = pdfs_Table_CPlans();
        DBUtility dbU;
        int Recnum = 1;
        int initialRecnum = 0;
        int C_Recnum = 1;
        string errors = "";
        int errorcount = 0;
        int totP = 0;
        int page_addrs = 1;
        string errorMSG = "";
        string m_transID, m_Insert, m_TOD, m_csz, m_metadata, m_JulianDate, m_BatchID, m_importDate, m_IDNumber;
        List<string> addrs = new List<string>();

        public string zipFilesinDir_I_Information(string dateProcess, string directory)
        {
            if (Directory.Exists(directory))
            {
                //string[] extensions = new[] { "mba*.pdf", "sbc*.pdf", "smn*.pdf" };
                DirectoryInfo originalPDFs = new DirectoryInfo(directory);
                var FilesPDF = originalPDFs.GetFiles("*ONEXRN*.pdf").ToList();
                var FilesPDF1 = originalPDFs.GetFiles("SBC*.pdf").ToList();
                var FilesPDF2 = originalPDFs.GetFiles("*OFCTRN*.pdf").ToList();
                var FilesPDF3 = originalPDFs.GetFiles("*RNLOFF*.pdf").ToList();
                var FilesPDF4 = originalPDFs.GetFiles("*SAPDRN*.pdf").ToList();
                var FilesPDF5 = originalPDFs.GetFiles("*SHOPRN*.pdf").ToList();
                var FilesPDF6 = originalPDFs.GetFiles("ONC*.pdf").ToList();
                //var FilesPDF7 = originalPDFs.GetFiles("MBALTR*.pdf").ToList();
                var Fmaster = FilesPDF.Concat(FilesPDF1).Concat(FilesPDF2).Concat(FilesPDF3).Concat(FilesPDF4).Concat(FilesPDF5).Concat(FilesPDF6).ToArray();



                if (Fmaster.Count() > 0)
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


                    foreach (var filEE in Fmaster)
                    {
                        //FileInfo file = new System.IO.FileInfo(filEE.ToString());
                        if (filEE.Name.Substring(0, 1) != "_")
                        {
                            try
                            {
                                //HOR_parse_CareRadius_2
                                var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_CPlans where FileName = '" + filEE.Name + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                {
                                    dbU.ExecuteScalar("delete from HOR_parse_CPlans where FileName = '" + filEE.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + filEE.Name + "'");
                                }

                                Cplanspdfs.Clear();
                                string error = evaluate_CPlanspdf(filEE.FullName, "");
                                if (error != "")
                                    errors = errors + error + "\n\n";
                                else
                                    if (Cplanspdfs.Rows.Count > 0)
                                    {
                                        DateTime dateUpload;
                                        string strsql = "select importdate_start from HOR_parse_files_downloaded where filename = '" + filEE.Name + "'";
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
                                                   DSeqnum + ",'" + filEE.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + filEE.Extension.Replace(".", "") + "',1,'" + directory + "','" +
                                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                                        }
                                        else
                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);

                                        finalprocessCPlans(directory.Replace("\\from_FTP", ""), dateProcess, filEE.Name, dateProcess, "CPlans", dateUpload);   //    finalprocess(direcTory, dateHLGS, "Coba", cycleDate, "HLGS");
                                        if (File.Exists(ProcessVars.OtherProcessed + filEE.Name))
                                            File.Delete(ProcessVars.OtherProcessed + filEE.Name);
                                        File.Copy(filEE.FullName, ProcessVars.OtherProcessed + filEE.Name);
                                        File.Move(filEE.FullName, filEE.DirectoryName + "\\__" + filEE.Name); //  file.FullName.Replace("MBA", "__MBA"));
                                    }
                            }
                            catch (Exception ez)
                            {
                                errors = errors + filEE.FullName + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                }

            }
            return errors;
        }
        public string evaluate_CPlanspdf(string fileName, string dest)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            errorMSG = "";

            m_transID = m_Insert = m_TOD = m_csz = m_metadata = m_JulianDate = m_BatchID = m_importDate = m_IDNumber = "";
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
                        m_IDNumber = "";
                        if (Cplanspdfs.Rows.Count > 0)
                        {
                            if (page_addrs > 1)
                            {
                                Cplanspdfs.Rows[Cplanspdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
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
                        string[] limitString = new string[] { "IMPORTANT INFORMATION", "DEAR ", "IMPORTANT:" };   //  , "RE:" 
                        m_TOD = page.ToString();
                        m_metadata = words[0].ToString();
                        index_re = 0;
                        for (int i = 1; i < words.Length; i++)
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

                                if ((tmp.Contains("/2018") || tmp.Contains("/2016") || tmp.Contains(", 2018") || 
                                    tmp.Contains(", 2016") || tmp.Contains("/2017") || tmp.Contains(", 2017")) && !boolYearFiled)
                                {
                                    boolYearFiled = true;
                                    addrs.Clear();
                                   
                                }
                                else
                                {
                                    if (boolYearFiled)
                                    {
                                        bool b = limitString.Any(tmp.Contains);

                                        if (b)
                                        {
                                            endAddr = true;
                                            while (addrs.Count < 5)
                                            {
                                                addrs.Add("");
                                            }
                                            addToTableCplans(1, fileInfo.Name, "Cplans");
                                            boolYearFiled = false;
                                        }
                                        else
                                        {
                                            if (tmp.Contains("3HZ"))
                                            {
                                            }
                                            else
                                                addrs.Add(words[i].ToString().TrimStart().TrimEnd());
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
                                            Cplanspdfs.Rows[Cplanspdfs.Rows.Count - 1]["artifactId"] = m_IDNumber;
                                        }
                                    }
                                }
                            }
                        }
                    //}
                    //else
                        page_addrs++;
                }

                Cplanspdfs.Rows[Cplanspdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

                reader.Close();

            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = ex.Message;
                addToTableCplans(1, fileInfo.Name, "MBA_SMN");

                //MessageBox.Show(ex.Message);
            }
            return "";
        }
        public void addToTableCplans(int currline, string fname, string jobClass)
        {
            string test = "";
            if (m_csz.ToString().Length > 100)
                m_csz = m_csz.ToString().Substring(0, 100);
            for (int x = 0; x < 5; x++)
            {
                if (addrs[x].ToString().Length > 200)
                    addrs[x] = addrs[x].ToString().Substring(0, 200);
            }



            var row = Cplanspdfs.NewRow();
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
            if (errorMSG != "")
                row["Errors"] = errorMSG;
            else
            {
                row["coverPageName"] = addrs[0];
                row["coverPageAddress1"] = addrs[1];
                row["coverPageAddress2"] = addrs[2];
                row["coverPageAddress3"] = addrs[3];
                row["coverPageAddress4"] = addrs[4];
                row["coverPageCityStateZip"] = m_csz;
               
                    row["BRE"] = "";   // nO INSERT
            }
            //row["JOBID"] = JobID;
            row["MED_Flag"] = "N";
            row["JobClass"] = jobClass; // "HLGS";
            row["TOD"] = m_TOD;
            row["Metadata"] = m_metadata;
            //row["CycleDate"] = DateTime.Now.ToString("yyyy-MM-dd");
            if (addrs[0].ToString().IndexOf("JESSICA YOUNG") != -1)
                test = "here";

            Cplanspdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            C_Recnum++;

        }

        public string finalprocessCPlans(string direcTory, string dateHLGS, string unzipName, string cycleDate, string FileType, DateTime lastW)
        {
            string processCompleted = "";
            DataView dv = Cplanspdfs.DefaultView;
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

            foreach (DataRow row in sortedPDFs.Rows)
            {
                for (int ii = 15; ii > 0; ii--)
                {
                    if (row[ii].ToString() != "")
                    {
                        row[15] = row[ii];
                        row[ii] = "";
                        break;
                    }
                }
            }
                
            //upload to sql
            int updErrors = 0;
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from HOR_parse_CPlans_TMP");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                //bulkCopy.DestinationTableName =
                //    "[dbo].[Tempo_fsaData]";
                bulkCopy.DestinationTableName = "[dbo].[HOR_parse_CPlans_TMP]";

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(sortedPDFs);
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
                sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", "HOR_parse_HNJH_Champion_TMP") };

                dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);
                dbU.ExecuteScalar("Insert into HOR_parse_CPlans select * from HOR_parse_CPlans_TMP");

            }
            int GRecnum;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_CPlans_TMP");

            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) ;
            string csvName = unzipName.Substring(0, unzipName.Length - 4) + ".csv";
            string BCCname = "HNJH-PR_" + unzipName.Substring(0, unzipName.Length - 4) + "_toBCC.csv";
            string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";

            dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + GRecnum  + ",'HOR_parse_CPlans', GETDATE())");
            string wSysout = csvName.Substring(0, csvName.IndexOf("_") - 1);
            dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                            sortedPDFs.Rows.Count + ",'" + BCCname + "','" + csvName + "','" + lastW.ToString("MM/dd/yyyy HH:mm:ss") + "','HOR_parse_CPlans','" +
                                            directoryAfterCass + "','','" + wSysout + "','','Receive','" + GlobalVar.DateofProcess + "')");
            DataTable toBCC = dbU.ExecuteDataTable("select Recnum, '' as f2,'' as f3,'' as f4,'' as f5,'' as f6,'' as f7,'' as f8,'' as f9,'' as f10,'' as f11,'' as f12,'' as f13,'' as f14, " +
                "coverPageName, coverPageAddress1, coverPageAddress2, coverPageAddress3, coverPageAddress4,coverPageCityStateZip from HOR_parse_CPlans where filename = '" + csvName.Replace(".csv", ".pdf") + "'");



            createCAS_CSV create_cas__csv = new createCAS_CSV();
            string pName = direcTory + @"\" + BCCname;
            //string BCCname = unzipName.Replace(".pdf", "") + "_toBCC.csv";
            if (toBCC.Rows.Count > 0)
            {
                if (File.Exists(pName))
                    File.Delete(pName);

                createCSV createcsv = new createCSV();
                createcsv.printCSV_fullProcess(pName, toBCC, "", "N");
                string cassFileName = ProcessVars.gDMPs + BCCname;
                File.Copy(pName, cassFileName);
            }

            return processCompleted;
        }
        public string retun_Cplans()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable toProcess = dbU.ExecuteDataTable("select filename, filenamecass from HOR_parse_files_to_CASS where processed is null and tablename = 'HOR_parse_CPlans' and convert(date,dateprocess) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'");
            if (toProcess.Rows.Count > 0)
            {
                foreach (DataRow row in toProcess.Rows)
                {
                    Import_Generic returns = new Import_Generic();
                    string result = returns.ProcessReturnfromBCC( row[1].ToString());
                }
            }
            return "updates back ready C Plans";
        }
        public string print_Cplans()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable toProcess = dbU.ExecuteDataTable("select filename, filenamecass from HOR_parse_files_to_CASS where processed is not null and tablename = 'HOR_parse_CPlans' and dateprocess = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'");
            if (toProcess.Rows.Count > 0)
            {
                foreach (DataRow row in toProcess.Rows)
                {
                    //DataTable datatoSCI = dbU.ExecuteDataTable("Select Recnum, TransactionID,  filename as FName, artifactid, imbChar as lettername, UpdAddr1 as coverPageName, UpdAddr5 as coverPageAddress1, UpdAddr2 as coverPageAddress2,  UpdAddr3 as coverPageAddress3, UpdAddr4 as coverPageAddress4,UpdCity as City, UpdState as State, UpdZip as ZIP , bre, TOD, DL " +
                    //                        "from HOR_parse_CPlans where FileName = '" + row[0].ToString().Replace(".csv",".pdf") + "' order by recnum");
                     SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@fname", row[0].ToString().Replace(".csv",".pdf")) };


                DataTable datatoSCI = dbU.ExecuteDataTable("HOR_rpt_C_Plans_Output_SCI", sqlParams2);
                    if (datatoSCI.Rows.Count > 0)
                    {
                        string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";

                        string pName = directoryAfterCass + @"\" + row[0].ToString().Replace(".csv", "_toSCI.csv");
                        if (File.Exists(pName))
                            File.Delete(pName);
                        createCSV createcsv = new createCSV();
                        createcsv.printCSV_fullProcess(pName, datatoSCI, "", "N");
                    }
                }
            }
            return "updates back ready C Plans";
        }
        private static DataTable pdfs_Table_CPlans()
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
            newt.Columns.Add("coverPageName");
            newt.Columns.Add("coverPageAddress1");
            newt.Columns.Add("coverPageAddress2");
            newt.Columns.Add("coverPageAddress3");
            newt.Columns.Add("coverPageAddress4");
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
