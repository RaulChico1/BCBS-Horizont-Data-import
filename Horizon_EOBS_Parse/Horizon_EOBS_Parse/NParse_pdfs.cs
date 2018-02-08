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

namespace Horizon_EOBS_Parse
{
    public class NParse_pdfs
    {
        DataTable NLPdfs = pdfs_Table();
        DataTable CR2Pdfs = pdfs_Table_CR2();
        DataTable MBApdfs = pdfs_Table_CR2();
        DataTable SBCpdfs = pdfs_Table_SBC();
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
        string m_transID, m_Insert, m_TOD, m_csz, m_metadata, m_JulianDate, m_BatchID, m_importDate, m_IDNumber;
        DBUtility dbU;

        
        public string ProcessFiles(string dateProcess)
        {
            ProcessVars.serviceIsrunning = true;
            //autoEvent.WaitOne(1000 * 60 * 3, false);
            string result = zipFilesinDir(dateProcess);
            ProcessVars.serviceIsrunning = false;

            return "Done at" + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");;
        }
        public string zipFilesinDir(string dateProcess)
        {

            if (Directory.Exists(ProcessVars.InputDirectory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(ProcessVars.oNLpdfsDirectory);
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
                                string error = evaluate_pdf(file.FullName, "",file.Name);
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
                    finalprocess("", "", "", "", "HLGS");   //dateHLGS
            }
            return errors;
        }

        public string zipFilesinDir_Cr2(string dateProcess, string directory)
        {

            if (Directory.Exists(directory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(directory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("CR_*.pdf");
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
                        if (file.Name.IndexOf("_CR_") == -1)
                        {
                            try
                            {
                                //HOR_parse_CareRadius_2
                                var fileU = dbU.ExecuteScalar("select FName from HOR_parse_CareRadius_2 where FName = '" + file.Name + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                {
                                    dbU.ExecuteScalar("delete from HOR_parse_CareRadius_2 where FName = '" + file.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + file.Name + "'");
                                }

                                CR2Pdfs.Clear();
                                string error = evaluate_CR2_pdf(file.FullName, "");
                                if (error != "")
                                    errors = errors + error + "\n\n";
                                else
                                    if (CR2Pdfs.Rows.Count > 0)
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
                                                   DSeqnum + ",'" + file.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + file.Extension.Replace(".","") + "',1,'" + directory + "','" +
                                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                                        }
                                        else
                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);


                                        //string dateUpload = (string)dbU.ExecuteScalar(strsql);
                                       // DateTime DateUpload = Convert.ToDateTime(dateUpload);
                                        //file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss")
                                        finalprocessCR2(directory.Replace("\\from_FTP", ""), dateProcess, file.Name, dateProcess, "CareRadius_2", dateUpload);   //    finalprocess(direcTory, dateHLGS, "Coba", cycleDate, "HLGS");
                                        File.Move(file.FullName, file.FullName.Replace("CR_", "__CR_"));
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
        public string zipFilesinDir_MBA(string dateProcess, string directory)
        {

            if (Directory.Exists(directory))
            {
                //string[] extensions = new[] { "mba*.pdf", "sbc*.pdf", "smn*.pdf" };
                DirectoryInfo originalPDFs = new DirectoryInfo(directory);
                var FilesPDF = originalPDFs.GetFiles("MBA*.pdf").ToList();
                //var FilesPDF1 = originalPDFs.GetFiles("SBC*.pdf").ToList();
                var FilesPDF2 = originalPDFs.GetFiles("SMN*.pdf").ToList();
                var FilesPDF3 = originalPDFs.GetFiles("PNO*.pdf").ToList();
                //var Fmaster = FilesPDF.Concat(FilesPDF1).Concat(FilesPDF2).ToArray();
                var Fmaster = FilesPDF.Concat(FilesPDF2).Concat(FilesPDF3).ToArray();



                //FileInfo[] FilesPDF = originalPDFs.EnumerateFiles()
                //        .Where(f => extensions.Contains(f.Name.ToLower()))
                //        .ToArray();


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
                                var fileU = dbU.ExecuteScalar("select FName from HOR_parse_MBA_SMN where FName = '" + filEE.Name + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                {
                                    dbU.ExecuteScalar("delete from HOR_parse_MBA_SMN where FName = '" + filEE.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + filEE.Name + "'");
                                }

                                MBApdfs.Clear();
                                string error = evaluate_MBA_pdf(filEE.FullName, "");
                                if (error != "")
                                    errors = errors + error + "\n\n";
                                else
                                    if (MBApdfs.Rows.Count > 0)
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


                                        //string dateUpload = (string)dbU.ExecuteScalar(strsql);
                                        // DateTime DateUpload = Convert.ToDateTime(dateUpload);
                                        //file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss")
                                        finalprocessMBA(directory.Replace("\\from_FTP", ""), dateProcess, filEE.Name, dateProcess, "MBA_SMN", dateUpload);   //    finalprocess(direcTory, dateHLGS, "Coba", cycleDate, "HLGS");
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
        public string zipFilesinDir_SBC(string dateProcess, string directory)
        {

            if (Directory.Exists(directory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(directory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("SBC*.pdf");
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
                        //FileInfo file = new System.IO.FileInfo(filEE.ToString());
                        if (file.Name.Substring(0, 1) != "_")
                        {
                            try
                            {
                                //HOR_parse_CareRadius_2
                                var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_SBC where FileName = '" + file.Name + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                {
                                    dbU.ExecuteScalar("delete from HOR_parse_SBC where FileName = '" + file.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + file.Name + "'");
                                }

                                SBCpdfs.Clear();
                                string error = evaluate_SBC_pdf(file.FullName, "");
                                if (error != "")
                                    errors = errors + error + "\n\n";
                                else
                                    if (SBCpdfs.Rows.Count > 0)
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
                                                   DSeqnum + ",'" + file.Name + "','" + file.Extension.Replace(".", "") + "',1,'" + directory + "','" +
                                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                                        }
                                        else
                                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);


                                        finalprocessSBC(directory.Replace("\\from_FTP", ""), dateProcess, file.Name, dateProcess, "SBC", dateUpload);   //    finalprocess(direcTory, dateHLGS, "Coba", cycleDate, "HLGS");
                                        File.Move(file.FullName, file.DirectoryName + "\\__" + file.Name); //  file.FullName.Replace("MBA", "__MBA"));
                                    }
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file.FullName + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                }

            }
            return errors;
        }
        public string zipFilesinDirService(string dateProcess, string direcTory)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            cycleDate = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            if (Directory.Exists(direcTory))
            {

                DirectoryInfo originalZIPs = new DirectoryInfo(direcTory);
                string unzipDirName = "";
                foreach (FileInfo f in originalZIPs.GetFiles("HLGS*.zip"))
                {
                    if (f.Name.IndexOf("_") == 0)
                    { //processed already
                    }
                    else
                    {
                        string strsql = "delete from HOR_parse_HLGS where CONVERT(DATE,ImportDate)= '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and ZipName = '" + f.Name + "'";
                        dbU.ExecuteNonQuery(strsql);

                       // GlobalVar.dbaseName = "BCBS_Horizon";
                        //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                        var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + f.Name.ToString() + "'");
                        if (fileDate != null)
                            dateHLGS = fileDate.ToString();
                        else
                            dateHLGS = GlobalVar.DateofProcess.ToString("yyyy-MM-dd HH:mm:ss");
                         unzipDirName = f.Name.ToString().Replace(".zip", "").Replace("HLGS_", "");

                        if (Directory.Exists(unzipDirName))
                            Directory.Delete(unzipDirName);
                        DirectoryInfo originalPDFs = new DirectoryInfo(direcTory + @"\" + unzipDirName);
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
                                        string error = evaluate_pdf(file.FullName, "", f.Name);
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
                    }
                    if (NLPdfs.Rows.Count > 0)
                    {
                        finalprocess(direcTory, dateHLGS, "D" + unzipDirName, cycleDate, "HLGS");
                        File.Move(f.FullName, f.FullName.Replace("HLGS_", "__HLGS_"));

                    }
                    NLPdfs.Clear();
                }

                foreach (FileInfo f in originalZIPs.GetFiles("COBA*.pdf"))
                {
                    GlobalVar.dbaseName = "BCBS_Horizon";
                    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                    if (f.Name.IndexOf("_") == 0)
                    { //processed already
                    }
                    else
                    {
                        string strsql = "delete from HOR_parse_HLGS where CONVERT(DATE,ImportDate)= '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and ZipName = '" + f.Name + "'";
                        dbU.ExecuteNonQuery(strsql);

                        var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + f.Name.ToString() + "'");
                        if (fileDate != null)
                            dateHLGS = fileDate.ToString();
                        else
                            dateHLGS = GlobalVar.DateofProcess.ToString("yyyy-MM-dd HH:mm:ss");


                        try
                        {
                            string error = evaluate_pdf(f.FullName, "", f.Name);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + f + "  " + ez.Message + "\n\n";
                        }
                        if (NLPdfs.Rows.Count > 0)
                        {
                            finalprocess(direcTory, dateHLGS, "Coba", cycleDate, "HLGS");
                            File.Move(f.FullName, f.FullName.Replace("COBA_", "__COBA_"));
                        }
                        NLPdfs.Clear();
                    }
                }



       
            }
            return errors;
        }
        public string evaluate_pdf(string fileName, string dest, string ZipName)
        {
            
            errorMSG = "";
            bool doc_NO_addr = false;
            int LineStart = 5;

            bool addrFound = false;

            bool found_RE_Dear = false;

            bool isNotification = false;

            int index_date = 0;
            int index_re = 0;

            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            if (recnum.ToString() == "")
                Recnum = 1;
            else
                Recnum = Convert.ToInt32(recnum.ToString()) + 1;

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            if (fileInfo.Name == "COBA_15236_9426.pdf" ||
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
                   
                    string[] words = s.Split('\n');
                    //Text.Append(currentText);
                    int n;
                    if (page == 1)
                    {
                        for (int i = 0; i < words.Length; i++)
                        {

                            string[] importString = new string[] { "RE:", "DEAR", "INQUIRY ID:", "THIS NOTIFICATION WAS ISSUED", "NOTICE OF DISMISSAL" };
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
                                if (words[i].Contains("/2015") || words[i].Contains("/2016"))
                                {
                                    index_date = i;
                                    break;
                                }
                                else
                                {
                                    if (words[i].Contains(" 2015") || words[i].Contains(" 2016"))
                                    {
                                        index_date = i;
                                        break;
                                    }
                                    else
                                    {
                                        if (words[i].Contains(" 2015") || words[i].Contains(" 2016"))
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
                                if (words[index_re].ToUpper().Contains("DEAR") || words[index_re].ToUpper().Contains("RE:")
                                    || words[index_re].ToUpper().Contains("INQUIRY ID:")
                                    || words[index_re].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                                    || words[index_re].ToUpper().Contains("NOTICE OF DISMISSAL")
                                    || words[index_re + 1].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                                    )
                                {
                                    //other addrs
                                    NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = page_addrs;
                                    page_addrs = 1;
                                    addrFound = false;

                                    addrFound = true;
                                    for (int ii = index_date; ii < index_re; ii++)
                                    {
                                        addrs.Add(words[ii]);
                                    }
                                    if (addrs.Count < 9)
                                    {
                                        while (addrs.Count < 9)
                                        {
                                            addrs.Add("");
                                        }
                                    }

                                    addToTable(1, fileInfo.Name, "HLGS", ZipName);
                                    //rowCount++;
                                }
                                else if (words[index_re - 1].ToUpper().Contains("DEAR") || words[index_re].ToUpper().Contains("RE:")
                           || words[index_re].ToUpper().Contains("INQUIRY ID:")
                           || words[index_re].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                           || words[index_re].ToUpper().Contains("NOTICE OF DISMISSAL")
                           || words[index_re + 1].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                           )
                                {
                                    //other addrs
                                    NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = page_addrs;
                                    page_addrs = 1;
                                    addrFound = false;

                                    addrFound = true;
                                    for (int ii = index_date; ii < index_re; ii++)
                                    {
                                        addrs.Add(words[ii]);
                                    }
                                    if (addrs.Count < 9)
                                    {
                                        while (addrs.Count < 9)
                                        {
                                            addrs.Add("");
                                        }
                                    }

                                    addToTable(1, fileInfo.Name, "HLGS", ZipName);
                                    //rowCount++;
                                }
                                else if (words[index_re + 1].ToUpper().Contains("DEAR") || words[index_re].ToUpper().Contains("RE:")
                                || words[index_re].ToUpper().Contains("INQUIRY ID:")
                                || words[index_re].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                                || words[index_re].ToUpper().Contains("NOTICE OF DISMISSAL")
                                || words[index_re + 1].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                                )
                                {
                                    //other addrs
                                    NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = page_addrs;
                                    page_addrs = 1;
                                    addrFound = false;

                                    addrFound = true;
                                    for (int ii = index_date; ii < index_re; ii++)
                                    {
                                        addrs.Add(words[ii]);
                                    }
                                    if (addrs.Count < 9)
                                    {
                                        while (addrs.Count < 9)
                                        {
                                            addrs.Add("");
                                        }
                                    }

                                    addToTable(1, fileInfo.Name, "HLGS", ZipName);
                                    //rowCount++;
                                }
                                else if (words[index_re + 2].ToUpper().Contains("DEAR") || words[index_re].ToUpper().Contains("RE:")
                            || words[index_re].ToUpper().Contains("INQUIRY ID:")
                            || words[index_re].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                            || words[index_re].ToUpper().Contains("NOTICE OF DISMISSAL")
                            || words[index_re + 1].ToUpper().Contains("THIS NOTIFICATION WAS ISSUED")
                            )
                                {
                                    //other addrs
                                    NLPdfs.Rows[NLPdfs.Rows.Count - 1][3] = page_addrs;
                                    page_addrs = 1;
                                    addrFound = false;

                                    addrFound = true;
                                    for (int ii = index_date; ii < index_re; ii++)
                                    {
                                        addrs.Add(words[ii]);
                                    }
                                    if (addrs.Count < 9)
                                    {
                                        while (addrs.Count < 9)
                                        {
                                            addrs.Add("");
                                        }
                                    }

                                    addToTable(1, fileInfo.Name, "HLGS", ZipName);
                                    //rowCount++;
                                }
                                else
                                    page_addrs++;
                            }
                            catch (Exception outIndex)
                            {
                                page_addrs++;
                                //no error just short page
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

                            addToTable(1, fileInfo.Name, "HLGS", ZipName);
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
                                addrs.Add(words[ii]);
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

                            addToTable(1, fileInfo.Name, "HLGS", ZipName);
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
                addToTable(1, fileInfo.Name, "HLGS", ZipName);

                //MessageBox.Show(ex.Message);
            }
            return "";
        }

        public string evaluate_CR2_pdf(string fileName, string dest)
        {

            errorMSG = "";

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            int index_re = 0;
            string strText = string.Empty;
            try
            {
                string[] fInfo = fileInfo.Name.Split('_');
                m_JulianDate = fInfo[1].ToString();
                m_BatchID = fInfo[2].ToString().ToUpper().Replace(".PDF","");
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
                        if (CR2Pdfs.Rows.Count > 0)
                        {
                            if (page_addrs > 1)
                            {
                                CR2Pdfs.Rows[CR2Pdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
                                page_addrs = 1;
                            }
                        }
                        page_addrs = 1;

                        m_transID = m_Insert = m_TOD = m_csz = m_metadata = string.Empty;

                        string[] metaData = words[0].ToString().Split('|');
                        m_transID = metaData[1].ToString();
                        //m_Insert = metaData[5].ToString(); // (3, 4 5)  ~
                        if(metaData[3].ToString().Length > 0)
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
                        for (int i = 1; i < words.Length; i++)
                        {
                            var pattern = @",\s?[A-Za-z]{2} (\d{5}|[A-Za-z0-9]{3}\s?[A-Za-z0-9]{3})";
                            //var pattern = @"(([a-zA-Z ]+, [a-zA-z]+) ((\d{5})|([a-zA-Z]\d[a-zA-Z] ?\d[a-zA-Z]\d))?|((\d{5})|([a-zA-Z]\d[a-zA-Z] ?\d[a-zA-Z]\d)))";
                            Regex rgx = new Regex(pattern);

                            Match match = rgx.Match(words[i].ToUpper());
                            if (match.Success)
                            {
                                //if (words[i].ToUpper().Contains(", NJ") || words[i].ToUpper().Contains(", PA")
                                //            || words[i].ToUpper().Contains(", NY"))

                                m_csz = Regex.Replace(words[i].ToString(), "[^0-9A-Za-z ,]", "");
                                while (addrs.Count < 5)
                                {
                                    addrs.Add("");
                                }
                                addToTableCR2(1, fileInfo.Name, "CareRadius_2");
                                break;
                            }
                            else
                            {
                                //string tmp = Regex.Replace(words[i].ToString(), "[^0-9a-zA-Z]+", "");
                                string tmp = Regex.Replace(words[i].ToString(), "[^0-9A-Za-z ,]", "");
                                if ((tmp.IndexOf(", 2015") == -1 && tmp.IndexOf("THIS IS NOT") == -1) ||
                                        (tmp.IndexOf(", 2016") == -1 && tmp.IndexOf("THIS IS NOT") == -1))
                                addrs.Add(tmp.TrimStart().TrimEnd());
                                else
                                {
                                    m_csz = "";
                                    while (addrs.Count < 5)
                                    {
                                        addrs.Add("");
                                    }
                                    addToTableCR2(1, fileInfo.Name, "CareRadius_2");
                                    break;
                                }
                            }

                        }

                    }
                    else
                        page_addrs++;
                }

                CR2Pdfs.Rows[CR2Pdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

                reader.Close();

            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = ex.Message;
                addToTable(1, fileInfo.Name, "CareRadius_2", "");

                //MessageBox.Show(ex.Message);
            }
            return "";
        }
        public string evaluate_MBA_pdf(string fileName, string dest)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            errorMSG = "";

            
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
                        if (MBApdfs.Rows.Count > 0)
                        {
                            if (page_addrs > 1)
                            {
                                MBApdfs.Rows[MBApdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
                                page_addrs = 1;
                            }
                        }
                        page_addrs = 1;

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
                        bool boolYearFiled = false;
                        bool endAddr = false;
                        string[] limitString = new string[] { "RE:", "DEAR", "IDENTIFICATION"};
                        m_TOD = page.ToString();
                        m_metadata = words[0].ToString();
                        index_re = 0;
                        for (int i = 1; i < words.Length; i++)
                        {
                            string tmp = words[i].ToString().ToUpper();
                            if (!endAddr)
                            {
                                
                                if (tmp.Contains("/2015") || tmp.Contains("/2016") || tmp.Contains(", 2015") || tmp.Contains(", 2016"))
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
                                            addToTableMBA(1, fileInfo.Name, "MBA_SMN");
                                            boolYearFiled = false;
                                        }
                                        else
                                        {
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
                                            MBApdfs.Rows[MBApdfs.Rows.Count - 1]["artifactId"] = m_IDNumber;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                        page_addrs++;
                }

                MBApdfs.Rows[MBApdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

                reader.Close();

            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = ex.Message;
                addToTableMBA(1, fileInfo.Name, "MBA_SMN");

                //MessageBox.Show(ex.Message);
            }
            return "";
        }

        public string evaluate_SBC_pdf(string fileName, string dest)
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
                        m_IDNumber = "";
                        if (MBApdfs.Rows.Count > 0)
                        {
                            if (page_addrs > 1)
                            {
                                MBApdfs.Rows[MBApdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;
                                page_addrs = 1;
                            }
                        }
                        page_addrs = 1;

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
                        bool boolYearFiled = false;
                        
                        string[] limitString = new string[] { "RE:", "DEAR", "IDENTIFICATION" };
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
                                    m_IDNumber = tmp.Substring(posc1, 12);
                                    boolYearFiled = true;
                                }
                            }


                            if (boolYearFiled && !tmp.Contains("3HZ"))
                            {
                                bool b = limitString.Any(tmp.Contains);

                                if (b)
                                {
                                   
                                    while (addrs.Count < 5)
                                    {
                                        addrs.Add("");
                                    }
                                    addToTableSBC(1, fileInfo.Name, "SBC");
                                    boolYearFiled = false;
                                }
                                else
                                {
                                    addrs.Add(words[i].ToString().TrimStart().TrimEnd());
                                }

                            }

                        }
                    }
                    else
                        page_addrs++;
                }

                SBCpdfs.Rows[SBCpdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

                reader.Close();

            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = ex.Message;
                addToTableSBC(1, fileInfo.Name, "SBC");

                //MessageBox.Show(ex.Message);
            }
            return "";
        }

        public string finalprocess(string direcTory, string dateHLGS, string unzipName, string cycleDate, string FileType)
        {
            string processCompleted = "";
            DataView dv = NLPdfs.DefaultView;
            dv.Sort = "FileName";
            DataTable sortedPDFs = dv.ToTable();
            string prevFile = "";
            int totDoc = 0;
            int totFile = 0;
            int backupRowNumber = 0;
            for(int i=0;i<sortedPDFs.Rows.Count;i++)
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
            if (sortedPDFs.Rows.Count > 0)
            {
                string resultcsv = create_cas__csv.create_HLGS_CSV(
                                    "", sortedPDFs, FileType, Recnum, "", "", "", dateHLGS, cycleDate);   
                if (resultcsv != "")
                    processCompleted = resultcsv + "\n\n";
            }

            //DataTable working_NLPdfs = NLPdfs.Copy();
            sortedPDFs.Columns.Remove("MED_Flag");

            createCSV createcsv = new createCSV();
            //string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";  // +DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //string pNameToCASS = direcTory + "HLGS_Pdfs.csv";
            //string directoryAfterCass = ProcessVars.oNLpdfsDirectory + "FromCASS";
            string pName = direcTory + @"\" + unzipName + "_" + FileType + "_Pdfs.csv";

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
        public string finalprocessCR2(string direcTory, string dateHLGS, string unzipName, string cycleDate, string FileType, DateTime lastW)
        {
            string processCompleted = "";
            DataView dv = CR2Pdfs.DefaultView;
            dv.Sort = "FName";
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
            string pName = direcTory +  @"\" + unzipName.Replace(".pdf", "") + "_toBCC.csv";
            string BCCname = unzipName.Replace(".pdf", "") + "_toBCC.csv";
            if (sortedPDFs.Rows.Count > 0)
            {
                string resultcsv = create_cas__csv.create_CR2_CSV(
                                    unzipName, BCCname, sortedPDFs, FileType, Recnum, "", "", m_BatchID, dateHLGS, cycleDate, lastW);
                if (resultcsv != "")
                    processCompleted = resultcsv + "\n\n";
            }

            //DataTable working_NLPdfs = NLPdfs.Copy();
            sortedPDFs.Columns.Remove("MED_Flag");
            sortedPDFs.Columns.Remove("errors");
            sortedPDFs.Columns.Remove("BRE"); 
            sortedPDFs.Columns.Remove("JobClass");
            sortedPDFs.Columns.Remove("TOD");
            sortedPDFs.Columns.Remove("Metadata");
            createCSV createcsv = new createCSV();
            //string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";  // +DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //string pNameToCASS = direcTory + "HLGS_Pdfs.csv";
            //string directoryAfterCass = ProcessVars.oNLpdfsDirectory + "FromCASS";
           

            if (File.Exists(pName))
                File.Delete(pName);
           
            
            var fieldnames = new List<string>();
            fieldnames.Add("Recnum");
            fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
            fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
            fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");

            bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            foreach (DataRow row in sortedPDFs.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < sortedPDFs.Columns.Count; index++)
                {
                    if (index == 0)
                        rowData.Add(row[index].ToString());

                    else if (index == 9)
                    {
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                        rowData.Add(row[index].ToString());
                    }
                    else if (index > 9)
                        rowData.Add(row[index].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsCSV(pName, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            //copy to CASS
            string cassFileName = ProcessVars.gDMPs + BCCname;
            File.Copy(pName, cassFileName);

            return processCompleted;
        }
        public string finalprocessMBA(string direcTory, string dateHLGS, string unzipName, string cycleDate, string FileType, DateTime lastW)
        {
            string processCompleted = "";
            DataView dv = MBApdfs.DefaultView;
            dv.Sort = "FName";
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
            string pName = direcTory + @"\" + unzipName.Replace(".pdf", "") + "_toBCC.csv";
            string BCCname = unzipName.Replace(".pdf", "") + "_toBCC.csv";
            if (sortedPDFs.Rows.Count > 0)
            {
                string resultcsv = create_cas__csv.create_MBA_CSV(
                                    unzipName, BCCname, sortedPDFs, FileType, Recnum, "", "", m_BatchID, dateHLGS, cycleDate, lastW);
                if (resultcsv != "")
                    processCompleted = resultcsv + "\n\n";
            }

            //DataTable working_NLPdfs = NLPdfs.Copy();
            sortedPDFs.Columns.Remove("MED_Flag");
            sortedPDFs.Columns.Remove("errors");
            sortedPDFs.Columns.Remove("BRE");
            sortedPDFs.Columns.Remove("JobClass");
            sortedPDFs.Columns.Remove("TOD");
            sortedPDFs.Columns.Remove("Metadata");
            createCSV createcsv = new createCSV();
            //string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";  // +DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //string pNameToCASS = direcTory + "HLGS_Pdfs.csv";
            //string directoryAfterCass = ProcessVars.oNLpdfsDirectory + "FromCASS";


            if (File.Exists(pName))
                File.Delete(pName);


            var fieldnames = new List<string>();
            fieldnames.Add("Recnum");
            fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
            fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
            fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");

            bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            foreach (DataRow row in sortedPDFs.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < sortedPDFs.Columns.Count; index++)
                {
                    if (index == 0)
                        rowData.Add(row[index].ToString());

                    else if (index == 9)
                    {
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                        rowData.Add(row[index].ToString());
                    }
                    else if (index > 9)
                        rowData.Add(row[index].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsCSV(pName, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            //copy to CASS
            string cassFileName = ProcessVars.gDMPs + BCCname;
            File.Copy(pName, cassFileName);

            return processCompleted;
        }
        public string finalprocessSBC(string direcTory, string dateHLGS, string unzipName, string cycleDate, string FileType, DateTime lastW)
        {
            string processCompleted = "";
            DataView dv = SBCpdfs.DefaultView;
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
            string pName = direcTory + @"\" + unzipName.Replace(".pdf", "") + "_toBCC.csv";
            string BCCname = unzipName.Replace(".pdf", "") + "_toBCC.csv";
            if (sortedPDFs.Rows.Count > 0)
            {
                string resultcsv = create_cas__csv.create_SBC_CSV(
                                    unzipName, BCCname, sortedPDFs, FileType, Recnum, "", "", m_BatchID, dateHLGS, cycleDate, lastW);
                if (resultcsv != "")
                    processCompleted = resultcsv + "\n\n";
            }

            //DataTable working_NLPdfs = NLPdfs.Copy();
            sortedPDFs.Columns.Remove("MED_Flag");
            sortedPDFs.Columns.Remove("errors");
            sortedPDFs.Columns.Remove("BRE");
            sortedPDFs.Columns.Remove("JobClass");
            sortedPDFs.Columns.Remove("TOD");
            sortedPDFs.Columns.Remove("Metadata");
            createCSV createcsv = new createCSV();
            //string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";  // +DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //string pNameToCASS = direcTory + "HLGS_Pdfs.csv";
            //string directoryAfterCass = ProcessVars.oNLpdfsDirectory + "FromCASS";


            if (File.Exists(pName))
                File.Delete(pName);


            var fieldnames = new List<string>();
            fieldnames.Add("Recnum");
            fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
            fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
            fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");

            bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            foreach (DataRow row in sortedPDFs.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < sortedPDFs.Columns.Count; index++)
                {
                    if (index == 0)
                        rowData.Add(row[index].ToString());

                    else if (index == 9)
                    {
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                        rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                        rowData.Add(row[index].ToString());
                    }
                    else if (index > 9)
                        rowData.Add(row[index].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsCSV(pName, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            //copy to CASS
            string cassFileName = ProcessVars.gDMPs + BCCname;
            File.Copy(pName, cassFileName);

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
        public void addToTable(int currline, string fname, string jobClass, string zipname)
        {
            var test = "";
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
            row["JobClass"] = jobClass; // "HLGS";
            row["zipname"] = zipname;

            if (addrs[0].ToString().IndexOf("JESSICA YOUNG") != -1)
                test = "here";
            NLPdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            C_Recnum++;

        }
        public void addToTableCR2(int currline, string fname, string jobClass)
        {
            string test = "";
            if (m_csz.ToString().Length > 100)
                m_csz = m_csz.ToString().Substring(0, 100);
            for(int x=0; x < 5; x++)
            {
                if (addrs[x].ToString().Length > 200)
                    addrs[x] = addrs[x].ToString().Substring(0, 200);
            }
            

          
            var row = CR2Pdfs.NewRow();
            row["Recnum"] = Recnum;
            row["FName"] = fname;
            row["ImportDate"] = m_importDate;
            row["TotalP"] = totP;
            row["page_addrs"] = page_addrs;
            row["JulianDate"] = m_JulianDate;
            row["BatchID"] = m_BatchID;
            row["TransactionID"] = m_transID;
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
                row["BRE"] = m_Insert;
            }
            //row["JOBID"] = JobID;
            row["MED_Flag"] = "N";
            row["JobClass"] = jobClass; // "HLGS";
            row["TOD"] = m_TOD;
            row["Metadata"] = m_metadata;

            if (addrs[0].ToString().IndexOf("JESSICA YOUNG") != -1)
                test = "here";

            CR2Pdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            C_Recnum++;

        }
        public void addToTableMBA(int currline, string fname, string jobClass)
        {
            string test = "";
            if (m_csz.ToString().Length > 100)
                m_csz = m_csz.ToString().Substring(0, 100);
            for (int x = 0; x < 5; x++)
            {
                if (addrs[x].ToString().Length > 200)
                    addrs[x] = addrs[x].ToString().Substring(0, 200);
            }



            var row = MBApdfs.NewRow();
            row["Recnum"] = Recnum;
            row["FName"] = fname;
            row["ImportDate"] = m_importDate;
            row["TotalP"] = totP;
            row["page_addrs"] = page_addrs;
            row["JulianDate"] = m_JulianDate;
            row["BatchID"] = m_BatchID;
            row["TransactionID"] = m_transID;
            
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

            MBApdfs.Rows.Add(row);
            addrs.Clear();

            Recnum++;
            C_Recnum++;

        }
        public void addToTableSBC(int currline, string fname, string jobClass)
        {
            string test = "";
            if (m_csz.ToString().Length > 100)
                m_csz = m_csz.ToString().Substring(0, 100);
            for (int x = 0; x < 5; x++)
            {
                if (addrs[x].ToString().Length > 200)
                    addrs[x] = addrs[x].ToString().Substring(0, 200);
            }



            var row = SBCpdfs.NewRow();
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
               
                row["BRE"] = m_Insert;
              
            }
            //row["JOBID"] = JobID;
            row["MED_Flag"] = "N";
            row["JobClass"] = jobClass; // "HLGS";
            row["TOD"] = m_TOD;
            row["Metadata"] = m_metadata;

         
            SBCpdfs.Rows.Add(row);
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
            newt.Columns.Add("ZipName");

            return newt;
        }
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
        private static DataTable pdfs_Table_SBC()
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
            newt.Columns.Add("JobClass");
            newt.Columns.Add("TOD");
            newt.Columns.Add("Metadata");
            return newt;
        }
    }
}
