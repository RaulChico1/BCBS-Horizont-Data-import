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
using System.IO.Compression;
using System.Threading.Tasks;

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
        string OutputDataPath = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed";
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
                var FilesPDF4 = originalPDFs.GetFiles("SVN*.pdf").ToList();
                var FilesPDF5 = originalPDFs.GetFiles("ABL*.pdf").ToList();
                var FilesPDF6 = originalPDFs.GetFiles("OEL*.pdf").ToList();
                var FilesPDF7 = originalPDFs.GetFiles("CMC*.pdf").ToList();
                //var Fmaster = FilesPDF.Concat(FilesPDF1).Concat(FilesPDF2).ToArray();
                var Fmaster = FilesPDF.Concat(FilesPDF2).Concat(FilesPDF3).Concat(FilesPDF4).Concat(FilesPDF5).Concat(FilesPDF6).ToArray();



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
                        var fileProcessed = dbU.ExecuteScalar("select distinct FName from HOR_parse_MBA_SMN where FName ='" + filEE.Name.Replace("__", "") + "'");
                        string fileWasProcessed = "No";
                        if (fileProcessed != null)
                            {
                            fileWasProcessed = "Yes";
                            string toUpdate = "Update HOR_parse_Log_VLTrader set commentProcess = 'error file already processed' where filename like '" + filEE.Name.Replace("__", "") + "'";
                            dbU.ExecuteScalar(toUpdate);
                            File.Move(filEE.FullName, filEE.DirectoryName + "\\__error_already_processed" + filEE.Name);
                                  
                            }
                        if (filEE.Name.IndexOf("__") == -1 && fileWasProcessed == "No")
                        {
                            if (filEE.Name.Substring(0, 1) != "_")
                            {
                                try
                                {
                                    //HOR_parse_CareRadius_2

                                    dbU.ExecuteScalar("delete from HOR_parse_MBA_SMN where FName = '" + filEE.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + filEE.Name + "'");

                                    MBApdfs.Clear();
                                    string error = "";
                                    if (filEE.ToString().Contains("SMNJDL"))
                                        error = evaluate_NoDate_pdf(filEE.FullName, "");
                                    else
                                        error = evaluate_MBA_pdf(filEE.FullName, "");
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

                                            if (filEE.Name.IndexOf("CRC") == 0)
                                            {
                                                File.Copy(filEE.FullName,@"\\CIERANT-TAPER\Clients\Horizon BCBS\TEST FILES\SECURE DATA\" +  DateTime.Now.ToString("yyyy-MM-dd"));
                                                File.Copy(filEE.FullName, filEE.DirectoryName + "\\__" + filEE.Name);
                                                //File.Move(filEE.FullName, ProcessVars.OtherProcessed + filEE.Name); //  file.FullName.Replace("MBA", "__MBA"));
                                            }
                                            else
                                            {
                                                File.Move(filEE.FullName, ProcessVars.OtherProcessed + filEE.Name); //  file.FullName.Replace("MBA", "__MBA"));
                                            }
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

            }
            return errors;
        }
        public string zipFilesinDir_SBC(string dateProcess, string directory)
        {

            if (Directory.Exists(directory))
            {
                DirectoryInfo originalPDFs = new DirectoryInfo(directory);
                FileInfo[] FilesPDF = originalPDFs.GetFiles("SBC*.pdf");
                var FilesPDF2 = originalPDFs.GetFiles("CMC*.pdf").ToList();
                //var Fmaster = FilesPDF.Concat(FilesPDF1).Concat(FilesPDF2).ToArray();
                var Fmaster = FilesPDF.Concat(FilesPDF2).ToArray();
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


                    foreach (FileInfo file in Fmaster)
                    {
                        //FileInfo file = new System.IO.FileInfo(filEE.ToString());
                        if (file.Name.Substring(0, 1) != "_")
                        {
                            try
                            {
                                //HOR_parse_CareRadius_2
                                //var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_SBC where FileName = '" + file.Name + "'");
                                ////+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                //if (fileU != null)
                                //{
                                    dbU.ExecuteScalar("delete from HOR_parse_SBC where FileName = '" + file.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + file.Name + "'");
                               // }

                                SBCpdfs.Clear();
                                string error = evaluate_N_Renewalspdf(file.FullName, "");  // evaluate_SBC_pdf(file.FullName, "");
                                //createCSV createcsv = new createCSV();
                                //createcsv.printCSV_fullProcess(file.FullName.ToString().Replace(".pdf",".csv"), SBCpdfs, "", "");

                                if (error != "")
                                {
                                    errors = errors + error + "\n\n";
                                    File.Move(file.FullName, file.DirectoryName +  "\\__error_" + file.Name);

                                    string errFilename = file.DirectoryName + "\\__error_" + file.Name.Replace(".pdf", ".txt");
                                    FileStream fs1 = new FileStream(errFilename, FileMode.OpenOrCreate, FileAccess.Write);
                                    StreamWriter writer = new StreamWriter(fs1);
                                    string[] words = error.Split('~');
                                    //Text.Append(currentText);
                                    for (int i = 0; i < words.Length; i++)
                                    {
                                        writer.Write(words[i].ToString() + Environment.NewLine);
                                    }
                                    writer.Close();


                                }
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
                                        if (file.Name.IndexOf("CRC") == 0)
                                            File.Copy(file.FullName, @"\\CIERANT-TAPER\Clients\Horizon BCBS\TEST FILES\SECURE DATA\" + DateTime.Now.ToString("yyyy-MM-dd") + "\\" + file.Name);
                                        else
                                            File.Copy(file.FullName, ProcessVars.OtherProcessed + file.Name, true);

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

        static bool IsFileLocked(FileInfo file)
            {
            FileStream stream = null;
            try
                {
                stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                }
            catch (IOException)
                {
                return true;
                }
            finally
                {
                if (stream != null)
                    stream.Close();
                }
            return false;
            }
        public string zipFilesinDirService(string dateProcess, string direcTory)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string LocationZip = "";
            cycleDate = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            if (Directory.Exists(direcTory))
            {
                DirectoryInfo originalZIPs = new DirectoryInfo(direcTory);
                string unzipDirName = direcTory + "\\tmp_HLGS\\";
                if (Directory.Exists(unzipDirName))
                    Directory.Delete(unzipDirName, true);
                Directory.CreateDirectory(unzipDirName);
                foreach (FileInfo f in originalZIPs.GetFiles("HLGS*.zip"))
                    {
                    if (!IsFileLocked(f))
                        {
                        try
                            {
                            using (var archive = ZipFile.OpenRead(f.FullName))
                                {
                                foreach (var s in archive.Entries)
                                    {
                                    string path = Path.Combine(unzipDirName, s.Name);

                                    if (!Directory.Exists(path))
                                        Directory.CreateDirectory(Path.GetDirectoryName(path));

                                    s.ExtractToFile(path);
                                    }

                                }
                            }
                        catch (Exception ez)
                            {
                            errors = errors + f.Name + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                foreach (FileInfo f in originalZIPs.GetFiles("HLGS*.zip"))
                {
                    if (f.Name.IndexOf("_") == 0)
                    { //processed already
                    }
                    else
                    {
                        string strsql = "select filename from HOR_parse_HLGS where  ZipName = '" + f.Name + "'";
                        var fileExist = dbU.ExecuteScalar(strsql);
                        if (fileExist == null)
                        {
                            GlobalVar.dbaseName = "BCBS_Horizon";
                            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                            DataTable fdownloaded = dbU.ExecuteDataTable("select importDate_Start,Misc_Location from HOR_parse_files_downloaded where filename = '" + f.Name.ToString() + "'");
                            if (fdownloaded.Rows.Count > 0)
                                dateHLGS = fdownloaded.Rows[0][0].ToString();
                            else
                                dateHLGS = f.LastWriteTime.ToString("yyyy-MM-dd hh:mm:ss");  // GlobalVar.DateofProcess.ToString("yyyy-MM-dd HH:mm:ss");
                            
                            DirectoryInfo originalPDFs = new DirectoryInfo(unzipDirName);
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
                                                errors = errors + file.Name + "~";
                                        }
                                        catch (Exception ez)
                                        {
                                            errors = errors + file.FullName + "  " + ez.Message + "~";
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (NLPdfs.Rows.Count > 0 && errors  == "")
                    {
                        //finalprocess(direcTory, dateHLGS, "D" + unzipDirName, cycleDate, "HLGS");
                        finalprocess(direcTory, dateHLGS, Path.GetFileNameWithoutExtension(f.FullName), cycleDate, "HLGS");


                        DateTime dateUpload;
                        string strsql = "select importdate_start from HOR_parse_files_downloaded where filename = '" + f.Name + "'";
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
                                   DSeqnum + ",'" + f.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + f.Extension.Replace(".", "") + "',1,'" + direcTory + "','" +
                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                        }
                        else
                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);



                        string OutputpName = ProcessVars.OtherProcessed +  f.Name;

                        if (File.Exists(OutputpName))
                            File.Delete(OutputpName);


                        File.Copy(f.FullName, OutputpName);


                       File.Move(f.FullName, f.Directory + @"\__" + f.Name);
                       N_loadFromFTP ftpload = new N_loadFromFTP();
                       string ftplocation = "ftp://sftp.cierant.com//IN//";
                       string info_User = "Horizon";
                       string info_Pass = "CyRyk1al";
                       ftpload.NotDownLoadFile_just_Move(ftplocation, f.Name, info_User, info_Pass);
                    }
                    else
                    {
                        //var here = NLPdfs.Rows.Count.ToString() + "  " + errors;
                        if (errors.Length > 0)
                        {
                            string errFilename = f.Directory + "\\__error_" + f.Name.Replace(".zip", ".txt");
                            FileStream fs1 = new FileStream(errFilename, FileMode.OpenOrCreate, FileAccess.Write);
                            StreamWriter writer = new StreamWriter(fs1);
                            string[] words = errors.Split('~');
                            //Text.Append(currentText);
                            writer.Write("Error in file:  " + f.Name + Environment.NewLine);
                            writer.Write(Environment.NewLine);
                            for (int i = 0; i < words.Length; i++)
                            {
                                writer.Write(words[i].ToString() + Environment.NewLine);
                            }
                            writer.Write( Environment.NewLine);
                            writer.Write("File not processed, after correction  pls change ZIP name" + Environment.NewLine);

                            writer.Close();
                            File.Move(f.FullName, f.Directory + "\\__error_" + f.Name);
                            int DSeqnum = 0;
                            var Drecnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");

                            if (Drecnum.ToString() == "")
                                DSeqnum = 1;
                            else
                                DSeqnum = Convert.ToInt32(Drecnum.ToString()) + 1;
                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn,AfterProcessLocation) values(" +
                                  DSeqnum + ",'" + f.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + f.Extension.Replace(".", "") + "',1,'" + f.DirectoryName + "','" +
                                  DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                  DateTime.Now.ToString("yyyy-MM-dd") + "',1,'Error: " + errors.Replace("\\", "\\\\") + "')");


                        }

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
                        string strsql = "delete from HOR_parse_HLGS where CONVERT(DATE,ImportDate)= '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and filename = '" + f.Name + "'";
                        dbU.ExecuteNonQuery(strsql);

                        var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + f.Name.ToString() + "'");
                        if (fileDate == null)
                        {
                            int DSeqnum = 0;
                            var Drecnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");

                            if (Drecnum.ToString() == "")
                                DSeqnum = 1;
                            else
                                DSeqnum = Convert.ToInt32(Drecnum.ToString()) + 1;

                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                   DSeqnum + ",'" + f.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + f.Extension.Replace(".", "") + "',1,'" + direcTory + "','" +
                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");
                            dateHLGS = GlobalVar.DateofProcess.ToString("yyyy-MM-dd HH:mm:ss");
                        }
                        else
                            dateHLGS = fileDate.ToString();


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
                            string nfilename = f.Name.Replace(".pdf", "_");   //
                            finalprocess(direcTory, dateHLGS, nfilename, cycleDate, "HLGS");
                            File.Copy(f.FullName, ProcessVars.OtherProcessed + f.Name);
                            File.Move(f.FullName, f.FullName.Replace("COBA_", "__COBA_"));
                        }
                        NLPdfs.Clear();
                    }
                }



       
            }
            return errors;
        }
        public string zipFilesinDirMISC(string zipName, string direcTory, string txtDir, string txtName, string Code)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            cycleDate = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
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
            if (Directory.Exists(direcTory))
            {

                DirectoryInfo originalZIPs = new DirectoryInfo(direcTory);
                string unzipDirName = "";
                FileInfo[] FilesPDF = originalZIPs.GetFiles("*.pdf", SearchOption.AllDirectories);
                
                if (FilesPDF.Count() > 0)
                {
                    foreach (FileInfo f in originalZIPs.GetFiles("*.pdf"))
                    {

                        string strsql = "delete from HOR_parse_PDFs_Misc where Filename = '" + f.Name.ToString() +
                                        "' and ZipName = '" + zipName + "'";
                        dbU.ExecuteNonQuery(strsql);

                        var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + zipName + "'");
                        if (fileDate != null)
                            dateHLGS = fileDate.ToString();
                        else
                            dateHLGS = GlobalVar.DateofProcess.ToString("yyyy-MM-dd HH:mm:ss");
                        try
                        {
                            string error = evaluate_pdf(f.FullName, "", zipName);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + f.Name + "  " + ez.Message + "\n\n";
                        }
  
                        
                    }
                    if (NLPdfs.Rows.Count > 0)
                    {
                        finalprocess(direcTory, dateHLGS, "D" + unzipDirName, cycleDate, "PDFs_Misc~" + Code);
                        File.Move(txtDir + "\\" + txtName, txtDir + "\\__" + txtName);

                    }
                    NLPdfs.Clear();

                }
               
            }
            return errors;
        }
        public string zipFilesinDir_NoDate(string dateProcess, string directory)
        {

            if (Directory.Exists(directory))
            {
                //string[] extensions = new[] { "mba*.pdf", "sbc*.pdf", "smn*.pdf" };
                DirectoryInfo originalPDFs = new DirectoryInfo(directory);
                var FilesPDF = originalPDFs.GetFiles("PRTDEFL*.pdf").ToList();

                var FilesPDF2 = originalPDFs.GetFiles("SMNJDL*.pdf").ToList();
                //var FilesPDF3 = originalPDFs.GetFiles("PNO*.pdf").ToList();
                var FilesPDF4 = originalPDFs.GetFiles("SVN*.pdf").ToList();
                
                //var Fmaster = FilesPDF.ToArray();
                //var Fmaster = FilesPDF.Concat(FilesPDF2).ToArray();
                var Fmaster = FilesPDF.Concat(FilesPDF2).Concat(FilesPDF4).ToArray();



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
                              
                                    dbU.ExecuteScalar("delete from HOR_parse_MBA_SMN where FName = '" + filEE.Name + "'");
                                    dbU.ExecuteScalar("delete from HOR_parse_files_to_CASS where FileName = '" + filEE.Name + "'");
                              
                                MBApdfs.Clear();
                                string error = evaluate_NoDate_pdf(filEE.FullName, "");
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
                                        File.Move(filEE.FullName, ProcessVars.OtherProcessed + filEE.Name); //  file.FullName.Replace("MBA", "__MBA"));
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
            if (fileInfo.Name == "D07_0.47347072183644556.PDF" ||
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
                            if (index_re == 0)
                            {
                                string[] importString = new string[] { "RE:", "DEAR", "INQUIRY ID:", "THIS NOTIFICATION WAS ISSUED", "NOTICE OF DISMISSAL", "TO THE ESTATE", "ATTN:", "YOUR APPEAL STATUS", "NOTICE OF RIGHT" };
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
                                if (words[i].Contains("/2018") || words[i].Contains("/2016") || 
                                    words[i].Contains("/2017"))
                                {
                                    index_date = i;
                                    break;
                                }
                                else
                                {
                                    if (words[i].Contains(" 2018") || words[i].Contains(" 2016") ||
                                        words[i].Contains(" 2017"))
                                    {
                                        index_date = i;
                                        break;
                                    }
                                    else
                                    {
                                        if (words[i].Contains(" 2018") || words[i].Contains(" 2016") || 
                                            words[i].Contains(" 2017"))
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
                                    || words[index_re + 1].ToUpper().Contains("TO THE ESTATE")
                                    || words[index_re + 1].ToUpper().Contains("ATTN:")
                                    || words[index_re + 1].ToUpper().Contains("YOUR APPEAL STATUS")
                                    || words[index_re + 1].ToUpper().Contains("NOTICE OF RIGHT")
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
                                    || words[index_re + 1].ToUpper().Contains("TO THE ESTATE")
                                     || words[index_re + 1].ToUpper().Contains("ATTN:")
                                    || words[index_re + 1].ToUpper().Contains("YOUR APPEAL STATUS")
                                    || words[index_re + 1].ToUpper().Contains("NOTICE OF RIGHT")
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
                                    || words[index_re + 1].ToUpper().Contains("TO THE ESTATE")
                                     || words[index_re + 1].ToUpper().Contains("ATTN:")
                                    || words[index_re + 1].ToUpper().Contains("YOUR APPEAL STATUS")
                                    || words[index_re + 1].ToUpper().Contains("NOTICE OF RIGHT")
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
                                    || words[index_re + 1].ToUpper().Contains("TO THE ESTATE")
                                     || words[index_re + 1].ToUpper().Contains("ATTN:")
                                    || words[index_re + 1].ToUpper().Contains("YOUR APPEAL STATUS")
                                    || words[index_re + 1].ToUpper().Contains("NOTICE OF RIGHT")
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
                                if (words[ii].ToString().Length > 1)
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
                errorMSG = errorMSG + fileInfo.Name + "  " + ex.Message + Environment.NewLine;
                //addToTable(1, fileInfo.Name, "HLGS", ZipName);

                //MessageBox.Show(ex.Message);
            }
            return errorMSG;
        }

        public string evaluate_CR2_pdf(string fileName, string dest)
        {

            errorMSG = "";
            addrs.Clear();
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
                        if (m_TOD ==  "24559")
                            errorMSG = "";
                        //m_metadata = words[0].ToString();
                        StringBuilder builder = new StringBuilder();
                        int maxWords = 0;
                        foreach (string value in words)
                        {
                            builder.Append(value);
                            builder.Append('~');
                            maxWords++;
                            try
                            {
                                if (maxWords > 15)
                                    break;
                            }
                            catch (Exception)
                            { }
                        }
                        m_metadata = builder.ToString();
                        index_re = 0;
                        for (int i = 1; i < words.Length; i++)
                        {
                            if (words[i].ToString() == ",")
                                index_re = index_re;
                            if (words[i].ToString() == "CONFIRMATION NOTICE")
                                index_re = index_re;

                            var pattern = @"(\d*)\s+((?:[\w+\s*-])+)[\,]\s+([a-zA-Z]+)\s+([0-9a-zA-Z]+)";
                            //var pattern = @"^.*(\d{5}(-\d{4})?$)|(^[ABCEGHJKLMNPRSTVXY]{1}\d{1}[A-Z]{1} *\d{1}[A-Z]{1}\d{1}).*$";
                            //var pattern = @",\s?[A-Za-z]{2} (\d{5}|[A-Za-z0-9]{3}\s?[A-Za-z0-9]{3})"; //@",\s?[A-Za-z]{2} (\d{5}|[A-Za-z0-9]{3}\s?[A-Za-z0-9]{3})";
                            //var pattern = @"(([a-zA-Z ]+, [a-zA-z]+) ((\d{5})|([a-zA-Z]\d[a-zA-Z] ?\d[a-zA-Z]\d))?|((\d{5})|([a-zA-Z]\d[a-zA-Z] ?\d[a-zA-Z]\d)))";
                            Regex rgx = new Regex(pattern);

                            //if (words[i].ToString().IndexOf("GIORGIO MELONI") != -1)
                            //    index_re = index_re;
                            bool isState = false;
                            bool isnotStreet = false;
                            Match match = rgx.Match(" " + words[i].ToUpper());
                            try
                            {
                                string statee = match.Groups[3].Value;
                                isState = isStateAbbreviation(statee);
                                //isnotStreet = isStreetAbbreviation(match.Groups[2].Value);
                                if(isState)
                                {
                                    if(words[i].ToUpper().IndexOf("STREET") != -1)
                                        isState = false;
                                    else if (words[i].ToUpper().IndexOf("AVENUE") != -1)
                                        isState = false;
                                }
                            }
                            catch (Exception) { };
                            if (match.Success && isState )
                            {
                                //if (words[i].ToUpper().Contains(", NJ") || words[i].ToUpper().Contains(", PA")
                                //            || words[i].ToUpper().Contains(", NY"))
                                //string statee = match.Groups[3].Value;
                                //if (isStateAbbreviation(statee))
                                //{
                                    m_csz = Regex.Replace(words[i].ToString(), "[^0-9A-Za-z ,]", "");
                                    //addrs.Add(words[i].ToString());
                                    while (addrs.Count < 5)
                                    {
                                        addrs.Add("");
                                    }
                                    addToTableCR2(1, fileInfo.Name, "CareRadius_2");
                                    break;
                                //}
                                
                                    
                            }
                            else
                            {
                                //string tmp = Regex.Replace(words[i].ToString(), "[^0-9a-zA-Z]+", "");
                                string tmp = Regex.Replace(words[i].ToString(), "[^0-9A-Za-z ,]", "");


                                if ((tmp.IndexOf(", 2018") == -1) &&
                                        (tmp.IndexOf(", 2016") == -1) &&
                                        (tmp.IndexOf(", 2017") == -1 ) &&
                                        (tmp.IndexOf("THIS IS NOT") != 0) &&
                                        (tmp.IndexOf("CONFIRMATION NOTICE") != 0) )
                                    if (tmp != ",")
                                        addrs.Add(tmp.TrimStart().TrimEnd());
                                    else
                                    { }
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
                string testname = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Table_CR2Pdfs_" + fileInfo.Name.ToString().Replace(".pdf",".csv");
                if (File.Exists(testname))
                    File.Delete(testname);
                reader.Close();
                //foreach (DataRow row in CR2Pdfs.Rows)
                //{
                //    for (int ii = 15; ii > 0; ii--)
                //    {
                //        if (row[ii].ToString() != "")
                //        {
                //            //if (ii < 21)
                //            //    erros = "";

                //            row[15] = row[ii];
                //            row[ii] = "";
                //            break;

                //        }
                //    }
                //}



                createCSV create_cas__csv = new createCSV();
                create_cas__csv.printCSV_fullProcess(testname, CR2Pdfs, "", "");

            }
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = ex.Message;
                addToTable(1, fileInfo.Name, "CareRadius_2", "");

                //MessageBox.Show(ex.Message);
            }
            return errorMSG;
        }
        
        private static String states = "|AL|AK|AS|AZ|AR|CA|CO|CT|DE|DC|FM|FL|GA|GU|HI|ID|IL|IN|IA|KS|KY|LA|ME|MH|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|MP|OH|OK|OR|PW|PA|PR|RI|SC|SD|TN|TX|UT|VT|VI|VA|WA|WV|WI|WY|";
        private static String streets = "|STREET|AVENUE|";   //"|STREET|LANE|TERR|AVENUE|AVE|CIR|EXT|LOOP|TPKE|TRTWY|";
        public static bool isStateAbbreviation(String state)
        {
            return state.Length == 2 && states.IndexOf(state) > 0;
        }
        public static bool isStreetAbbreviation(String strStrreet)
        {
            return streets.IndexOf(strStrreet) > 0;
           //return streets.Any(s => strStrreet.Contains(s));
        }
        public string evaluate_MBA_pdf(string fileName, string dest)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            bool fABL = (fileInfo.Name.Substring(0, 3) == "ABL") ? true : false;
            errorMSG = "";
            int pagenum = 0;
            
            int index_re = 0;
            string strText = string.Empty;
            try
            {
                string[] fInfo = fileInfo.Name.Split('_');
               
                if (fInfo.Length > 1)
                {
                    m_JulianDate = fInfo[1].ToString();
                    m_BatchID = fInfo[2].ToString().ToUpper().Replace(".PDF", "");
                    m_importDate = fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss");
                }
                PdfReader reader = new PdfReader(fileName);
                totP = reader.NumberOfPages;
                for (int page = 1; page <= reader.NumberOfPages; page++)
                {
                    pagenum = page;
                    //if (page == 58284)
                    //    index_re = index_re;
                    ITextExtractionStrategy its = new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy();
                    string s = PdfTextExtractor.GetTextFromPage(reader, page, its);

                    s = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(s)));
                    
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
                        if (fABL)
                            m_Insert = "";
                        bool boolYearFiled = false;
                        bool endAddr = false;
                        string[] limitString = new string[] { "RE:", "DEAR", "IDENTIFICATION","INITIAL NOTICE","REFERENCE INFORMATION","FINAL NOTICE","YOUR APPLICATION FOR"};
                        m_TOD = page.ToString();
                        m_metadata = words[0].ToString();
                        index_re = 0;
                        for (int i = 1; i < words.Length; i++)
                        {
                            string tmp = words[i].ToString().ToUpper();
                            if (!endAddr)
                            {

                                if (tmp.Contains("/2018") || tmp.Contains("/2016") || 
                                    tmp.Contains(", 2018") || tmp.Contains(", 2016") || 
                                    tmp.Contains("/2017") || tmp.Contains(", 2017"))
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
                                            m_IDNumber = tmp.Substring(posc1, tmp.Length - posc1);
                                            if (m_IDNumber.Length > 13)
                                                m_IDNumber = m_IDNumber.Substring(0, 13).Trim();
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
                errorMSG = ex.Message + " page " + pagenum + " file " + fileInfo.Name;
                //addToTableMBA(1, fileInfo.Name, "MBA_SMN");

                //MessageBox.Show(ex.Message);
            }
            return errorMSG;
        }
        public string evaluate_NoDate_pdf(string fileName, string dest)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            errorMSG = "";
            bool isSMNJDL = false;
            if(fileInfo.Name.IndexOf("SMNJAL_17002") == 0)
            {
                errorMSG = "";
            }
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
                        isSMNJDL = false;
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
                        string[] limitString = new string[] { "RE:", "DEAR", "IDENTIFICATION" };
                        m_TOD = page.ToString();
                        m_metadata = words[0].ToString();
                        index_re = 0;
                        for (int i = 1; i < words.Length; i++)
                        {
                            string tmp = words[i].ToString().ToUpper();
                            if (!endAddr)
                            {

                                if (tmp.Contains(".COM"))
                                {
                                    boolYearFiled = true;
                                    addrs.Clear();
                                }
                                else
                                {
                                    if (fileInfo.Name.StartsWith("SMNJDL") && !isSMNJDL)
                                    {
                                        boolYearFiled = true;
                                        addrs.Clear();
                                        isSMNJDL = true;
                                    }
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
            try
            {
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

                            string[] limitString = new string[] { "DEAR", "IDENTIFICATION" };   // { "RE:", "DEAR", "IDENTIFICATION" };
                            m_TOD = page.ToString();
                            m_metadata = words[0].ToString();
                            index_re = 0;
                            for (int i = 1; i < words.Length; i++)
                            {
                                string tmp = words[i].ToString().ToUpper();
                                if ((tmp.Contains("/2018") || tmp.Contains("/2016") || 
                                    tmp.Contains(", 2018") || tmp.Contains(", 2016") || 
                                    tmp.Contains("/2017") || tmp.Contains(", 2017")) && !boolYearFiled)
                                {
                                    boolYearFiled = true;
                                    addrs.Clear();

                                }
                                if (m_IDNumber == "")
                                {
                                    if (tmp.Contains("3HZ"))
                                    {
                                        int posc1 = tmp.IndexOf("3HZ");
                                        m_IDNumber = tmp.Substring(posc1, 12);
                                        boolYearFiled = false;
                                    }
                                    //else if (tmp.Contains("RE:"))
                                    //{
                                    //    int posc1 = tmp.IndexOf("RE:");
                                    //    m_IDNumber = tmp.Substring(posc1 + 3, tmp.Length - 3);
                                    //    boolYearFiled = true;
                                    //}
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
                    if (ex.Message.IndexOf("trailer") != -1)
                    { }
                    else
                        addToTableSBC(1, fileInfo.Name, "SBC");

                    //MessageBox.Show(ex.Message);
                }
            }
            catch (Exception ex2)
            {
                errorMSG = ex2.Message;
            }
            return errorMSG;
        }

        public string evaluate_N_Renewalspdf(string fileName, string dest)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            bool fPNO = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            errorMSG = "";
             m_JulianDate = "";
                    m_BatchID =  "";
                    m_importDate = fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss");
                    m_csz = "";
            int index_re = 0;
            string strText = string.Empty;
            try
            {
                string[] fInfo = fileInfo.Name.Split('_');
                if (fInfo.Length > 1)
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

                    }
                    page_addrs = 1;
                    bool boolYearFiled = false;
                    bool endAddr = false;
                    string[] limitString = new string[] { "IMPORTANT INFORMATION", "DEAR ", "IMPORTANT:", "RE:" };   //  , "RE:" 
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
                                tmp = "";
                            }
                        }

                        if (!endAddr)
                        {

                            if ((tmp.Contains("/2018") || tmp.Contains("/2016") || 
                                tmp.Contains(", 2018") || tmp.Contains(", 2016") || 
                                tmp.Contains("/2017") || tmp.Contains(", 2017")) && !boolYearFiled)
                            {
                                if (i < 15 && tmp.ToUpper().IndexOf("COVERAGE") == -1 && tmp.ToUpper().IndexOf(".COM") == -1)
                                {
                                    boolYearFiled = true;
                                    addrs.Clear();
                                }
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

                                        addToTableSBC(1, fileInfo.Name, "SBC");
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
                                        MBApdfs.Rows[MBApdfs.Rows.Count - 1]["artifactId"] = m_IDNumber;
                                    }
                                }
                            }
                        }
                    }
                    //}
                    //else
                    page_addrs++;
                }

                reader.Close();
            }
                
            catch (Exception ex)
            {
                errorcount++;
                errorMSG = errorMSG + ex.Message;
                addToTableSBC(1, fileInfo.Name, "SBC");

                //MessageBox.Show(ex.Message);
            }
            return errorMSG;
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
            if (File.Exists(ProcessVars.OtherProcessed + unzipName + "_" + FileType + "_Pdfs.csv"))
                File.Delete(ProcessVars.OtherProcessed + unzipName + "_" + FileType + "_Pdfs.csv");
                File.Copy(pName, ProcessVars.OtherProcessed + unzipName + "_" + FileType + "_Pdfs.csv");

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
            string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var t = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t.Wait();

            string time2 = t.Result;

            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");
            BackCASS processRedturns = new BackCASS();
            string ResultsBack_CASS = processRedturns.ProcessFiles("CareRadius_2");

            return processCompleted + Environment.NewLine + ResultsBack_CASS;
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

            var tM = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            tM.Wait();
            string returnBCC = unzipName.Replace(".pdf", "") + "_toBCC-OUTPUT.csv";
            string dirBcc = ProcessVars.gODMPs;
            if (!File.Exists(dirBcc + returnBCC))
                {
                    processCompleted = "BCC file not present";
                }
            BackCASS processRedturnsM = new BackCASS();
            string ResultsBack_CASSM = processRedturnsM.ProcessFiles(unzipName.Replace(".pdf", ""));   //"MBA_SMN"   (FileType

            return processCompleted + Environment.NewLine + ResultsBack_CASSM;
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

            string time1S = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var tS = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            tS.Wait();


            BackCASS processRedturnsS = new BackCASS();
            string ResultsBack_CASS_S = processRedturnsS.ProcessFiles("SBC");

            return processCompleted;
        }

        public string finalprocessOEL(string direcTory, string dateHLGS, string unzipName, string cycleDate, string FileType, DateTime lastW)
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

            string time1S = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var tS = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            tS.Wait();


            BackCASS processRedturnsS = new BackCASS();
            string ResultsBack_CASS_S = processRedturnsS.ProcessFiles("OEL");

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
