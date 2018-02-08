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

namespace Horizon_EOBS_Parse
{
    
    public class N_loadFromFTP
    {
        DBUtility dbU;
        int Seqnum = 1;
        int updErrors = 0;
        private FtpWebRequest ftpRequest = null;
        private FtpWebResponse ftpResponse = null;
        private Stream ftpStream = null;
        private int bufferSize = 2048;
        string OutputDataPath = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed";
       
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
        public void MoveFilesFrom_VLTrader()
            {
            string verRuning = "";
            if (ProcessVars.gTest)
                verRuning = "TEST";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            string source = @"\\criticalapps\Horizon\fromVLTrader" + verRuning;
            string NotProcess = source + @"\D_not processed Raul\";
            string done = source + @"\Done\";


            SqlParameter[] sqlParams;
            var directory = new DirectoryInfo(source);
            var masks = new[] { "*" };
            var files = masks.SelectMany(directory.EnumerateFiles);
            string errmsg = "";
            string fileMoveto = "";
            foreach (var fileName in files)
                {
                FileInfo fInfo = new FileInfo(fileName.FullName);
                if (!IsFileLocked(fInfo))
                    {
                    if (fileName.Name.Substring(0, 2) != "__")
                        {
                        string justFname = fInfo.Name.Substring(0, (fInfo.Name.ToString().Length) - Path.GetExtension(fInfo.Name).Length);
                        string extension = Path.GetExtension(fInfo.Name).Replace(".", "");
                        int posc = fInfo.Name.IndexOf("_");

                        string fCode = fInfo.Name.Substring(0, posc);
                        sqlParams = new SqlParameter[] { new SqlParameter("@Filecode", fCode),
                                                 new SqlParameter("@Filename", justFname),
                                                 new SqlParameter("@formatFile", extension)};   //

                        var result = dbU.ExecuteScalar("HOR_check_uploaded", sqlParams);

                        try
                            {
                            string res = result.ToString();
                            if (res == "NO definition")
                                {
                                string errFname = fInfo.Directory + "\\__err_" + justFname + ".txt";
                                FileStream fs1 = new FileStream(errFname, FileMode.OpenOrCreate, FileAccess.Write);
                                StreamWriter writer = new StreamWriter(fs1);
                                writer.Write("NO definition for file " + fCode + "  extension: " + extension + Environment.NewLine);
                                writer.Write("In Table  HOR_parse_N_Category_Master" + Environment.NewLine);
                                writer.Close();
                                File.Move(fInfo.FullName, fInfo.Directory + @"\__err_" + fileName.Name);
                                }
                            else if (res == "Already processed")
                                {
                               
                                File.Move(fInfo.FullName, fInfo.Directory + @"\__err_already_processed_" + fileName.Name);
                                sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                                                         new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                                                         new SqlParameter("@Tlocation", fileMoveto),
                                                         new SqlParameter("@MoveError", "Already processed")};   //

                                dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);

                                }
                            else if (res == "ok to process")
                                {
                                try
                                    {
                                    var varMoveTo = dbU.ExecuteScalar("select status + '' from HOR_parse_N_Category_Master where code = '" + fCode + "' and fileExtension ='" + extension + "'");
                                    if (varMoveTo.ToString() == "No-Parse")
                                        {
                                        fileMoveto = NotProcess;
                                        File.Move(fileName.FullName, NotProcess + fileName.Name);
                                       
                                        }
                                    else
                                        {
                                        fileMoveto = ProcessVars.InputDirectory + @"from_FTP\";
                                        File.Copy(fileName.FullName, ProcessVars.InputDirectory + @"from_FTP\" + fileName.Name);
                                        File.Move(fileName.FullName, done + fileName.Name);
                                        }                                    
                                    

                                    sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                                                         new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                                                         new SqlParameter("@Tlocation", fileMoveto),
                                                         new SqlParameter("@MoveError", "")};   //

                                    dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);
                                    }
                                catch (Exception ex)
                                    {
                                    
                                    if (ex.Message.Length > 250)
                                        errmsg = ex.Message.Substring(0, 250);
                                    else
                                        errmsg = ex.Message.Substring(0, ex.Message.Length - 2);

                                    File.Move(fileName.FullName, fileName.Directory + @"\__error_" +  fileName.Name);

                                    sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                                                         new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                                                         new SqlParameter("@Tlocation", fileMoveto),
                                                         new SqlParameter("@MoveError", errmsg)};   //
                                    dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);
                                    }
                                }
                            else
                                {
                                File.Move(fileName.FullName, fileName.Directory + @"\__error_No return SP_" + fileName.Name);

                                sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                                                         new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                                                         new SqlParameter("@Tlocation", fileMoveto),
                                                         new SqlParameter("@MoveError", "No return from SP')};   //
                                dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);
                                }
                            }

                        //if (noProcess.Contains(fileName.Name.Substring(0, 6)))
                        //    {
                        //    File.Move(fileName.FullName, NotProcess + fileName.Name);

                        //    sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                        //                     new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                        //                     new SqlParameter("@Tlocation", NotProcess),
                        //                     new SqlParameter("@MoveError", "")};   //

                        //    dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);

                        //    }
                        //else
                        //    {
                        //    File.Copy(fileName.FullName, ProcessVars.InputDirectory + @"from_FTP\" + fileName.Name);
                        //    File.Move(fileName.FullName, done + fileName.Name);

                        //    sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                        //                     new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                        //                     new SqlParameter("@Tlocation", ProcessVars.InputDirectory + @"from_FTP\"),
                        //                     new SqlParameter("@MoveError", "")};   //

                        //    dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);
                        //    }


                        catch (Exception ex)
                            {
                            //erro not defined

                            File.Move(fInfo.FullName, fInfo.Directory + @"\__err_" + fileName.Name);


                            if (ex.Message.Length > 250)
                                errmsg = ex.Message.Substring(0, 250);
                            else
                                errmsg = ex.Message.Substring(0, ex.Message.Length - 2);

                            File.Move(fileName.FullName, fileName.Directory + @"\__error_" + fileName.Name);

                            sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                                                         new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                                                         new SqlParameter("@Tlocation", fileMoveto),
                                                         new SqlParameter("@MoveError", errmsg)};   //
                            dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);

                            }
                        
                        //if (ex.Message.Length > 250)
                        //    errmsg = ex.Message.Substring(0, 250);
                        //else
                        //    errmsg = ex.Message.Substring(0, ex.Message.Length-2);
                        //sqlParams = new SqlParameter[] { new SqlParameter("@Fname", fileName.Name),
                        //                     new SqlParameter("@Flocation", fileName.DirectoryName.ToString()),
                        //                     new SqlParameter("@Tlocation", ProcessVars.InputDirectory + @"from_FTP\"),
                        //                     new SqlParameter("@MoveError", errmsg)};   //

                        //dbU.ExecuteScalar("HOR_upd_Log_VLTrader", sqlParams);
                        //File.Move(fileName.FullName, fileName.DirectoryName + "\\__error moving__" + fileName.Name);

                        }
                    }
                }
            }
        public string downloadData(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles(dateProcess, false, "Ticket2");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string downloadDataC(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles(dateProcess, false, "Ticket2Control");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string downloadDataPriority(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles(dateProcess, false, "Ticket2Priority");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string downloadDataP(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles_PLANS(dateProcess, false, "PLANS");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string downloadData_MFT(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles_MFT(dateProcess, false, "PLANS");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string downloadFiles_MFT(DateTime dateProcess, bool Ticket1, string option)
        {
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport(option);
            // var miscToExpand = appsets.getFilesMisc();
            // clean files 0 in if file was not downloaded manually
            DataTable manuallyDownloaded = dbU.ExecuteDataTable("select FileName from HOR_parse_files_downloaded where FilesIn = 0");
            foreach (DataRow row in manuallyDownloaded.Rows)
            {
                //string fname = row["filename"].ToString();
                //if (!File.Exists(ProcessVars.InputDirectory + @"from_FTP\" + fname))
                //    dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 and filename = '" + fname + "'");
            }
            //dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 ");
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;



            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.Domain = "CierantEmployees";
            info.User = "rchico";
            info.Pass = "CTCierant2017";
            info.Host = "ftp://sftp.files.cierant.com";
            int steps = 0;
            //string tarFiles = "filesautomaticallyconverted";
            string ftpSubDir = "sftp.files.cierant.com/VLTraderInbox/";
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/VLTraderInbox"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass,info.Domain);

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            DateTime lastModifiedDate = response.LastModified;


            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            string NetworkDIR = ProcessVars.networkDir + GlobalVar.DateofProcess.ToString("yyyy_MMMM") + "\\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"_test\";

            //System.IO.Directory.CreateDirectory(NetworkDIR);

            //DirectoryInfo dirLocal = new DirectoryInfo(ProcessVars.InputDirectory + @"from_FTP");
            string DirLocal = "";
            string listFiles = "";
            int countFiles = 0;
            try
            {
                while (!reader.EndOfStream)
                {

                    string filename = reader.ReadLine().ToString();
                    if (filename.ToString().Contains("ONEXRN_17287_27596"))
                        DirLocal = DirLocal;
                    var result = fileToImport.FirstOrDefault(s => filename.Contains(s.Name));
                    if (result != null)
                    {
                        if (result.Name != null)
                        {
                            int compareValue;
                            int filesInzip = 0;
                            SessionOptions sessionOptions = new SessionOptions
                            {
                                Protocol = Protocol.Sftp,
                                HostName = "sftp.cierant.com",
                                UserName = info.User,
                                Password = info.Pass,
                                PortNumber = 22,

                                SshHostKeyFingerprint = "ssh-rsa 2048 a7:9d:68:84:88:ec:e4:e1:6d:09:9b:0f:b5:20:b1:7a"
                            };
                            using (Session session = new Session())
                            {
                                session.Open(sessionOptions); //Attempt to connect to sFtp site

                                //Get Ftp File Info
                                TransferOptions transferOptions = new TransferOptions();
                                transferOptions.TransferMode = TransferMode.Binary;

                                RemoteFileInfo FileInfo = session.GetFileInfo("/HorizonBCBS/" + filename);
                                lastModifiedDate = FileInfo.LastWriteTime;
                                string ZipfileName = FileInfo.Name;
                                System.Int64 Long = FileInfo.Length;
                                //object[] Return = { ZipfileName, LastWriteTime };
                            }
                            //compareValue = lastModifiedDate.CompareTo((DateTime.Today.AddDays(-1)));

                            compareValue = lastModifiedDate.CompareTo((dateProcess));
                            //responseD.Close();
                            var lastDay = lastModifiedDate.ToShortDateString();
                            // check if is today
                            string resultDownload = "";
                            DataTable pdfsInXML = new DataTable();

                            pdfsInXML.Columns.Add("filename", typeof(String));

                            if (compareValue == 1 || compareValue != 1)
                            {
                                if ((Ticket1 && filename.IndexOf("filesautomaticallyconverted") != -1) ||
                                    (!Ticket1))
                                {
                                    //check if already uploaded
                                    listFiles = listFiles + filename.Replace("IN/", "") + "____";
                                    countFiles++;
                                    var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename.Replace("IN/", "") + "'");
                                    //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                    if (fileU != null)
                                    { }
                                    else
                                    {
                                        DirLocal = ProcessVars.InputDirectory + @"Renewals";
                                        string ext = result.Ext;

                                        try
                                        {
                                            string downlResult = "0";
                                            //int totTry = 0;
                                            //do 
                                            //{
                                            resultDownload = DownLoadFiles(info.Host + "/" + filename, filename.Replace("IN/", ""), DirLocal, info.User, info.Pass);
                                            if (resultDownload.ToString() == "")
                                                downlResult = "1";
                                            else
                                                error = error + filename.Replace("IN/", "") + Environment.NewLine;
                                            LogWriter logerror = new LogWriter();
                                            logerror.WriteLogToTable("file downloaded", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", info.Host + "/" + filename + " " + filesInzip, "email", resultDownload);
                                            //} while (downlResult == "0" && totTry < 2);
                                            Seqnum++;
                                        }
                                        catch (Exception ex)
                                        {
                                            LogWriter logerror = new LogWriter();
                                            error = error + ex.Message + Environment.NewLine;
                                            logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);
                                        }



                                        if (ext.ToUpper() == "PDF")
                                        {
                                            //Dir = ProcessVars.InputDirectory + @"from_FTP";

                                            // check date
                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                                        }
                                    }
                                    //other
                                }
                            }
                        }

                    }
                    else
                        filestoImport = filestoImport + "\n\n" + filename;

                }
            }
            catch (Exception ex)
            {
                var exception = ex.Message;
            }
            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);
            if (error != "")
                return "Download manually :  " + error;
            else
                return "";
        }
        public string downloadDataMRDF(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFilesMRDF(dateProcess, "MRDF");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string FileNamesFtp(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = ftp_CheckFiles(dateProcess, false, "Ticket2");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string downloadDataTicket01(DateTime dateProcess, string option)
        {
            //var t = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 40);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //t.Wait();

            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles(dateProcess, true, option);
            //ProcessVars.InputDirectory + @"\from_FTP"

            return "";
        }
        public string uploadftpTEST(string remoteFile, string localFile)
        {
            string valReturn = "upload ok";
            string subDir = "/Usr/CaptainCrunch/CaptainCrunch/Bills/";
            string user = "CaptainCrunch";
            string pass = "Fr00tL00ps!";  // "C4pt4iN!336";  // "C!h3cks@Sci374";     //"H3althyBr3akfastN0t!";
            string host = "ftp://ftp.sciimage.com";
            //string host = "ftp://ftp.sciimageftp.com";
            //string host = "ftp://24.157.51.68/";
           
                try
                {
                    //"/Bills/"
                    /* Create an FTP Request */
                    ftpRequest = (FtpWebRequest)FtpWebRequest.Create(host + subDir + remoteFile);
                    /* Log in to the FTP Server with the User Name and Password Provided */
                    ftpRequest.Credentials = new NetworkCredential(user, pass);
                    /* When in doubt, use these options */
                    ftpRequest.UseBinary = true;
                    ftpRequest.UsePassive = true;
                    ftpRequest.KeepAlive = true;
                    /* Specify the Type of FTP Request */
                    ftpRequest.Method = WebRequestMethods.Ftp.UploadFile;
                    /* Establish Return Communication with the FTP Server */
                    ftpStream = ftpRequest.GetRequestStream();
                    /* Open a File Stream to Read the File for Upload */
                    FileStream localFileStream = new FileStream(localFile, FileMode.Open);
                    /* Buffer for the Downloaded Data */
                    byte[] byteBuffer = new byte[bufferSize];
                    int bytesSent = localFileStream.Read(byteBuffer, 0, bufferSize);
                    /* Upload the File by Sending the Buffered Data Until the Transfer is Complete */
                    try
                    {
                        while (bytesSent != 0)
                        {
                            ftpStream.Write(byteBuffer, 0, bytesSent);
                            bytesSent = localFileStream.Read(byteBuffer, 0, bufferSize);
                        }
                    }
                    catch (Exception ex) { valReturn = ex.Message; }
                    /* Resource Cleanup */
                    localFileStream.Close();
                    ftpStream.Close();
                    ftpRequest = null;
                }
                catch (Exception ex) { 
                    valReturn = ex.Message; 
                }
            
            return "Test";
        }
        public string uploadftp(string remoteFile, string localFile, int totfilesin, string subDir, int totTXT, int TotCSV)
        {
            
            string valReturn = "upload ok";
            if(totfilesin == 0)
                valReturn = "Not uploaded";
            string user = "CaptainCrunch";
            string pass = "Fr00tL00ps!"; //"C!h3cks@Sci374";     // "H3althyBr3akfastN0t!";
            //string host = "ftp://ftp.sciimage.com";
            string host = "ftp://ftp.sciimageftp.com";
            //string host = "ftp://24.157.51.68/";
            if (totfilesin != 0)
            {
                try
                {
                    //"/Bills/"
                    /* Create an FTP Request */
                    ftpRequest = (FtpWebRequest)FtpWebRequest.Create(host + subDir + remoteFile);
                    /* Log in to the FTP Server with the User Name and Password Provided */
                    ftpRequest.Credentials = new NetworkCredential(user, pass);
                    /* When in doubt, use these options */
                    ftpRequest.UseBinary = true;
                    ftpRequest.UsePassive = true;
                    ftpRequest.KeepAlive = true;
                    /* Specify the Type of FTP Request */
                    ftpRequest.Method = WebRequestMethods.Ftp.UploadFile;
                    /* Establish Return Communication with the FTP Server */
                    ftpStream = ftpRequest.GetRequestStream();
                    /* Open a File Stream to Read the File for Upload */
                    FileStream localFileStream = new FileStream(localFile, FileMode.Open);
                    /* Buffer for the Downloaded Data */
                    byte[] byteBuffer = new byte[bufferSize];
                    int bytesSent = localFileStream.Read(byteBuffer, 0, bufferSize);
                    /* Upload the File by Sending the Buffered Data Until the Transfer is Complete */
                    try
                    {
                        while (bytesSent != 0)
                        {
                            ftpStream.Write(byteBuffer, 0, bytesSent);
                            bytesSent = localFileStream.Read(byteBuffer, 0, bufferSize);
                        }
                    }
                    catch (Exception ex) { valReturn = ex.Message; }
                    /* Resource Cleanup */
                    localFileStream.Close();
                    ftpStream.Close();
                    ftpRequest = null;
                }
                catch (Exception ex) { 
                    valReturn = ex.Message; 
                }
            }
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_uploaded");
           
            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;

            dbU.ExecuteScalar("Insert into HOR_parse_files_uploaded(SeqNum, FileName, FromLocation,ImportDate_Start,FilesIn,ftpsite,result,Txts,CSVs) values(" +
                                         Seqnum + ",'" + remoteFile + "','" + localFile + "',GETDATE(), " + totfilesin + ",'" + host + "','" + valReturn + "'," + totTXT + "," + TotCSV + ")");
            


            return valReturn;
        }
        public string downloadFilesMRDF(DateTime dateProcess, string option)
        {
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport(option);
            DataTable manuallyDownloaded = dbU.ExecuteDataTable("select FileName from HOR_parse_files_downloaded where FilesIn = 0");
            foreach (DataRow row in manuallyDownloaded.Rows)
            {
                string fname = row["filename"].ToString();
                if (!File.Exists(ProcessVars.InputDirectory + @"from_FTP\" + fname))
                    dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 and filename = '" + fname + "'");
            }
            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "CaptainCrunch";
            info.Pass = "Fr00tL00ps!";  //"H3althyBr3akfastN0t!";
            info.Host = "ftp://sftp.cierant.com";  //

            int steps = 0;

            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass);

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            DateTime lastModifiedDate = response.LastModified;


            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            string NetworkDIR = ProcessVars.networkDir + GlobalVar.DateofProcess.ToString("yyyy_MMMM") + "\\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"_test\";

    
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;

            string DirLocal = "";
            string listFiles = "";
            int countFiles = 0;
            while (!reader.EndOfStream)
            {
                string filename = reader.ReadLine().ToString();
                //if (filename == "IN/CON2_20151123_NSR_NASCO_HIX_PROCESSED.zip")
                //    listFiles = listFiles;
                var result = fileToImport.FirstOrDefault(s => filename.Contains(s.Ext));
                if (result != null)
                {
                    //check if already uploaded
                    var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename + "'");
                    //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                    if (fileU != null)
                    { }
                    else
                    {
                        int compareValue;
                        int filesInzip = 0;
                        SessionOptions sessionOptions = new SessionOptions
                        {
                            Protocol = Protocol.Sftp,
                            HostName = "sftp.cierant.com",
                            UserName = info.User,
                            Password = info.Pass,
                            PortNumber = 22,

                            SshHostKeyFingerprint = "ssh-rsa 2048 a7:9d:68:84:88:ec:e4:e1:6d:09:9b:0f:b5:20:b1:7a"
                        };
                        using (Session session = new Session())
                        {
                            session.Open(sessionOptions); //Attempt to connect to sFtp site

                            //Get Ftp File Info
                            TransferOptions transferOptions = new TransferOptions();
                            transferOptions.TransferMode = TransferMode.Binary;

                            RemoteFileInfo FileInfo = session.GetFileInfo("/HOR_AutoFlow/" + filename);

                            lastModifiedDate = FileInfo.LastWriteTime;
                            string ZipfileName = FileInfo.Name;
                            System.Int64 Long = FileInfo.Length;
                            //object[] Return = { ZipfileName, LastWriteTime };
                        }
                        //compareValue = lastModifiedDate.CompareTo("8/31/2016 9:10:07");
                        compareValue = lastModifiedDate.CompareTo((dateProcess.AddDays(-1)));
                        int totfiles = 1;
                        compareValue = 1;
                        string relationship = "";
                        if (compareValue == 0)
                        {
                        }
                        //if (compareValue < 0)
                        //    relationship = "is earlier than";
                        //else if (compareValue == 0)
                        //    relationship = "is the same time as";
                        else
                        {
                            relationship = "is later than";
                            if (!Directory.Exists(ProcessVars.InputDirectory + "MRDF"))
                                Directory.CreateDirectory(ProcessVars.InputDirectory + "MRDF");
                            listFiles = listFiles + filename.Replace("IN/", "") + "____";
                            countFiles++;
                            DirLocal = ProcessVars.InputDirectory + @"MRDF";
                            string ext = result.Ext;

                            try
                            {
                                //string resultDownload = DownLoadFiles(info.Host + "/" + filename, filename, DirLocal, info.User, info.Pass);
                                string resultDownload = DownLoadFilesAndMove(info.Host + "/" + filename, filename, DirLocal, info.User, info.Pass);

                                if (resultDownload == "")
                                    totfiles = 1;
                                else
                                    totfiles = 0;   // some error downloading
                                Seqnum++;
                            }
                            catch (Exception ex)
                            {
                                LogWriter logerror = new LogWriter();
                                error = ex.Message;
                                logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);
                            }
                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                DateTime.Now.ToString("yyyy-MM-dd") + "'," + totfiles.ToString() + ")");
                        }
                    }

                }
                //else
                //    filestoImport = filestoImport + "\n\n" + filename;
            }


            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);

            return "";
        }
        public string downloadFiles_PLANS(DateTime dateProcess, bool Ticket1, string option)
        {
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport(option);
           // var miscToExpand = appsets.getFilesMisc();
            // clean files 0 in if file was not downloaded manually
            DataTable manuallyDownloaded = dbU.ExecuteDataTable("select FileName from HOR_parse_files_downloaded where FilesIn = 0");
            foreach (DataRow row in manuallyDownloaded.Rows)
            {
                //string fname = row["filename"].ToString();
                //if (!File.Exists(ProcessVars.InputDirectory + @"from_FTP\" + fname))
                //    dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 and filename = '" + fname + "'");
            }
            //dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 ");
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;



            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "Horizon";
            info.Pass = "CyRyk1al";
            info.Host = "ftp://sftp.cierant.com";
            int steps = 0;
            //string tarFiles = "filesautomaticallyconverted";
            string ftpSubDir = "sftp.cierant.com/HorizonBCBS/IN/";
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/IN"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass);

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            DateTime lastModifiedDate = response.LastModified;


            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            string NetworkDIR = ProcessVars.networkDir + GlobalVar.DateofProcess.ToString("yyyy_MMMM") + "\\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"_test\";

            //System.IO.Directory.CreateDirectory(NetworkDIR);

            //DirectoryInfo dirLocal = new DirectoryInfo(ProcessVars.InputDirectory + @"from_FTP");
            string DirLocal = "";
            string listFiles = "";
            int countFiles = 0;
            try
            {
                while (!reader.EndOfStream)
                {

                    string filename = reader.ReadLine().ToString();
                    if (filename.ToString().Contains("ONEXRN_17287_27596"))
                        DirLocal = DirLocal;
                    var result = fileToImport.FirstOrDefault(s => filename.Contains(s.Name));
                    if (result != null)
                    {
                        if (result.Name != null)
                        {
                            int compareValue;
                            int filesInzip = 0;
                            SessionOptions sessionOptions = new SessionOptions
                            {
                                Protocol = Protocol.Sftp,
                                HostName = "sftp.cierant.com",
                                UserName = info.User,
                                Password = info.Pass,
                                PortNumber = 22,

                                SshHostKeyFingerprint = "ssh-rsa 2048 a7:9d:68:84:88:ec:e4:e1:6d:09:9b:0f:b5:20:b1:7a"
                            };
                            using (Session session = new Session())
                            {
                                session.Open(sessionOptions); //Attempt to connect to sFtp site

                                //Get Ftp File Info
                                TransferOptions transferOptions = new TransferOptions();
                                transferOptions.TransferMode = TransferMode.Binary;

                                RemoteFileInfo FileInfo = session.GetFileInfo("/HorizonBCBS/" + filename);
                                lastModifiedDate = FileInfo.LastWriteTime;
                                string ZipfileName = FileInfo.Name;
                                System.Int64 Long = FileInfo.Length;
                                //object[] Return = { ZipfileName, LastWriteTime };
                            }
                            //compareValue = lastModifiedDate.CompareTo((DateTime.Today.AddDays(-1)));
                           
                                compareValue = lastModifiedDate.CompareTo((dateProcess));
                            //responseD.Close();
                            var lastDay = lastModifiedDate.ToShortDateString();
                            // check if is today
                            string resultDownload = "";
                            DataTable pdfsInXML = new DataTable();

                            pdfsInXML.Columns.Add("filename", typeof(String));

                            if (compareValue == 1 || compareValue != 1)
                            {
                                if ((Ticket1 && filename.IndexOf("filesautomaticallyconverted") != -1) ||
                                    (!Ticket1))
                                {
                                    //check if already uploaded
                                    listFiles = listFiles + filename.Replace("IN/", "") + "____";
                                    countFiles++;
                                    var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename.Replace("IN/", "") + "'");
                                    //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                    if (fileU != null)
                                    { }
                                    else
                                    {
                                        DirLocal = ProcessVars.InputDirectory + @"Renewals";
                                        string ext = result.Ext;

                                        try
                                        {
                                            string downlResult = "0";
                                            //int totTry = 0;
                                            //do 
                                            //{
                                            resultDownload = DownLoadFiles(info.Host + "/" + filename, filename.Replace("IN/", ""), DirLocal, info.User, info.Pass);
                                            if (resultDownload.ToString() == "")
                                                downlResult = "1";
                                            else
                                                error = error + filename.Replace("IN/", "") + Environment.NewLine;
                                            LogWriter logerror = new LogWriter();
                                            logerror.WriteLogToTable("file downloaded", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", info.Host + "/" + filename + " " + filesInzip, "email", resultDownload);
                                            //} while (downlResult == "0" && totTry < 2);
                                            Seqnum++;
                                        }
                                        catch (Exception ex)
                                        {
                                            LogWriter logerror = new LogWriter();
                                            error = error + ex.Message + Environment.NewLine;
                                            logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);
                                        }

                                       
                                      
                                        if (ext.ToUpper() == "PDF")
                                        {
                                            //Dir = ProcessVars.InputDirectory + @"from_FTP";

                                            // check date
                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                                        }
                                    }
                                    //other
                                }
                            }
                        }

                    }
                    else
                        filestoImport = filestoImport + "\n\n" + filename;

                }
            }
            catch (Exception ex)
            {
                var exception = ex.Message;
            }
            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);
            if (error != "")
                return "Download manually :  " + error;
            else
                return "";
        }
        public string downloadFiles(DateTime dateProcess, bool Ticket1, string option)
        {
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport(option);
            var miscToExpand = appsets.getFilesMisc();


            // clean files 0 in if file was not downloaded manually
            DataTable manuallyDownloaded = dbU.ExecuteDataTable("select FileName from HOR_parse_files_downloaded where FilesIn = 0");
            foreach (DataRow row in manuallyDownloaded.Rows)
            {
                string fname = row["filename"].ToString();
                if (!File.Exists(ProcessVars.InputDirectory + @"from_FTP\" + fname))
                    dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 and filename = '" + fname + "'");
            }
            //dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 ");
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;



            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "Horizon";
            info.Pass = "CyRyk1al";
            info.Host = "ftp://sftp.cierant.com";
            int steps = 0;
            //string tarFiles = "filesautomaticallyconverted";
            string ftpSubDir = "sftp.cierant.com/HorizonBCBS/IN/";
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/IN"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass);
            string listFiles = "";
            int countFiles = 0;
            try
            {
            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            DateTime lastModifiedDate = response.LastModified;
            
         
                Stream responseStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(responseStream);
                string NetworkDIR = ProcessVars.networkDir + GlobalVar.DateofProcess.ToString("yyyy_MMMM") + "\\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"_test\";

                //System.IO.Directory.CreateDirectory(NetworkDIR);

                //DirectoryInfo dirLocal = new DirectoryInfo(ProcessVars.InputDirectory + @"from_FTP");
                string DirLocal = "";
              
                try
                {
                    while (!reader.EndOfStream)
                    {

                        string filename = reader.ReadLine().ToString();
                        if (filename.ToString().Contains("SVN") || filename.ToString().ToUpper().Contains(".XLS")
                            || filename.ToString().ToUpper().Contains(".XLSX"))
                            DirLocal = DirLocal;
                        var result = fileToImport.FirstOrDefault(s => filename.Contains(s.Name));
                        if (filename.Contains("CIE_"))
                            try
                                {
                                result.Name = null;
                                }
                            catch
                                {

                                }

                        if (result != null)
                        {
                            if (result.Name != null)
                            {
                                int filesInzip = 0;

                                string resultDownload = "";
                                DataTable pdfsInXML = new DataTable();
                                string downlResult = "0";
                                pdfsInXML.Columns.Add("filename", typeof(String));

                                if (filename.IndexOf("SVN") != -1)
                                         filesInzip = 0;
                                if ((Ticket1 && filename.IndexOf("filesautomaticallyconverted") != -1) ||
                                    (!Ticket1))
                                {
                                    //check if already uploaded
                                    listFiles = listFiles + filename.Replace("IN/", "") + "____";
                                    countFiles++;
                                    var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename.Replace("IN/", "") + "'");
                                    //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()

                                    if (filename.ToUpper().IndexOf("ANNUAL SBC") != -1)
                                        fileU = "x";

                                    if (fileU != null)
                                    { }
                                    else
                                    {
                                        
                                        DirLocal = ProcessVars.InputDirectory + @"from_FTP";
                                        string ext = result.Ext;

                                        try
                                        {
                                            resultDownload = DownLoadFiles(info.Host + "/" + filename, filename.Replace("IN/", ""), DirLocal, info.User, info.Pass);
                                           
                                            if (resultDownload.ToString() == "")
                                            {
                                                downlResult = "1";
                                                lastModifiedDate = System.IO.File.GetLastWriteTime(ProcessVars.InputDirectory + @"from_FTP\" + filename.Replace("IN/", ""));
                                                try
                                                    {
                                                    // string ftplocation = "ftp://sftp.cierant.com//IN//";
                                                    if(filename.Contains("HLGS")  == false)
                                                    NotDownLoadFile_just_Move(info.Host + "//IN//", filename.Replace("IN/", ""), info.User, info.Pass);
                                                    }
                                                catch (Exception ex)
                                                    {
                                                    var heree = "error renaming";
                                                    }

                                            }
                                            else
                                            {
                                                error = error + filename.Replace("IN/", "") + Environment.NewLine;
                                                lastModifiedDate = DateTime.Now;//.ToString("MM/dd/yyyy HH:mm:ss");
                                                dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                             Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "',0)");


                                            }
                                            LogWriter logerror = new LogWriter();
                                            logerror.WriteLogToTable("file downloaded", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", info.Host + "/" + filename + " " + filesInzip, "email", resultDownload);
                                            //} while (downlResult == "0" && totTry < 2);
                                            Seqnum++;
                                        }
                                        catch (Exception ex)
                                        {
                                            LogWriter logerror = new LogWriter();
                                            error = error + ex.Message + Environment.NewLine;
                                            logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);
                                            lastModifiedDate = DateTime.Now;//.ToString("MM/dd/yyyy HH:mm:ss");
                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                         Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "',0)");


                                        }
                                        if (downlResult == "1")
                                        {
                                            if (ext == "pgp")
                                            {
                                                string resultDecrypt = DecryptFile(DirLocal + @"\" + filename.Replace("IN/", ""));  //DirLocal + @"\" + filename.Replace("IN/", "")
                                                LogWriter logerror = new LogWriter();
                                                logerror.WriteLogToTable("file decrypted", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", info.Host + "/" + filename + " " + filesInzip, "email", resultDecrypt);
                                                if (resultDecrypt.IndexOf("error") == -1)
                                                {
                                                    filesInzip = UnzipFile(resultDecrypt, ProcessVars.InputDirectory + "Decrypted");
                                                    //LogWriter logerror = new LogWriter();
                                                    logerror.WriteLogToTable("file unziped", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", info.Host + "/" + filename + " " + filesInzip, "email");
                                                }

                                                dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                       Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "'," + filesInzip + ")");
                                                if (filesInzip < 5)
                                                {
                                                    //LogWriter logerror = new LogWriter();
                                                    logerror.WriteLogToTable("minimun files in pgp", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error files in pgp " + info.Host + "/" + filename + " " + filesInzip, "email");
                                                }
                                            }
                                            if (ext == "zip" && filename.IndexOf("CRNJLTR") != -1)
                                            {
                                                int totf = 0;
                                                DataTable datafromPdfs = data_Table();
                                                string xmlName = "";
                                                int linenum = 0;
                                                try
                                                {
                                                    Directory.CreateDirectory(DirLocal + "\\tmp");
                                                    using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("IN/", "")))
                                                    {
                                                        //moved outside , because if file need to manual download
                                                        foreach (ZipArchiveEntry entry in archive.Entries)
                                                        {
                                                            if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                                            {
                                                                totf++;
                                                            }
                                                        }
                                                    }
                                                }
                                                catch (Exception ex)
                                                {
                                                    totf = 0;
                                                }

                                                if (totf > 0)
                                                {
                                                    foreach (DataRow row in datafromPdfs.Rows)
                                                    {
                                                        dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                                             filename.Replace("IN/", "") + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                                                    }
                                                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                        Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                        lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                        DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");


                                                }
                                                else
                                                {
                                                    foreach (DataRow row in datafromPdfs.Rows)
                                                    {
                                                        dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                                             filename.Replace("IN/", "") + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                                                    }
                                                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                        Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                        lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                        DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");
                                                    LogWriter logerror = new LogWriter();
                                                    logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + info.Host + "/" + filename + " " + filesInzip, "email");
                                                }
                                                if (xmlName.Length > 1)
                                                    process_xmlCR(xmlName, "", datafromPdfs.Rows.Count);
                                            }
                                            //==============
                                            if (ext == "zip" && filename.IndexOf("OEINV") != -1)
                                            {
                                                int totf = 0;
                                                DataTable datafromPdfs = data_Table();
                                                string xmlName = "";
                                                int linenum = 0;
                                                
                                                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                        Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                        lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                        DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                                                   
                                            }


                                            //==============

                                            if (ext == "zip" && filename.IndexOf("MAPAR") != -1)
                                            {
                                                int totf = 0;   //nor renameing __
                                                DataTable datafromPdfs = data_Table();
                                                string xmlName = "";
                                                int linenum = 0;
                                                try
                                                {
                                                    if (Directory.Exists(DirLocal + "\\tmp"))
                                                    {
                                                        DirectoryInfo dir = new DirectoryInfo(DirLocal + "\\tmp");

                                                        foreach (FileInfo fi in dir.GetFiles())
                                                        {
                                                            fi.IsReadOnly = false;
                                                            fi.Delete();
                                                        }
                                                        Directory.Delete(DirLocal + "\\tmp");
                                                    }
                                                    Directory.CreateDirectory(DirLocal + "\\tmp");

                                                    using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("IN/", "")))
                                                    {

                                                        foreach (ZipArchiveEntry entry in archive.Entries)
                                                        {
                                                            if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                                            {
                                                                xmlName = Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName);
                                                                entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName));
                                                                totf++;
                                                            }
                                                            else
                                                            {
                                                                var row = datafromPdfs.NewRow();
                                                                entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                                                                linenum++;
                                                                row["Seqnum"] = linenum;
                                                                row["ZIP"] = filename.Replace("IN/", "");
                                                                row["Fname"] = entry.Name.ToString();
                                                                int totPages = 0;
                                                                PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\tmp", entry.FullName));
                                                                totPages = readerP.NumberOfPages;
                                                                row["Pages"] = totPages.ToString();
                                                                row["FileInXML"] = "N";
                                                                datafromPdfs.Rows.Add(row);
                                                            }
                                                        }
                                                    }
                                                }
                                                catch (Exception ex)
                                                {
                                                    totf = 0;
                                                }

                                                if (totf > 0)
                                                {
                                                    //check to upload data to all tables

                                                    process_xmlMAPAR(xmlName, filename.Replace("IN/", ""));
                                                    foreach (DataRow row in datafromPdfs.Rows)
                                                    {
                                                        dbU.ExecuteScalar("Insert into HOR_Parse_Mapar_Client_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                                             filename.Replace("IN/", "") + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                                                    }
                                                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                        Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                        lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                        DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");

                                                   // File.Move(DirLocal + @"\" + filename.Replace("IN/", ""), DirLocal + @"\" + filename.Replace("IN/", "").Replace("MAPAR", "__MAPAR"));
                                                }

                                            }

                                            if (ext == "zip" && filename.IndexOf("CRNJLTR") == -1 && filename.IndexOf("MAPAR") == -1)
                                            {
                                                //DirectoryInfo originaZips = new DirectoryInfo(DirLocal);
                                                //FileInfo[] filesZ = originaZips.GetFiles("*.zip");
                                                //filesZ.Count();
                                                string DirNAme = "";
                                                filesInzip = 0;
                                                string extractPath = ProcessVars.InputDirectory + "From_FTP";
                                                string zipFile = DirLocal + @"\" + filename.Replace("IN/", "");
                                                //foreach (FileInfo zipFile in filesZ)
                                                //{
                                                string JustFName = filename.Replace("IN/", "").ToUpper(); //zipFile.Name;
                                                //if (JustFName.IndexOf("SHBP HLGS") != -1)
                                                //{
                                                //    string directoryUnzip = extractPath + "\\" + filename.Replace("IN/", "").ToUpper().Replace(".ZIP","_unzip");
                                                //    Directory.CreateDirectory(directoryUnzip);
                                                //    ZipArchive zip = ZipFile.OpenRead(zipFile.ToString());

                                                //    foreach (ZipArchiveEntry entry in zip.Entries)
                                                //    {
                                                //        //var xfilename = entry.FullName;
                                                //        ZipFile.ExtractToDirectory(extractPath + "\\" + entry.FullName, directoryUnzip);
                                                //        //ZipFile.ExtractToDirectory(entry.FullName, directoryUnzip);
                                                //    }


                                                //}
                                                if (JustFName.IndexOf("HLGS") != -1)
                                                {

                                                    //if (zipFile.Name.ToUpper() == filename.Replace("IN/", "").ToUpper())
                                                    //{
                                                    try
                                                    {
                                                        //ZipArchive zip = ZipFile.OpenRead(zipFile.ToString());
                                                        //foreach (ZipArchiveEntry entry in zip.Entries)
                                                        //{
                                                        //    int lengthD = entry.FullName.Length - entry.Name.Length;
                                                        //    DirNAme = entry.FullName.Substring(0, lengthD - 1);
                                                        //    break;
                                                        //}
                                                        //filesInzip++;
                                                        //System.IO.Compression.ZipFile.ExtractToDirectory(zipFile, extractPath);
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        //filesInzip = filesInzip;
                                                        LogWriter logErrorProcess = new LogWriter();
                                                        logErrorProcess.WriteLogToTable("error extracting ZIP", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "File name: " + JustFName + " files in zip " + filesInzip + " Msg:" + ex.Message);
                                                    }



                                                }
                                                else
                                                {
                                                    bool direxist = false;
                                                    var result2 = miscToExpand.FirstOrDefault(s => JustFName.ToUpper().Contains(s.Name.ToUpper()));
                                                    if (result2 != null)
                                                    {

                                                        string name = result2.Name;
                                                        string code = result2.Code;
                                                        extractPath = ProcessVars.InputDirectory + @"From_FTP\MISC\" + result2.Dir;
                                                        if (Directory.Exists(extractPath))
                                                            direxist = true;
                                                        System.IO.Directory.CreateDirectory(extractPath);
                                                        string textIn = "";
                                                        try
                                                        {
                                                            filesInzip++;
                                                            using (ZipArchive archive = ZipFile.Open(zipFile, ZipArchiveMode.Read))
                                                            {
                                                                DirNAme = "";
                                                                foreach (ZipArchiveEntry entry in archive.Entries)
                                                                {
                                                                    DirNAme = entry.ToString().Substring(0, entry.ToString().LastIndexOf('/'));
                                                                    if (!Directory.Exists(extractPath + @"\" + DirNAme))
                                                                    {
                                                                        textIn = textIn + extractPath + @"\" + DirNAme + "~" + JustFName + "~" + code + "\n\n";
                                                                        Directory.CreateDirectory(extractPath + @"\" + DirNAme);
                                                                    }

                                                                    //Console.WriteLine("Size: " + entry.CompressedLength);
                                                                    //Console.WriteLine("Name: " + entry.Name);
                                                                    entry.ExtractToFile(extractPath + @"\" + entry);
                                                                }
                                                                //textIn = extractPath + @"\" + DirNAme + "~" + JustFName + "\n\n";
                                                            }




                                                            //System.IO.Compression.ZipFile.ExtractToDirectory(zipFile, extractPath);
                                                            //string text = extractPath + "~" + JustFName + "~" ;

                                                            System.IO.File.WriteAllText(extractPath + @"\" + JustFName.Replace(".ZIP", ".txt"), textIn);
                                                        }
                                                        catch (Exception ex)
                                                        {
                                                            //filesInzip = filesInzip;
                                                            LogWriter logErrorProcess = new LogWriter();
                                                            logErrorProcess.WriteLogToTable("error extracting ZIP", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "File name: " + JustFName + " files in zip " + filesInzip + " Msg:" + ex.Message);
                                                        }
                                                    }
                                                }
                                                //}
                                                if (filename.IndexOf("MAPDP") > 0)
                                                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn,Misc_Location) values(" +
                                                        Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "',1,'" + DirNAme + "')");//   + extractPath + @"\" + DirNAme + "')");

                                                else
                                                dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn,Misc_Location) values(" +
                                                    Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "'," + filesInzip + ",'" + DirNAme + "')");//   + extractPath + @"\" + DirNAme + "')");

                                            }
                                            if (ext.ToLower() == "pdf" || ext == "txt")
                                            {
                                                //Dir = ProcessVars.InputDirectory + @"from_FTP";
                                                DataTable results = dbU.ExecuteDataTable("select* from HOR_parse_files_downloaded where filename ='" + filename.Replace("IN/", "") + "'");
                                                if (results.Rows.Count == 0)
                                                {
                                                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                        Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "',1)");
                                                }
                                            }
                                        }
                                    }
                                    //other
                                }
                                // }
                            }

                        }
                        else
                            filestoImport = filestoImport + "\n\n" + filename;

                    }
                }
                catch (Exception ex)
                {
                    //error reading vtoc to download
                    var exception = ex.Message;
                }
            }
            catch (Exception ex)
            {
                var exception = ex.Message;
            }
            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);
            if (error != "")
                return "Download manually :  " + error;
            else
                return "";
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
        public void expand_CRNJLTR_ZIPPriority(DateTime dateProcess)
        {
            string DirLocal = ProcessVars.InputDirectory + "From_FTP";

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            //DirectoryInfo CRNJLTRZIPs = new DirectoryInfo(DirLocal);
            string unzipDirName = "";
            string[] files = GetFiles(ProcessVars.InputDirectory + @"From_FTP", "CRC*.zip|CRN*.zip", SearchOption.TopDirectoryOnly);

            //foreach (FileInfo f in CRNJLTRZIPs.GetFiles("CRN*.zip"))
            foreach (string fi in files)
            {
                FileInfo f = new FileInfo(fi);
                int totf = 0; int linenum = 0;
                string xmlName = "";
                DataTable datafromPdfs = data_Table();
                var fileProcessed = dbU.ExecuteScalar("select distinct sourcename from HOR_Care_Radius_DataXML_Detail_pdfs where SourceName ='" + f.Name.Replace("__", "") + "'");
                string fileWasProcessed = "No";
                if (fileProcessed != null)
                    fileWasProcessed = "Yes";
                if (f.Name.IndexOf("__") == -1 && fileWasProcessed == "No" && f.Name.IndexOf("PR_") != -1)
                {
                    try
                    {
                        Directory.CreateDirectory(DirLocal + "\\tmp");
                        System.IO.DirectoryInfo di = new DirectoryInfo(DirLocal + "\\tmp");
                        foreach (FileInfo file in di.GetFiles())
                        {
                            file.Delete();
                        }
                        foreach (DirectoryInfo dir in di.GetDirectories())
                        {
                            dir.Delete(true);
                        }
                        using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + f.Name))
                        {

                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                {
                                    xmlName = Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName);
                                    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName));
                                    totf++;
                                }
                                else
                                {
                                    var row = datafromPdfs.NewRow();
                                    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                                    linenum++;
                                    row["Seqnum"] = linenum;
                                    row["ZIP"] = f.Name;
                                    row["Fname"] = entry.Name.ToString();
                                    int totPages = 0;
                                    PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\tmp", entry.FullName));
                                    totPages = readerP.NumberOfPages;
                                    row["Pages"] = totPages.ToString();
                                    row["FileInXML"] = "N";
                                    datafromPdfs.Rows.Add(row);
                                    readerP.Close();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        totf = 0;
                    }

                    if (totf > 0)
                    {
                        dbU.ExecuteNonQuery("delete from HOR_Care_Radius_DataXML_Detail_pdfs where SourceName ='" + f.Name + "'");


                        foreach (DataRow row in datafromPdfs.Rows)
                        {
                            dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                 f.Name + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        }
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
                                   DSeqnum + ",'" + f.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + f.Extension.Replace(".", "") + "',1,'" + DirLocal + "','" +
                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                        }
                        else
                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);




                        //dbU.ExecuteScalar("update HOR_parse_files_downloaded set Unziped = 1 where filename = '" + f.Name + "'");
                        //File.Move(f.FullName, f.Directory + "\\__" + f.Name);
                    }
                    else
                    {
                        foreach (DataRow row in datafromPdfs.Rows)
                        {
                            dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                 f.Name + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        }
                        LogWriter logerror = new LogWriter();
                        logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + f.Name, "email");
                    }
                    if (xmlName.Length > 1)
                        process_xmlCR(xmlName, "PR_", datafromPdfs.Rows.Count);
                }
            }
        }
        public void expand_CRNJLTR_ZIP(DateTime dateProcess)
        {
            string DirLocal = ProcessVars.InputDirectory + "From_FTP";

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            //DirectoryInfo CRNJLTRZIPs = new DirectoryInfo(DirLocal);
            string unzipDirName = "";
            string[] files = GetFiles(ProcessVars.InputDirectory + @"From_FTP", "CRC*.zip|CRN*.zip", SearchOption.TopDirectoryOnly);
            string errProcess = "";
            //foreach (FileInfo f in CRNJLTRZIPs.GetFiles("CRN*.zip"))
            foreach(string fi in files )
            {
                errProcess = "";
                FileInfo f = new FileInfo(fi);
                int totf = 0; int linenum = 0;
                string xmlName = "";
                DataTable datafromPdfs = data_Table();
                var fileProcessed = dbU.ExecuteScalar("select distinct sourcename from HOR_Care_Radius_DataXML_Detail_pdfs where SourceName ='" + f.Name.Replace("__","") + "'");
                string fileWasProcessed = "No";
                if(fileProcessed != null)
                    fileWasProcessed = "Yes";
                if (f.Name.IndexOf("__") == -1 && fileWasProcessed == "No" )
                {
                    try
                    {
                        Directory.CreateDirectory(DirLocal + "\\tmp");
                        System.IO.DirectoryInfo di = new DirectoryInfo(DirLocal + "\\tmp");
                        foreach (FileInfo file in di.GetFiles())
                        {
                            file.Delete();
                        }
                        foreach (DirectoryInfo dir in di.GetDirectories())
                        {
                            dir.Delete(true);
                        }
                        using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + f.Name))
                        {
                           
                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                {
                                    try
                                    {
                                        xmlName = Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName);
                                        entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName));
                                        totf++;
                                    }
                                    catch (Exception ex)
                                    {
                                        totf = 0;
                                        errProcess = errProcess + "Zip: " + f.Name + " file: " + entry.FullName + "  error: " + ex.Message + "~";
                                    }
                                }
                                else
                                {
                                    try
                                    {
                                        var row = datafromPdfs.NewRow();
                                        entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                                        linenum++;
                                        row["Seqnum"] = linenum;
                                        row["ZIP"] = f.Name;
                                        row["Fname"] = entry.Name.ToString();
                                        int totPages = 0;
                                        PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\tmp", entry.FullName));
                                        totPages = readerP.NumberOfPages;
                                        row["Pages"] = totPages.ToString();
                                        row["FileInXML"] = "N";
                                        datafromPdfs.Rows.Add(row);
                                        readerP.Close();
                                    }
                                    catch (Exception ex)
                                    {
                                        totf = 0;
                                        errProcess = errProcess + "Zip: " + f.Name + " file: " + entry.FullName + "  error: " + ex.Message + "~";
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        totf = 0;
                        errProcess = errProcess + f.Name + "  error: " + ex.Message;
                    }

                    if (errProcess.Length == 0)
                    {
                        dbU.ExecuteNonQuery("delete from HOR_Care_Radius_DataXML_Detail_pdfs where SourceName ='" + f.Name + "'");


                        foreach (DataRow row in datafromPdfs.Rows)
                        {
                            dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                 f.Name + "','" + row["fname"].ToString() + "','" + dateProcess.ToString("yyyy/MM/dd") + " " + DateTime.Now.ToString("HH:mm:ss") + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        }
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
                                   DSeqnum + ",'" + f.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + f.Extension.Replace(".", "") + "',1,'" + DirLocal + "','" +
                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");

                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                        }
                        else
                            dateUpload = (DateTime)dbU.ExecuteScalar(strsql);
                        
                        
                        
                        
                        //dbU.ExecuteScalar("update HOR_parse_files_downloaded set Unziped = 1 where filename = '" + f.Name + "'");
                        //File.Move(f.FullName, f.Directory + "\\__" + f.Name);
                    }
                    else
                    {
                        if (errProcess.Length > 0)
                        {
                            string errFilename = f.Directory + "\\__error_" + f.Name.Replace(".zip", ".txt");
                            FileStream fs1 = new FileStream(errFilename, FileMode.OpenOrCreate, FileAccess.Write);
                            StreamWriter writer = new StreamWriter(fs1);
                            string[] words = errProcess.Split('~');
                            //Text.Append(currentText);
                            for (int i = 0; i < words.Length; i++)
                            {
                                writer.Write(words[i].ToString() + Environment.NewLine);
                            }
                            writer.Close();
                            File.Move(f.FullName, f.Directory + "\\__error_" + f.Name);
                            int DSeqnum = 0;
                            var Drecnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");

                            if (Drecnum.ToString() == "")
                                DSeqnum = 1;
                            else
                                DSeqnum = Convert.ToInt32(Drecnum.ToString()) + 1;
                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn,AfterProcessLocation) values(" +
                                  DSeqnum + ",'" + f.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + f.Extension.Replace(".", "") + "',1,'" + DirLocal + "','" +
                                  DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                  DateTime.Now.ToString("yyyy-MM-dd") + "',1,'" + errProcess.Replace("\\","\\\\") + "')");


                        }
                        //foreach (DataRow row in datafromPdfs.Rows)
                        //{
                        //    dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                        //         f.Name + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        //}
                        //LogWriter logerror = new LogWriter();
                        //logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + f.Name, "email");
                    }
                    string Priority = "";
                    if (f.Name.IndexOf("PR_") != -1)
                        Priority = "PR_";
                    if (xmlName.Length > 1 && errProcess.Length == 0)
                        process_xmlCR(xmlName, Priority, datafromPdfs.Rows.Count);
                }
            }
        }
        public void expand_MAPARTB_ZIP(DateTime dateProcess)
        {
            //string DirLocal = ProcessVars.InputDirectory + "From_FTP";
            string DirLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "\\from_ftp";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            DirectoryInfo CRNJLTRZIPs = new DirectoryInfo(DirLocal);
            string unzipDirName = "";
            foreach (FileInfo f in CRNJLTRZIPs.GetFiles("MAPARTBLTR_*.zip"))
            {
                int totf = 0; int linenum = 0;
                string xmlName = "";
                DataTable datafromPdfs = data_Table();
                if (f.Name.IndexOf("__MAPARTBLTR_") == -1)
                {
                    try
                    {
                        Directory.CreateDirectory(DirLocal + "\\tmp");
                        if (Directory.Exists(DirLocal + "\\tmp"))
                        {
                            DirectoryInfo dir = new DirectoryInfo(DirLocal + "\\tmp");

                            foreach (FileInfo fi in dir.GetFiles())
                            {
                                fi.IsReadOnly = false;
                                fi.Delete();
                            }
                        }
                        using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + f.Name))
                        {

                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                {
                                    xmlName = Path.Combine(DirLocal , entry.FullName);
                                    entry.ExtractToFile(Path.Combine(DirLocal , entry.FullName),true);
                                    totf++;
                                }
                                else
                                {
                                    var row = datafromPdfs.NewRow();
                                    entry.ExtractToFile(Path.Combine(DirLocal + "\\tmp" , entry.FullName),true);
                                    linenum++;
                                    row["Seqnum"] = linenum;
                                    row["ZIP"] = f.Name;
                                    row["Fname"] = entry.Name.ToString();
                                    int totPages = 0;
                                    PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\tmp", entry.FullName));
                                    totPages = readerP.NumberOfPages;
                                    row["Pages"] = totPages.ToString();
                                    row["FileInXML"] = "N";
                                    datafromPdfs.Rows.Add(row);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        totf = 0;
                    }
                  

                    if (totf > 0)
                    {
                        if (xmlName.Length > 1)
                            process_xmlMAPAR(xmlName, f.Name);

                        dbU.ExecuteScalar("delete from HOR_parse_MAPAR_Client_Detail_pdfs where SourceName = '" + f.Name + "'");
                        foreach (DataRow row in datafromPdfs.Rows)
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_MAPAR_Client_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                 f.Name + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        }

                        var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + f.Name.ToString() + "'");
                        //string dateMAPAR = "";
                        if (fileDate == null)
                        {
                            int DSeqnum = 0;
                            var Drecnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");

                            if (Drecnum.ToString() == "")
                                DSeqnum = 1;
                            else
                                DSeqnum = Convert.ToInt32(Drecnum.ToString()) + 1;

                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                   DSeqnum + ",'" + f.Name.Replace("REPORTING RENEWAL MAILING/", "") + "','" + f.Extension.Replace(".", "") + "',1,'" + DirLocal + "','" +
                                   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                   DateTime.Now.ToString("yyyy-MM-dd") + "',1)");
                            //dateHLGS = GlobalVar.DateofProcess.ToString("yyyy-MM-dd HH:mm:ss");
                        }
                        
                        
                        dbU.ExecuteScalar("update HOR_parse_files_downloaded set Unziped = 1 where filename = '" + f.Name + "'");
                        //File.Move(f.FullName, f.Directory + "\\__" + f.Name);
                        File.Move(f.FullName, ProcessVars.InputDirectory + @"\w_Process" + f.Name);
                    }
                    else
                    {
                        foreach (DataRow row in datafromPdfs.Rows)
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_MAPAR_Client_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                 f.Name + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        }
                        LogWriter logerror = new LogWriter();
                        logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + f.Name, "email");
                    }

                   
                }
            }
        }
        public void process_xmlMAP(string xmlName)
        {
            FileInfo filexml = new System.IO.FileInfo(xmlName);
            string fustFName = filexml.Name;
            string strsql = "delete from HOR_Parse_TH_Letters_Detail_xml where cycledate = '" + DateTime.Now.ToString("yyyy-MM-dd") +
                        "' and OriginalFileName = '" + fustFName + "'";
            dbU.ExecuteNonQuery(strsql);
            string dateHLGS = "";
            var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            if (fileDate != null)
            {
                dateHLGS = fileDate.ToString();
                dbU.ExecuteScalar("update HOR_parse_files_downloaded set Filesin = 1 where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            }
            else
                dateHLGS = DateTime.Now.ToString();
            xmlUploadData uploadMAP = new xmlUploadData();
            uploadMAP.UploadXML(xmlName, dateHLGS, DateTime.Now.ToString("yyyy-MM-dd"));
            ExportCSV(fustFName, filexml.Directory.ToString(),"HOR_rpt_TH_Letters_Detail_xml", "");
            

        }
        public void process_xmlCR(string xmlName, string Priority, int totpdfs)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            FileInfo filexml = new System.IO.FileInfo(xmlName);
            string fustFName = filexml.Name;
            string strsql = "delete from HOR_Care_Radius_DataXML where cycledate = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and OriginalFileName = '" + fustFName + "'";
            
            dbU.ExecuteNonQuery(strsql);
            string dateHLGS = "";
            var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            if (fileDate != null)
            {
                dateHLGS = fileDate.ToString();
                dbU.ExecuteScalar("update HOR_parse_files_downloaded set Filesin = 1 where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            }
            else
                dateHLGS = DateTime.Now.ToString();
            xmlUpload_CR uploadMAP = new xmlUpload_CR();
            //uploadMAP.CR_UploadXML(xmlName, dateHLGS, DateTime.Now.ToString("yyyy-MM-dd"));

            uploadMAP.CR_UploadXML(xmlName, dateHLGS, GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));
            int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(*) from HOR_Care_Radius_DataXML where OriginalFileName = '" + fustFName + "'"));
            if (FileCount == totpdfs)
                ExportCSV(fustFName, filexml.Directory.ToString(), "HOR_rpt_Care_Radius_UsrMail", Priority);
            else
            {
                strsql = "delete from HOR_Care_Radius_DataXML where OriginalFileName like '" + fustFName.Replace(".zip","") + "'";  // xml
                dbU.ExecuteNonQuery(strsql);
                strsql = "delete from HOR_Care_Radius_DataXML_Detail_pdfs where sourcename = '" + fustFName.Replace(".xml","") + "'";  //zip
                dbU.ExecuteNonQuery(strsql);
                File.Move(filexml.Directory.ToString() + fustFName.Replace(".xml", ".zip"), filexml.Directory.ToString().Replace("from_FTP", "Errors")  +fustFName.Replace(".xml", ".zip"));
                File.Delete(xmlName);


            }
            //ExportCSV(fustFName, filexml.Directory.ToString(), "HOR_rpt_Care_Radius");


        }
        public void process_xmlMAPAR(string xmlName, string zipname)
        {
            FileInfo filexml = new System.IO.FileInfo(xmlName);
            string fustFName = filexml.Name;
            string strsql = "delete from HOR_parse_MAPAR_Client where fromfile = '" + fustFName + "'";
            dbU.ExecuteNonQuery(strsql);
            dbU.ExecuteNonQuery("delete from HOR_parse_MAPAR_Receipt where fromfile = '" + fustFName + "'");
            dbU.ExecuteNonQuery("delete from HOR_parse_MAPAR_Flight where fromfile = '" + fustFName + "'");
            dbU.ExecuteNonQuery("delete from HOR_parse_MAPAR_Client_Detail_pdfs where Sourcename = '" + zipname + "'");
            string dateHLGS = "";
            var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            if (fileDate != null)
            {
                dateHLGS = fileDate.ToString();
                dbU.ExecuteScalar("update HOR_parse_files_downloaded set Filesin = 1 where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            }
            else
            {
                //dateHLGS = DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss");
                //dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                //                              Seqnum + ",'" + fustFName.Replace(".xml", "") + ".zip" + "','zip',1,' ','" +
                //                              dateHLGS + "', GETDATE(),'" +
                //                              DateTime.Now.ToString("yyyy-MM-dd") + "',0)");
            }
            xmlupload_MAPAR uploadMAP = new xmlupload_MAPAR();
            uploadMAP.MAPAR_UploadXML(xmlName, dateHLGS, DateTime.Now.ToString("yyyy-MM-dd"));
            ExportCSV(fustFName, filexml.Directory.ToString(), "HOR_rpt_MAPAR", "");


        }
        public void ExportCSV(string filename, string directory, string StoreProc, string Priority)
        {

            DataTable DatesToExport = new DataTable();
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable plData = new DataTable();
            SqlParameter[] sqlParams;

            sqlParams = new SqlParameter[] { new SqlParameter("@Ofilename", filename),
                                                 new SqlParameter("@Idate", GlobalVar.DateofProcess.ToString("yyyy-MM-dd"))};   //DateTime.Now.ToString("yyyy-MM-dd")

            plData = dbU.ExecuteDataTable(StoreProc, sqlParams);
            if (plData != null)
            {
                DataView dv = plData.DefaultView;
                dv.Sort = "Transactionid";
                DataTable sortedDT = dv.ToTable();
                DataColumnCollection columns = sortedDT.Columns;

                if (columns.Contains("OriginalFileName"))
                {
                    sortedDT.Columns.Remove("OriginalFileName");
                }

                string JustName = filename.Substring(0, filename.Length - 4);
                string pName = directory + "\\" + JustName + ".csv";
                string processedZip = directory + "\\__" + JustName + ".zip";
                //string OutputpName = OutputDataPath + "\\" + JustName + ".csv";
                string OutputpNameZip = directory + "\\" + JustName + ".zip";
                createCSV createFilecsv = new createCSV();
                createFilecsv.printCSV_fullProcess(pName, plData, "", "N");

                //if (File.Exists(pName))  //csv
                //    File.Delete(pName);
                if (Priority == "PR_")
                {
                    FileInfo filetoUpload = new System.IO.FileInfo(pName);
                    N_loadFromFTP uploadZip = new N_loadFromFTP();
                    string resultUpload = uploadZip.Upload_SFTP(filetoUpload.Name, pName, 1, "/CareRadiusPriority/", 0, plData.Rows.Count);

                    FileInfo filetoUpload2 = new System.IO.FileInfo(OutputpNameZip);
                    resultUpload = uploadZip.Upload_SFTP(filetoUpload2.Name, filetoUpload2.FullName, 1, "/CareRadiusPriority/", 0, 0);

                    dbU.ExecuteNonQuery("Insert into HOR_parse_Log_Zips (Logdate, Type, Zipname, ZipCount, csvNames, txtNames) Values ('" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "','Ticket02 PR zip','" +
                  JustName + "',1,'" + plData.Rows.Count.ToString() + "','1')");

                    File.Move(OutputpNameZip, ProcessVars.InputDirectory + @"\w_Process\" + JustName + ".zip");
                    File.Move(pName, ProcessVars.InputDirectory + @"\w_Process\" + JustName + ".csv");


                }
                else
                {
                    if (JustName.IndexOf("MAPAR") != -1)
                    {
                        var here = "check .csv and .zip where they are before move..";
                        File.Copy(OutputpNameZip, ProcessVars.OtherProcessed + JustName + ".zip");
                        File.Copy(pName, ProcessVars.OtherProcessed + JustName + ".csv");
                    }
                    else
                    {
                        FileInfo filetoUpload = new System.IO.FileInfo(pName);
                        N_loadFromFTP uploadZip = new N_loadFromFTP();
                        string resultUpload = uploadZip.Upload_SFTP(filetoUpload.Name, pName, 1, "/CareRadius/", 0, plData.Rows.Count);

              
                        FileInfo filetoUpload2 = new System.IO.FileInfo(OutputpNameZip);
                        resultUpload = uploadZip.Upload_SFTP(filetoUpload2.Name, filetoUpload2.FullName, 1, "/CareRadius/", 0, 0);
                        dbU.ExecuteNonQuery("Insert into HOR_parse_Log_Zips (Logdate, Type, Zipname, ZipCount, csvNames, txtNames) Values ('" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "','Ticket02 zip','" +
                            JustName + "',1,'" + plData.Rows.Count.ToString() + "','1')");

                        File.Move(OutputpNameZip, ProcessVars.InputDirectory + @"\w_Process\" + JustName + ".zip");
                        File.Move(pName, ProcessVars.InputDirectory + @"\w_Process\" + JustName + ".csv");


                    }
                }

                //File.Move(OutputpNameZip, directory + "\\__" + JustName + ".zip");
                //File.Move(pName, directory + "\\__" + JustName + ".csv");
                if (JustName.IndexOf("PR_") != -1)
                {
                    //File.Move(OutputpNameZip, ProcessVars.CRprocessed + JustName + ".zip");
                    //File.Move(pName, ProcessVars.CRprocessed + JustName + ".csv");
                }
                else
                {
                    //File.Move(OutputpNameZip, ProcessVars.OtherProcessed + JustName + ".zip");
                    //File.Move(pName, ProcessVars.OtherProcessed + JustName + ".csv");
                }

            }

            //string dateProc = DateTime.Now.ToString("yyyy-MM-dd");
            //produceSummary(dateProc, fileNAMES);


        }
        public string ftp_CheckFiles(DateTime dateProcess, bool Ticket1, string option)
        {
            
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport(option);

          
            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "Horizon";
            info.Pass = "CyRyk1al";
            info.Host = "ftp://sftp.cierant.com";
            int steps = 0;
            //string tarFiles = "filesautomaticallyconverted";
            string ftpSubDir = "sftp.cierant.com/HorizonBCBS/IN/";
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/IN"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass);

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            DateTime lastModifiedDate = response.LastModified;


            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            string NetworkDIR = ProcessVars.networkDir + GlobalVar.DateofProcess.ToString("yyyy_MMMM") + "\\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"_test\";

            //System.IO.Directory.CreateDirectory(NetworkDIR);

            //DirectoryInfo dirLocal = new DirectoryInfo(ProcessVars.InputDirectory + @"from_FTP");
            string DirLocal = "";
            string listFiles = "";
            int countFiles = 0;
            while (!reader.EndOfStream)
            {
                string filename = reader.ReadLine().ToString();
                var result = fileToImport.FirstOrDefault(s => filename.Contains(s.Name));
                if (result != null)
                {
                    if (result.Name != null)
                    {
                        int compareValue;
                        int filesInzip = 0;
                        SessionOptions sessionOptions = new SessionOptions
                        {
                            Protocol = Protocol.Sftp,
                            HostName = "sftp.cierant.com",
                            UserName = info.User,
                            Password = info.Pass,
                            PortNumber = 22,

                            SshHostKeyFingerprint = "ssh-rsa 2048 a7:9d:68:84:88:ec:e4:e1:6d:09:9b:0f:b5:20:b1:7a"
                        };
                        using (Session session = new Session())
                        {
                            session.Open(sessionOptions); //Attempt to connect to sFtp site

                            //Get Ftp File Info
                            TransferOptions transferOptions = new TransferOptions();
                            transferOptions.TransferMode = TransferMode.Binary;

                            RemoteFileInfo FileInfo = session.GetFileInfo("/HorizonBCBS/" + filename);
                            lastModifiedDate = FileInfo.LastWriteTime;
                            string ZipfileName = FileInfo.Name;
                            System.Int64 Long = FileInfo.Length;
                            //object[] Return = { ZipfileName, LastWriteTime };
                        }
                        //compareValue = lastModifiedDate.CompareTo((DateTime.Today.AddDays(-1)));
                        if (Ticket1)
                            compareValue = lastModifiedDate.CompareTo((DateTime.Today.AddDays(0)));
                        else
                            compareValue = lastModifiedDate.CompareTo((dateProcess));
                        //responseD.Close();
                        var lastDay = lastModifiedDate.ToShortDateString();
                        // check if is today
                        if (compareValue == 1 || compareValue != 1)
                        {
                            if ((Ticket1 && filename.IndexOf("filesautomaticallyconverted") != -1) ||
                                (!Ticket1))
                            {
                                //check if already uploaded
                                listFiles = listFiles + filename.Replace("IN/", "") + "____";
                                countFiles++;
                                var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename.Replace("IN/", "") + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                { }
                                else
                                {
                                    filestoImport = filestoImport + filename.Replace("IN/", "") + "~";
                                }
                                //other
                            }
                        }
                    }

                }
               
            }


            return filestoImport;
        }
        public string downloadFiles_ID_Cards(DateTime dateProcess)
        {
            string fileReaded = "";
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport("IDCards");

            // clean files 0 in
            dbU.ExecuteScalar("delete from HOR_parse_files_downloaded where FilesIn = 0 ");
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;



            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "Horizon";
            info.Pass = "CyRyk1al";
            info.Host = "ftp://sftp.cierant.com";
            //info.Host = "ftp3.sciimage.com";    //backup
            
            int steps = 0;
            //string tarFiles = "filesautomaticallyconverted";
            string ftpSubDir = "sftp.cierant.com/HorizonBCBS/IN/";
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/IN"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass);

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            DateTime lastModifiedDate = response.LastModified;


            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            string NetworkDIR = ProcessVars.networkDir + GlobalVar.DateofProcess.ToString("yyyy_MMMM") + "\\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"_test\";

            //System.IO.Directory.CreateDirectory(NetworkDIR);

            //DirectoryInfo dirLocal = new DirectoryInfo(ProcessVars.InputDirectory + @"from_FTP");
            string DirLocal = "";
            string listFiles = "";
            int countFiles = 0;
            string fnames = "";
            while (!reader.EndOfStream)
            {
                
                string filename = reader.ReadLine().ToString();
                fnames = fnames + filename + Environment.NewLine;
                if(filename.ToUpper().IndexOf("CON2_TES") != -1)
                    listFiles = listFiles;
                var result = fileToImport.FirstOrDefault(s => filename.Contains(s.Name));

                if (filename.Contains("CIE_"))
                    result = null;
                if (result != null)
                {
                    if (result.Name != null)
                    {
                        int compareValue;
                        int filesInzip = 0;
                        SessionOptions sessionOptions = new SessionOptions
                        {
                            Protocol = Protocol.Sftp,
                            HostName = "sftp.cierant.com",
                            UserName = info.User,
                            Password = info.Pass,
                            PortNumber = 22,

                            SshHostKeyFingerprint = "ssh-rsa 2048 a7:9d:68:84:88:ec:e4:e1:6d:09:9b:0f:b5:20:b1:7a"
                        };
                        using (Session session = new Session())
                        {
                            session.Open(sessionOptions); //Attempt to connect to sFtp site

                            //Get Ftp File Info
                            TransferOptions transferOptions = new TransferOptions();
                            transferOptions.TransferMode = TransferMode.Binary;

                            RemoteFileInfo FileInfo = session.GetFileInfo("/HorizonBCBS/" + filename);
                            lastModifiedDate = FileInfo.LastWriteTime;
                            string ZipfileName = FileInfo.Name;
                            System.Int64 Long = FileInfo.Length;
                            //object[] Return = { ZipfileName, LastWriteTime };
                        }
                        compareValue = lastModifiedDate.CompareTo((dateProcess));
                        //responseD.Close();

                        // check if is today
                        if (compareValue == 1 || compareValue != 1)
                        {
                            //if ((filename.IndexOf("GRP2_") != -1 || filename.IndexOf("CON2_") != -1 
                            //                                     || filename.IndexOf("Heavy and General Laborers_") != -1 
                            //                                     || filename.IndexOf("Bed Bath and Beyond_") != -1
                            //                                     || filename.IndexOf("OMNIA_Update_HCV") != -1))
                            //{
                                if (!Directory.Exists(ProcessVars.InputDirectory + "ID_Cards"))
                                    Directory.CreateDirectory(ProcessVars.InputDirectory + "ID_Cards");
                                //check if already uploaded
                                listFiles = listFiles + filename.Replace("IN/", "") + "____";
                                countFiles++;
                                var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename.Replace("IN/", "") + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                {
                                    var messsage = "file processed";
                                }
                                else
                                {
                                    //if(filename.IndexOf("CON2_OMNIA_20170117_NSR_NASCO_HCV_HIX") != -1)
                                    //{
                                    DirLocal = ProcessVars.InputDirectory + @"from_FTP";
                                    string ext = result.Ext;

                                    try
                                    {

                                        string resultDownload = DownLoadFiles(info.Host + "/" + filename, filename.Replace("IN/", ""), DirLocal, info.User, info.Pass);

                                        Seqnum++;
                                    }
                                    catch (Exception ex)
                                    {
                                        LogWriter logerror = new LogWriter();
                                        error = ex.Message;
                                        logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);
                                    }
                                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                               Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                               lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                               DateTime.Now.ToString("yyyy-MM-dd") + "',0)");

                                   
                                //}
                                }
                                //other
                           // }
                        }
                    }

                }
                else
                    filestoImport = filestoImport + "\n\n" + filename;
            }

            fnames = fnames;
            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);

            return "";
        }
        public void unzip_ID_Cards()
        {
            appSets appsets = new appSets();
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            var fileToImport = appsets.getFilesImport("IDCards");
            string extractPath = ProcessVars.InputDirectory + "From_FTP";
            DirectoryInfo IDCardsZips = new DirectoryInfo(extractPath);
            FileInfo[] filesZ = IDCardsZips.GetFiles("*.zip");
          
            foreach (FileInfo filename in filesZ)
            {
                var result = fileToImport.FirstOrDefault(s => filename.Name.ToString().Contains(s.Name));
                if (result != null && filename.Name.ToString().Substring(0,1) != "_")
                {
                   
                    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                    if (filename.Extension == ".zip")
                    {
                        string subdirname = filename.Name.ToString().Replace("IN/", "").Replace("_PROCESSED.zip", "");
                        if (!Directory.Exists(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname))
                            Directory.CreateDirectory(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname);
                        int totf = 0;
                        try
                        {
                            using (ZipArchive archive = ZipFile.OpenRead(filename.Directory + @"\" + filename.Name.Replace("IN/", "")))
                            {

                                foreach (ZipArchiveEntry entry in archive.Entries)
                                {

                                    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.Name));
                                    totf++;

                                }
                            }

                            //move zip into dir
                            File.Move(filename.Directory + "\\" + filename.Name.Replace("IN/", ""), ProcessVars.InputDirectory + @"ID_Cards\" + subdirname + "\\" + filename.Name.Replace("IN/", ""));
                        }
                        catch (Exception ex)
                        {
                            totf = 0;
                        }

                        if (totf > 0)
                        {
                             var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename.Name.Replace("IN/", "") + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                             if (fileU == null)
                             {

                                 var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
                                 int recordnumber = 0;
                                 if (recnum.ToString() == "")
                                     Seqnum = 1;
                                 else
                                     Seqnum = Convert.ToInt32(recnum.ToString()) + 1;


                                 dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                   Seqnum + ",'" + filename.Name.Replace("IN/", "") + "','" + filename.Extension + "',1,'Downloaded Manually','" +
                                                   filename.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                   DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");

                             }
                             else
                             {

                                 dbU.ExecuteScalar("update HOR_parse_files_downloaded set FilesIn = " + totf +
                                     " where filename = '" + filename.Name.Replace("IN/", "") + "'");
                             }

                        }
                        else
                        {
                            LogWriter logerror = new LogWriter();
                            logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + filename.Name, "email");
                        }
                    }

                    //++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                //if ((filename.Name.ToString().IndexOf("GRP2_") != -1 || filename.Name.ToString().IndexOf("CON2_") != -1) ||
                //     filename.Name.ToString().IndexOf("Bed Bath and Beyond_") != -1 || filename.Name.ToString().IndexOf("Heavy and General Laborers_") != -1
                //     || filename.Name.ToString().IndexOf("OMNIA_") != -1)
                
                   
                    //  OLD CODE ===========================================================
                    //if (!Directory.Exists(ProcessVars.InputDirectory + "ID_Cards"))
                    //    Directory.CreateDirectory(ProcessVars.InputDirectory + "ID_Cards");


                    //if (filename.Extension.ToString() == ".zip")
                    //{
                    //    string subdirname = filename.Name.ToString().Replace("IN/", "").Replace("_PROCESSED.zip", "");
                    //    if (!Directory.Exists(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname))
                    //        Directory.CreateDirectory(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname);
                    //    int totf = 0;
                    //    try
                    //    {
                    //        //using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("IN/", "")))
                    //        using (ZipArchive archive = ZipFile.OpenRead(filename.FullName.ToString()))
                    //        {
                    //            string newname = "";
                    //            foreach (ZipArchiveEntry entry in archive.Entries)
                    //            {
                    //                //if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                    //                //{
                    //                if (entry.FullName.Contains("printing instructions") || entry.FullName.Contains("Inserts"))
                    //                {
                    //                    newname = "";
                    //                    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName.Replace("printing instructions/", "").Replace("Inserts/", "")));
                    //                }
                    //                else
                    //                    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName));

                    //                totf++;
                    //                //}
                    //            }
                    //        }
                    //        //move zip into dir
                    //        //File.Move(DirLocal + "\\" + filename.Replace("IN/", ""), ProcessVars.InputDirectory + @"ID_Cards\" + subdirname + "\\" + filename.Replace("IN/", ""));
                    //        File.Move(filename.FullName.ToString(), ProcessVars.InputDirectory + @"ID_Cards\" + subdirname + "\\" + filename.Name.ToString());
                    //    }
                    //    catch (Exception ex)
                    //    {
                    //        totf = 0;
                    //    }

                     
                    //}


                }
            }
        }
        public static string DecryptFile(string encryptedFilePath)
        {
            string decryptedFileName = "";
            try
            {
                FileInfo info = new FileInfo(encryptedFilePath);
                decryptedFileName = info.FullName.Substring(0, info.FullName.LastIndexOf('.')).Replace(".bz2","") + ".d" ;
                if (File.Exists(decryptedFileName))
                    File.Delete(decryptedFileName);

                string encryptedFileName = info.FullName;

                string password = "password";

                System.Diagnostics.ProcessStartInfo psi = new System.Diagnostics.ProcessStartInfo("cmd.exe");

                psi.CreateNoWindow = true;
                psi.UseShellExecute = false;
                psi.RedirectStandardInput = true;
                psi.RedirectStandardOutput = true;
                psi.RedirectStandardError = true;
                psi.WorkingDirectory =info.Directory.ToString();   // @"C:\Program Files (x86)\GNU\GnuPG";  

                System.Diagnostics.Process process = System.Diagnostics.Process.Start(psi);
                string sCommandLine = @"echo " + password + "|gpg.exe --passphrase-fd 0 --batch --verbose --yes --output " + decryptedFileName + @" --decrypt " + encryptedFileName;

                process.StandardInput.WriteLine(sCommandLine);
                process.StandardInput.WriteLine(password);
                process.StandardInput.Flush();
                process.StandardInput.Close();
                process.WaitForExit();
                //string result = process.StandardOutput.ReadToEnd();
                //string error = process.StandardError.ReadToEnd();
                process.Close();


            }
            catch (Exception ex)
            {
                decryptedFileName = "error " + ex.Message;
            }
            return decryptedFileName;
        }
        public static int UnzipFile(string zippedFilePath, string dirLocal)
        {
            int fotfiles = 0;
           FileInfo info = new FileInfo(zippedFilePath);
            string unzippedFileNStatus = "ok";
            //string zPath = @"C:\Program Files (x86)\7-Zip\7z.exe";
            string zPath = @"C:\Program Files\7-Zip\7z.exe";
            string destination = info.Directory.ToString() + @"\filesautomaticallyconverted"; //info.FullName.Substring(0, info.FullName.LastIndexOf('.')).Replace(".","");

            if (File.Exists(destination))
                File.Delete(destination);


            string source = info.FullName;

            try
            {
                ProcessStartInfo pro = new ProcessStartInfo();
                pro.WindowStyle = ProcessWindowStyle.Hidden;
                pro.FileName = zPath;
                pro.Arguments = "x \"" + source + "\" -o" + destination;
                //pro.Arguments = "e -so " + source + " | " + zPath + " e -si -ttar -o " + destination;
                Process x = Process.Start(pro);
                x.WaitForExit();
            }
           
            catch (Exception ex)
            {
                unzippedFileNStatus = "error " + ex.Message;
            }
            string[] array1 = Directory.GetFiles(destination);
            foreach (string name in array1)
            {
                ProcessStartInfo pro = new ProcessStartInfo();
                pro.WindowStyle = ProcessWindowStyle.Hidden;
                pro.FileName = zPath;
                pro.Arguments = "x \"" + destination + "\" -o" + destination;
                //pro.Arguments = "e -so " + source + " | " + zPath + " e -si -ttar -o " + destination;
                Process x = Process.Start(pro);
                x.WaitForExit();
            }
            String Output = ProcessVars.InputDirectory + @"Decrypted";

            String[] allfiles = System.IO.Directory.GetFiles(destination, "*.*", System.IO.SearchOption.AllDirectories);
            foreach (string nameF in allfiles)
            {
                if(nameF.IndexOf("tar") == -1)
                {
                    File.Copy(nameF, Output + "\\" + Path.GetFileName(nameF),true);
                    //File.Copy(nameF, dirLocal + "\\" + Path.GetFileName(nameF), true);
                    fotfiles++;
                }
            }
            return fotfiles;
        }
        public string Upload_SFTP(string justFile, string localfile, int totfilesin, string destination, int totTXT, int TotCSV)
        {
            string valReturn = "upload ok";

            if (totfilesin == 0)
                valReturn = "Not uploaded";

            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "CaptainCrunch";
            info.Pass = "Fr00tL00ps!";   //"C4pt4iN!336";//   "C!h3cks@Sci374";     // "H3althyBr3akfastN0t!";
            info.Host = "ftp.sciimage.com";
            //info.Host = "ftp3.sciimage.com";   //backup


            try
            {
                SshTransferProtocolBase sshCp;
                sshCp = new Sftp(info.Host, info.User, info.Pass);

                sshCp.OnTransferStart += new FileTransferEvent(sshCp_OnTransferStart);
                sshCp.OnTransferProgress += new FileTransferEvent(sshCp_OnTransferProgress);
                sshCp.OnTransferEnd += new FileTransferEvent(sshCp_OnTransferEnd);

                sshCp.Connect();
                try
                {
                    if (File.Exists(localfile))
                    {
                        sshCp.Put(localfile, destination + justFile);
                        Thread.Sleep(100);
                    }
                    else
                    {
                        valReturn = "local files does not exist...";
                    }
                }
                catch (Exception ex)
                {
                    valReturn = ex.Message;
                }

                sshCp.Close();
            }
            catch   (Exception exp)
            {
                valReturn = "Upload manually";
            }
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_uploaded");

            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;

            dbU.ExecuteScalar("Insert into HOR_parse_files_uploaded(SeqNum, FileName, FromLocation,ImportDate_Start,FilesIn,ftpsite,result,Txts,CSVs) values(" +
                                         Seqnum + ",'" + justFile + "','" + localfile + "','" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "', " + totfilesin + ",'" + info.Host + "','" + valReturn + "'," + totTXT + "," + TotCSV + ")");
            

             return valReturn;
        }


        private static void sshCp_OnTransferStart(string src, string dst, int transferredBytes, int totalBytes, string message)
        {
            //Console.WriteLine();
            //progressBar = new ConsoleProgressBar();
            //progressBar.Update(transferredBytes, totalBytes, message);
        }
        private static void sshCp_OnTransferProgress(string src, string dst, int transferredBytes, int totalBytes, string message)
        {
            //if (progressBar != null)
            //{
            //    progressBar.Update(transferredBytes, totalBytes, message);
            //}
        }
        private static void sshCp_OnTransferEnd(string src, string dst, int transferredBytes, int totalBytes, string message)
        {
            //if (progressBar != null)
            //{
            //    progressBar.Update(transferredBytes, totalBytes, message);
            //    progressBar = null;
            //}
        }
        public struct SshConnectionInfo
        {
            public string Host;
            public string User;
            public string Pass;
            public string Domain;
        }

        public string DownLoadFiles(string ftplocation, string FileName, string NetworkLocation, string user, string pwd)
        {
            string error = "";
            try
            {
                //download  FTP to network
                //System.Net.WebClient wc = new System.Net.WebClient();
                //wc.Credentials = new NetworkCredential(user, pwd);
                //wc.OpenRead(ftplocation);
                //Int64 bytes_total = Convert.ToInt64(wc.ResponseHeaders["Content-Length"]);

                string Dir = NetworkLocation;// +"/" + DateTime.Now.Year.ToString() + "_" + DateTime.Now.Month.ToString() + "_" + DateTime.Now.Day.ToString() + "/Source/";
                if (File.Exists(Dir + "/" + FileName))
                    File.Delete(Dir + "/" + FileName);

                WebClient request = new WebClient();
                request.Credentials = new NetworkCredential(user, pwd);
                //                request.OpenRead(ftplocation);
                //Int64 bytes_total= Convert.ToInt64(request.ResponseHeaders["Content-Length"]);

                byte[] fileData = null;


                fileData = request.DownloadData(new Uri(ftplocation));

                FileStream file = File.Create(Dir + "/" + FileName);
                file.Write(fileData, 0, fileData.Length);
                file.Close();
                //FtpWebRequest renameRequest = (FtpWebRequest)WebRequest.Create(ftplocation);
                //renameRequest.UseBinary = true;
                //renameRequest.UsePassive = true;
                //renameRequest.Credentials = new NetworkCredential(user, pwd);
                //renameRequest.KeepAlive = true;
                //renameRequest.Method = WebRequestMethods.Ftp.Rename;
                //renameRequest.RenameTo = "/IN/Processed/__" + FileName;

                //try
                //{

                //    FtpWebResponse renameResponse = (FtpWebResponse)renameRequest.GetResponse();

                //    //Console.WriteLine("Rename OK, status code: {0}, rename status description: {1}", renameResponse.StatusCode, renameResponse.StatusDescription);

                //    renameResponse.Close();
                //}
                //catch (WebException ex)
                //{
                //    Console.WriteLine("Rename failed, status code: {0}, rename status description: {1}", ((FtpWebResponse)ex.Response).StatusCode,
                //        ((FtpWebResponse)ex.Response).StatusDescription);
                //}


            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            return error;
        }
        public string DownLoadFilesAndMove(string ftplocation, string FileName, string NetworkLocation, string user, string pwd)
        {
            string error = "";
            try
            {

                //download  FTP to network

                string Dir = NetworkLocation;// +"/" + DateTime.Now.Year.ToString() + "_" + DateTime.Now.Month.ToString() + "_" + DateTime.Now.Day.ToString() + "/Source/";
                if (File.Exists(Dir + "/" + FileName))
                    File.Delete(Dir + "/" + FileName);

                WebClient request = new WebClient();
                request.Credentials = new NetworkCredential(user, pwd);

                byte[] fileData = null;


                fileData = request.DownloadData(new Uri(ftplocation));
                FileStream file = File.Create(Dir + "/" + FileName);
                file.Write(fileData, 0, fileData.Length);
                file.Close();

                //WebClient requestFTP = new WebClient();
                ////requestFTP = (FtpWebRequest)FtpWebRequest.Create(new Uri("ftp://" + ftpServer + "/" + "httpdocs/webroot/" + destination + "/" + fileName));
                //requestFTP = (FtpWebRequest)FtpWebRequest.Create(new Uri(ftplocation));
                //requestFTP.Proxy = null;
                //requestFTP.Credentials = new NetworkCredential(user, pwd);

                //string newFilename = FileName.Replace(".ftp", "");
                //requestFTP.Method = WebRequestMethods.Ftp.Rename;
                //requestFTP.RenameTo = newFilename;
                //requestFTP.GetResponse();
                //rename

                FtpWebRequest renameRequest = (FtpWebRequest)WebRequest.Create(ftplocation);
                renameRequest.UseBinary = true;
                renameRequest.UsePassive = true;
                renameRequest.Credentials = new NetworkCredential(user, pwd);
                renameRequest.KeepAlive = true;
                renameRequest.Method = WebRequestMethods.Ftp.Rename;
                renameRequest.RenameTo = "/Processed/__" + FileName;

                try
                {

                    FtpWebResponse renameResponse = (FtpWebResponse)renameRequest.GetResponse();

                    Console.WriteLine("Rename OK, status code: {0}, rename status description: {1}", renameResponse.StatusCode, renameResponse.StatusDescription);

                    renameResponse.Close();
                }
                catch (WebException ex)
                {
                    Console.WriteLine("Rename failed, status code: {0}, rename status description: {1}", ((FtpWebResponse)ex.Response).StatusCode,
                        ((FtpWebResponse)ex.Response).StatusDescription);
                }
            
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            return error;
        }

        public string NotDownLoadFile_just_Move(string ftplocation, string FileName, string user, string pwd)
        {
            string error = "";
            try
            {
                FtpWebRequest reqFTP;
                Uri serverFile = new Uri(ftplocation + FileName);
                reqFTP = (FtpWebRequest)FtpWebRequest.Create(serverFile);
                reqFTP.Method = WebRequestMethods.Ftp.Rename;
                reqFTP.UseBinary = true;
                reqFTP.Credentials = new NetworkCredential(user, pwd);
                reqFTP.RenameTo = "CIE_" + FileName;
                FtpWebResponse response = (FtpWebResponse)reqFTP.GetResponse();

                Console.WriteLine("Rename OK, status code: {0}, rename status description: {1}", response.StatusCode, response.StatusDescription);

                response.Close();

            }
            catch (WebException ex)
            {
            error = ex.Message;
                //Console.WriteLine("Rename failed, status code: {0}, rename status description: {1}", ((FtpWebResponse)ex.Response).StatusCode,
                //    ((FtpWebResponse)ex.Response).StatusDescription);
            }



            return error;
        }
        
        private static DataTable data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Seqnum", typeof(Int32));
            newt.Columns.Add("ZIP");
            newt.Columns.Add("Fname");
            newt.Columns.Add("Pages");
            newt.Columns.Add("FileInXML");
            return newt;
        }


        //===================================
        public void expand_HCVR_ZIP(DateTime dateProcess, string DirLocal)
        {
            //string DirLocal = ProcessVars.InputDirectory + "From_FTP";
           
                //@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Test";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            DirectoryInfo CRNJLTRZIPs = new DirectoryInfo(DirLocal);
            string unzipDirName = "";
            foreach (FileInfo f in CRNJLTRZIPs.GetFiles("HCVR*.zip"))
            {
                int totf = 0; int linenum = 0;
                string xmlName = "";
                DataTable datafromPdfs = data_Table();
                if (f.Name.IndexOf("__HCVRLTR_") == -1)
                {
                    try
                    {
                        Directory.CreateDirectory(DirLocal + "\\tmp");
                        using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + f.Name))
                        {

                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                {
                                    xmlName = Path.Combine(DirLocal, entry.FullName);
                                    entry.ExtractToFile(Path.Combine(DirLocal, entry.FullName), true);
                                    totf++;
                                }
                                else
                                {
                                    var row = datafromPdfs.NewRow();
                                    entry.ExtractToFile(Path.Combine(DirLocal + "\\tmp", entry.FullName), true);
                                    linenum++;
                                    row["Seqnum"] = linenum;
                                    row["ZIP"] = f.Name;
                                    row["Fname"] = entry.Name.ToString();
                                    int totPages = 0;
                                    PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\tmp", entry.FullName));
                                    totPages = readerP.NumberOfPages;
                                    row["Pages"] = totPages.ToString();
                                    row["FileInXML"] = "N";
                                    datafromPdfs.Rows.Add(row);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        totf = 0;
                    }

                    if (totf > 0)
                    {
                        foreach (DataRow row in datafromPdfs.Rows)
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_HCVR_Client_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                 f.Name + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        }
                        dbU.ExecuteScalar("update HOR_parse_files_downloaded set Unziped = 1 where filename = '" + f.Name + "'");
                        File.Move(f.FullName, f.Directory + "\\__" + f.Name);
                    }
                    else
                    {
                        foreach (DataRow row in datafromPdfs.Rows)
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_MAPAR_Client_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                                 f.Name + "','" + row["fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");
                        }
                        LogWriter logerror = new LogWriter();
                        logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + f.Name, "email");
                    }
                    if (xmlName.Length > 1)
                        process_xmlHCVR(xmlName);
                }
            }
        }

        public void process_xmlHCVR(string xmlName)
        {
            FileInfo filexml = new System.IO.FileInfo(xmlName);
            string fustFName = filexml.Name;
            string strsql = "delete from HOR_parse_HCVR_Client where filename = '" + fustFName + "'";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            dbU.ExecuteNonQuery(strsql);
            string dateHLGS = "";
            var fileDate = dbU.ExecuteScalar("select importDate_Start from HOR_parse_files_downloaded where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            if (fileDate != null)
            {
                dateHLGS = fileDate.ToString();
                dbU.ExecuteScalar("update HOR_parse_files_downloaded set Filesin = 1 where filename = '" + fustFName.Replace(".xml", "") + ".zip" + "'");
            }
            else
                dateHLGS = DateTime.Now.ToString();
            xmlupload_HCVR uploadMAP = new xmlupload_HCVR();
            uploadMAP.HCVR_UploadXML(xmlName, dateHLGS, DateTime.Now.ToString("yyyy-MM-dd"));
            string strsql2 = "update HOR_parse_HCVR_Receipt set filename = f.filename from HOR_parse_HCVR_Receipt R " +
                            "join HOR_parse_HCVR_Flight F on R.batchid = F.batchid and R.clienttransactionid = F.clienttransactionid " +
                            "where R.filename_xml = '" + fustFName + "'";
            dbU.ExecuteScalar(strsql2);
            ExportCSV_HCVR_BCC(fustFName, filexml.Directory.ToString(), "HOR_rpt_HCVR_BCC");


        }
        public void ExportCSV_HCVR_BCC(string filename, string directory, string StoreProc)
        {
            string result = "";
            DataTable DatesToExport = new DataTable();
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable plData = new DataTable();
            SqlParameter[] sqlParams;
            string pNameT = directory + @"\HNJH-PR_" + filename.Replace(".xml", "_toBCC.csv");

            string cassFileName = ProcessVars.gDMPs + @"\HNJH-PR_" + filename.Replace(".xml", "_toBCC.csv");


            if (File.Exists(pNameT))
                File.Delete(pNameT);

            sqlParams = new SqlParameter[] { new SqlParameter("@Ofilename", filename)};

            DataTable table_BCC = dbU.ExecuteDataTable(StoreProc, sqlParams);
            if (plData != null)
            {

                var fieldnames = new List<string>();
                fieldnames.Add("Recnum");
                fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
                fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
                fieldnames.Add("F14"); fieldnames.Add("NameLine1"); fieldnames.Add("NameLine2"); fieldnames.Add("AddressLine1"); fieldnames.Add("AddressLine2"); fieldnames.Add("Addr5"); fieldnames.Add("CSZ");

                createCSV createcsvT = new createCSV();
                bool resp = createcsvT.addRecordsCSV(pNameT, fieldnames);
                foreach (DataRow row in table_BCC.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < table_BCC.Columns.Count; index++)
                    {
                        if (index == 0)
                            rowData.Add(row[index].ToString());

                        else if (index == 1)
                        {
                            rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                            rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); ; rowData.Add("");
                            rowData.Add(row[index].ToString());
                        }
                        else if (index == 2)
                            rowData.Add(row[index].ToString());
                        else if (index == 3)
                            rowData.Add(row[index].ToString());
                        else if (index == 4)
                            rowData.Add(row[index].ToString());
                        else if (index == 5)
                        {
                            rowData.Add(""); rowData.Add(row[index].ToString());
                        }
                    }
                    resp = false;
                    resp = createcsvT.addRecordsCSV(pNameT, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }
                //copy to CASS
                
                File.Copy(pNameT, cassFileName);
                var tR = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * 3);
                });
                tR.Wait();
                FileInfo fileInfo = new System.IO.FileInfo(cassFileName);
                DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
                if (File.Exists(processedFiles + "\\" + fileInfo.Name.Replace(".csv", "-OUTPUT.csv")))
                {
                    DataTable QualifiedRecords = readcsvHNJH(processedFiles + fileInfo.Name.Replace(".csv", "-OUTPUT.csv"));
                    string resultUPD = "";
                    if (QualifiedRecords.Rows.Count > 0)
                    {
                        resultUPD = updateTableSQL_HNJH(QualifiedRecords, "Y", filename, "HOR_parse_HCVR_Receipt");
                    }
                    string erroFile = ProcessVars.gODMPsMedicaid + fileInfo.Name.Replace(".csv", "-NON-DELIVERABLE.csv");
                    if (File.Exists(erroFile))
                    {
                        DataTable NonD_Records = readcsvError(erroFile);

                        if (NonD_Records.Rows.Count > 0)
                        {
                           
                            foreach (DataRow row in NonD_Records.Rows)
                            {
                                //TextBox1.Text = row["Recordnum"].ToString();
                                string strsql = "update  HOR_parse_HCVR_Receipt set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
                                dbU.ExecuteNonQuery(strsql);
                            }
                        }
                    }
                    //string pName = directory + "\\" + filename.Replace(".xml", "_toSCI.csv") ;
                    //DataTable dataTocsv = dbU.ExecuteDataTable("Select batchTransactionID as TransactionID, batchid as dlgUId, filename as FName, UpdAddr1 as coverPageName, UpdAddr5 as coverPageAddress1, UpdAddr2 as coverPageAddress2,  UpdAddr3 as coverPageAddress3, UpdAddr4 as coverPageAddress4,UpdCity as coverPageCity, UpdState as coverPageState, UpdZip as coverPageZIP " +
                    //                        ", Recnum , imbdig " +
                    //                        "from HOR_parse_HCVR_Receipt where DL = 'Y'  and filename_xml = '" + filename + "' order by recnum");
                    //createCSV createFilecsv = new createCSV();
                    //createFilecsv.printCSV_fullProcess(pName, dataTocsv, "", "N");
                }

               // ProcessBackData();
            }
            //return result;

        }
        public string updateTableSQL_HNJH(DataTable inputdata, string to_DL, string ffName, string TableName)
        {
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from tmp_From_CASS_HNJH");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                //bulkCopy.DestinationTableName =
                //    "[dbo].[Tempo_fsaData]";
                bulkCopy.DestinationTableName = "[dbo].[tmp_From_CASS_HNJH]";

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(inputdata);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;
                    updErrors++;
                }
            }
            Connection.Close();
            try
            {
                SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@dataTable", TableName), new SqlParameter("@DLvalue", to_DL), new SqlParameter("@ffName", ffName) };

                DataSet ds2 = new DataSet();

                ds2 = dbU.ExecuteDataSet("Update_From_CassHNJH_HCVR", sqlParams2);
              

            }
            catch (Exception ex)
            {
                LogWriter logerror = new LogWriter();
                logerror.WriteLogToTable("Update From Cass", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Update", "TableName :  " + TableName + " filename " + ffName + " " + ex.Message, "email");
                errors = errors + ex.Message;
                updErrors++;
            }
            return errors;
        }
        public DataTable readcsvHNJH(string fileName)
        {
            DataTable dataToUpdate = Result_data_Table();
            dataToUpdate.Columns.Add("IMBChar", typeof(String));
            dataToUpdate.Columns.Add("IMBDig", typeof(String));
            int currLine = 0;
            int valueOk = 0;
            string line;
            System.IO.StreamReader file =
           new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                currLine++;
                if (currLine == 1)
                    if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,RETURN_FIELD_13,RETURN_FIELD_14,NAME_FULL,ADDRESS_LINE_3,ALT_ADDRESS_2,ALT_ADDRESS_1,DELIVERY_ADDRESS,CITY,St,ZIP+4,De,Re,IMB CHARACTERS,IMB DIGITS")
                        valueOk = 1;

                    else if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,ADDRESS_LINE_3,ALT_ADDRESS_2,ALT_ADDRESS_1,DELIVERY_ADDRESS,CITY,St,ZIP+4,De,Re,IMB CHARACTERS,IMB DIGITS")
                        valueOk = 1;
                    else if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,St,ZIP+4,De,Re,Intelligent Mail barcode,Intelligent Mail barcode")
                        valueOk = 1;
                    else

                        valueOk = 0;

                if (currLine > 1 && valueOk == 1)
                {
                    Regex csvSplit = null;
                    var row = dataToUpdate.NewRow();
                    //var lineO = line.Split(',').ToList();
                    csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
                    int xMatch = 0;
                    foreach (Match match in csvSplit.Matches(line.ToString()))
                    {
                        // was 5
                        if (xMatch == 0)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 11)
                            row["County"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 14)
                            row["Uaddr1"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 15)
                            row["Uaddr2"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 16)
                            row["Uaddr3"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 17)
                            row["Uaddr4"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 18)
                            row["Uaddr5"] = match.Value.Replace("\"", "").Replace(",", "");

                        if (xMatch == 19)
                            row["City"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 20)
                            row["State"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 21)
                            row["Zip"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 24)
                            row["IMBChar"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 25)
                            row["IMBDig"] = match.Value.Replace("\"", "").Replace(",", "");
                        xMatch++;
                    }
                    dataToUpdate.Rows.Add(row);
                }

            }

            file.Close();
            return dataToUpdate;

        }
        public DataTable readcsvError(string fileName)
        {
            DataTable dataToUpdate = Result_data_Table();
            int currLine = 0;
            int valueOk = 0;
            string line;
            System.IO.StreamReader file =
           new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                currLine++;
                if (currLine == 1)
                    if (line.Replace("\"", "") == "Sysout,Sheet_count,Jobname,PrintDate,ArchiveDate,C_Recnum,Seq,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,County Name,Latitude,Longitude,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,St,ZIP+4,De,Re")

                        valueOk = 1;
                    else if (line.Replace("\"", "") == "RECNO,RETURN_FIELD_01,RETURN_FIELD_02,RETURN_FIELD_03,RETURN_FIELD_04,RETURN_FIELD_05,RETURN_FIELD_06,RETURN_FIELD_07,RETURN_FIELD_08,RETURN_FIELD_09,RETURN_FIELD_10,RETURN_FIELD_11,RETURN_FIELD_12,RETURN_FIELD_13,RETURN_FIELD_14,NAME_FULL,DELIVERY_ADDRESS,ALT_ADDRESS_1,ALT_ADDRESS_2,ADDRESS_LINE_3,CITY,State,ZIP+4,Delivery Point,Return Code")
                        valueOk = 1;
                    else
                        valueOk = 0;

                if (currLine > 1 && valueOk == 1)
                {
                    Regex csvSplit = null;
                    var row = dataToUpdate.NewRow();
                    //var lineO = line.Split(',').ToList();
                    csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
                    int xMatch = 0;
                    foreach (Match match in csvSplit.Matches(line.ToString()))
                    {//  WAS 5
                        if (xMatch == 0)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 14)
                            row["Uaddr1"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 15)
                            row["Uaddr2"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 16)
                            row["Uaddr3"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 17)
                            row["Uaddr4"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 18)
                            row["Uaddr5"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 19)
                            row["City"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 20)
                            row["State"] = match.Value.Replace("\"", "").Replace(",", "");
                        if (xMatch == 21)
                            row["Zip"] = match.Value.Replace("\"", "").Replace(",", "");
                        xMatch++;
                    }
                    dataToUpdate.Rows.Add(row);
                }
                if (currLine > 1 && valueOk == 2)
                {
                    Regex csvSplit = null;
                    var row = dataToUpdate.NewRow();
                    //var lineO = line.Split(',').ToList();
                    csvSplit = new Regex("(?:^|,)(\"(?:[^\"]+|\"\")*\"|[^,]*)", RegexOptions.Compiled);
                    int xMatch = 0;
                    foreach (Match match in csvSplit.Matches(line.ToString()))
                    {
                        if (xMatch == 1)
                            row["Recnum"] = match.Value.Replace("\"", "").Replace(",", "");

                        xMatch++;
                    }
                    dataToUpdate.Rows.Add(row);
                }
            }

            file.Close();
            return dataToUpdate;

        }

        private static DataTable Result_data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("Uaddr1");
            newt.Columns.Add("Uaddr2");
            newt.Columns.Add("Uaddr3");
            newt.Columns.Add("Uaddr4");
            newt.Columns.Add("Uaddr5");
            newt.Columns.Add("City");
            newt.Columns.Add("State");
            newt.Columns.Add("Zip");
            newt.Columns.Add("County");
         
            return newt;
        }

        public void MoveFile(string ftpURL, string UserName, string Password, string ftpDirectory, string ftpDirectoryProcessed, string FileName)
        {
            FtpWebRequest ftpRequest = null;
            FtpWebResponse ftpResponse = null;
            try
            {
                ftpRequest = (FtpWebRequest)WebRequest.Create(ftpURL +  ftpDirectory +  FileName);
                ftpRequest.Credentials = new NetworkCredential(UserName, Password);
                ftpRequest.UseBinary = true;
                ftpRequest.UsePassive = true;
                ftpRequest.KeepAlive = true;
                ftpRequest.Method = WebRequestMethods.Ftp.Rename;
                ftpRequest.RenameTo = ftpDirectoryProcessed +  FileName;
                ftpResponse = (FtpWebResponse)ftpRequest.GetResponse();
                ftpResponse.Close();
                ftpRequest = null;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
