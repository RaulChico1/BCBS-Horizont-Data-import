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

namespace Horizon_EOBS_Parse
{
    
    public class N_loadFromFTP
    {
        DBUtility dbU;
        int Seqnum = 1;
        private FtpWebRequest ftpRequest = null;
        private FtpWebResponse ftpResponse = null;
        private Stream ftpStream = null;
        private int bufferSize = 2048;

        public string downloadData(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles(dateProcess, false, "Ticket2");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return "";
        }

        public string downloadDataCR(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles(dateProcess, false, "Ticket2s");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return "";
        }

        public string downloadDataTicket01(DateTime dateProcess, string option)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFiles(dateProcess, true, option);
            return result;
        }
        public string downloadDataMRDF(DateTime dateProcess)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string result = downloadFilesMRDF(dateProcess, "MRDF");
            //ProcessVars.InputDirectory + @"\from_FTP"

            return result;
        }
        public string downloadFilesMRDF(DateTime dateProcess, string option)
        {
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport(option);
            DataTable manuallyDownloaded = dbU.ExecuteDataTable("select FileName from HOR_parse_files_downloaded_Mrdf_imb where FilesIn = 0");
            foreach (DataRow row in manuallyDownloaded.Rows)
            {
                string fname = row["filename"].ToString();
                if (!File.Exists(ProcessVars.InputDirectory + @"from_FTP\" + fname))
                    dbU.ExecuteScalar("delete from HOR_parse_files_downloaded_Mrdf_imb where FilesIn = 0 and filename = '" + fname + "'");
            }
            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "CaptainCrunch";
            info.Pass = "H3althyBr3akfastN0t!";  
            info.Host = "ftp://sftp.cierant.com";  

            int steps = 0;

            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass);

            FtpWebResponse response = (FtpWebResponse)request.GetResponse();
            DateTime lastModifiedDate = response.LastModified;


            Stream responseStream = response.GetResponseStream();
            StreamReader reader = new StreamReader(responseStream);
            string NetworkDIR = ProcessVars.networkDir + GlobalVar.DateofProcess.ToString("yyyy_MMMM") + "\\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"_test\";


            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded_Mrdf_imb");
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
                    var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded_Mrdf_imb where FileName = '" + filename + "'");
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
                            try
                            {
                                dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded_Mrdf_imb(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                    Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                    lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                    DateTime.Now.ToString("yyyy-MM-dd") + "'," + totfiles.ToString() + ")");
                            }
                            catch (Exception exx)
                            {
                                var eroor = exx.Message;
                            }
                        }
                    }

                }
                else
                    if (filename.IndexOf(".txt") != -1)
                        NotDownLoadFile_just_Move(info.Host + "/" + filename, filename, DirLocal, info.User, info.Pass);
            }


            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);

            return "";
        }
        public string NotDownLoadFile_just_Move(string ftplocation, string FileName, string NetworkLocation, string user, string pwd)
        {
            string error = "";
            try
            {

                //download  FTP to network

                //string Dir = NetworkLocation;// +"/" + DateTime.Now.Year.ToString() + "_" + DateTime.Now.Month.ToString() + "_" + DateTime.Now.Day.ToString() + "/Source/";
                //if (File.Exists(Dir + "/" + FileName))
                //    File.Delete(Dir + "/" + FileName);

                //WebClient request = new WebClient();
                //request.Credentials = new NetworkCredential(user, pwd);

                //byte[] fileData = null;


                //fileData = request.DownloadData(new Uri(ftplocation));
                //FileStream file = File.Create(Dir + "/" + FileName);
                //file.Write(fileData, 0, fileData.Length);
                //file.Close();



                FtpWebRequest renameRequest = (FtpWebRequest)WebRequest.Create(ftplocation);
                renameRequest.UseBinary = true;
                renameRequest.UsePassive = true;
                renameRequest.Credentials = new NetworkCredential(user, pwd);
                renameRequest.KeepAlive = true;
                renameRequest.Method = WebRequestMethods.Ftp.Rename;
                renameRequest.RenameTo = "/Processed/" + FileName;

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
        public string uploadftpTEST(string remoteFile, string localFile)
        {
            string valReturn = "upload ok";
            string subDir = "/Usr/CaptainCrunch/CaptainCrunch/Bills/";
            string user = "CaptainCrunch";
            string pass = "Fr00tL00ps!"; //"C4pt4iN!336";  // "C!h3cks@Sci374";     //"H3althyBr3akfastN0t!";
            string host = "ftp://ftp.sciimageftp.com";
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
                catch (Exception ex) { valReturn = ex.Message; }
            
            return "Test";
        }
        public string uploadftp(string remoteFile, string localFile, int totfilesin, string subDir, int totTXT, int TotCSV)
        {
            
            string valReturn = "upload ok";
            if(totfilesin == 0)
                valReturn = "Not uploaded";
            string user = "CaptainCrunch";
            string pass = "Fr00tL00ps!";   //"C4pt4iN!336";  //"C!h3cks@Sci374"; // "H3althyBr3akfastN0t!";
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

        public string Upload_SFTP(string justFile, string localfile, int totfilesin, string destination, int totTXT, int TotCSV)
        {
            string valReturn = "upload ok";

            if (totfilesin == 0)
                valReturn = "Not uploaded";

            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "CaptainCrunch";
            info.Pass = "Fr00tL00ps!";   //"C4pt4iN!336";  //"C!h3cks@Sci374";    //"H3althyBr3akfastN0t!";
            info.Host = "ftp.sciimage.com";
            //info.Host = "ftp3.sciimage.com";   //backup


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

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_uploaded");

            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;

            dbU.ExecuteScalar("Insert into HOR_parse_files_uploaded(SeqNum, FileName, FromLocation,ImportDate_Start,FilesIn,ftpsite,result,Txts,CSVs) values(" +
                                         Seqnum + ",'" + justFile + "','" + localfile + "',GETDATE(), " + totfilesin + ",'" + info.Host +  destination + "','" + valReturn + "'," + totTXT + "," + TotCSV + ")");


            return valReturn;
        }

        public string downloadFiles(DateTime dateProcess, bool Ticket1, string option)
        {
            int gpgCount = 0;
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport(option);

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
                        if(Ticket1)
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
                                    DirLocal = ProcessVars.InputDirectory + @"from_FTP";
                                newdir:
                                    gpgCount++;
                                    string DirLocal_TMP = ProcessVars.InputDirectory + @"from_FTP\tmp_"+ gpgCount;
                                    if (System.IO.Directory.Exists(DirLocal_TMP))
                                        goto newdir;
                                    System.IO.Directory.CreateDirectory(DirLocal_TMP);
                                    string ext = result.Ext;

                                    try
                                    {
                                        string downlResult = "0";
                                        int totTry = 0;
                                       do 
                                       {
                                        string resultDownload = DownLoadFiles(info.Host + "/" + filename, filename.Replace("IN/", ""), DirLocal, info.User, info.Pass);
                                        if (resultDownload.ToString() == "")
                                            downlResult = "1";
                                        else
                                            totTry++;
                                        LogWriter logerror = new LogWriter();
                                        logerror.WriteLogToTable("file downloaded", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import",info.Host + "/" + filename + " " + filesInzip, "email",resultDownload);
                                       } while (downlResult == "0" && totTry < 4);
                                        Seqnum++;
                                    }
                                    catch (Exception ex)
                                    {
                                        LogWriter logerror = new LogWriter();
                                        error = ex.Message;
                                        logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);
                                    }

                                    if (ext == "pgp")
                                    {
                                        File.Copy(DirLocal + @"\" + filename.Replace("IN/", ""), DirLocal_TMP + @"\" + filename.Replace("IN/", ""));

                                        string resultDecrypt = DecryptFile(DirLocal_TMP + @"\" + filename.Replace("IN/", ""));  //DirLocal + @"\" + filename.Replace("IN/", "")
                                        LogWriter logerror = new LogWriter();
                                        logerror.WriteLogToTable("file decrypted", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", info.Host + "/" + filename + " " + filesInzip, "email", resultDecrypt);
                                        if (resultDecrypt.IndexOf("error") == -1)
                                        {
                                           // filesInzip = UnzipFile(resultDecrypt, ProcessVars.InputDirectory + "Decrypted");
                                            filesInzip = UnzipFile(resultDecrypt, DirLocal_TMP+ "Decrypted");
                                            //DirectoryInfo dirDecrypted = new DirectoryInfo(DirLocal_TMP);
                                            //FileInfo[] filesZ = dirDecrypted.GetFiles("*.*");
                                            //foreach (FileInfo filenameD in filesZ)
                                            //{
                                            //    File.Delete(filenameD.FullName);

                                            //}



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
                                        try
                                        {
                                            using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("IN/", "")))
                                            {

                                                foreach (ZipArchiveEntry entry in archive.Entries)
                                                {
                                                    if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName));
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
                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");


                                        }
                                        else
                                        {
                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");
                                            LogWriter logerror = new LogWriter();
                                            logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + info.Host + "/" + filename + " " + filesInzip, "email");
                                        }
                                    }
                                    if (ext == "zip" && filename.IndexOf("CRNJLTR") == -1)
                                    {
                                        DirectoryInfo originaZips = new DirectoryInfo(DirLocal);
                                        FileInfo[] filesZ = originaZips.GetFiles("*.zip");
                                        filesZ.Count();
                                        filesInzip = 0;
                                        string extractPath = ProcessVars.InputDirectory + "From_FTP";

                                        foreach (FileInfo zipFile in filesZ)
                                        {
                                            if (zipFile.Name.ToUpper() == filename.Replace("IN/", "").ToUpper())
                                            {
                                                string JustFName = zipFile.Name;

                                                if (JustFName.IndexOf("HLGS") != -1)
                                                {
                                                    //"Care Radius_") == 0 || JustFName.IndexOf("CRNJLTR_
                                                    //zipName = zipFile;

                                                    try
                                                    {
                                                        filesInzip++;
                                                        System.IO.Compression.ZipFile.ExtractToDirectory(zipFile.FullName, extractPath);
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        //filesInzip = filesInzip;
                                                        LogWriter logErrorProcess = new LogWriter();
                                                        logErrorProcess.WriteLogToTable("error extracting ZIP", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "File name: " + JustFName + " files in zip " + filesInzip + " Msg:" + ex.Message);

                                                    }
                                                }
                                            }
                                        }
                                        dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                            Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" + lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "'," + filesInzip + ")");

                                    }
                                    if (ext == "pdf")
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


            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);

            return "";
        }
        public string downloadFiles_ID_Cards(DateTime dateProcess)
        {
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
                if(filename == "IN/CON2_20151123_NSR_NASCO_HIX_PROCESSED.zip")
                    listFiles = listFiles;
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
                                { }
                                else
                                {
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


                                    if (ext == "zip")
                                    {
                                         string subdirname = filename.Replace("IN/", "").Replace("_PROCESSED.zip", "");
                                        if (!Directory.Exists(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname))
                                            Directory.CreateDirectory(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname);
                                        int totf = 0;
                                        try
                                        {
                                            using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("IN/", "")))
                                            {

                                                foreach (ZipArchiveEntry entry in archive.Entries)
                                                {
                                                    //if (entry.FullName.ToUpper().Contains("PRINTING INSTRUCTIONS/"))

                                                    //    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName.ToUpper().Replace("PRINTING INSTRUCTIONS/", "")));
                                                    //if (entry.FullName.ToUpper().Contains("INSERT/"))
                                                    //    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName.ToUpper().Replace("INSERT/", "")));
                                                    //if (entry.FullName.ToUpper().Contains("INSERTS/"))
                                                    //    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName.ToUpper().Replace("INSERTS/", "")));
                                                    //else
                                                    //    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName));
                                                    entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.Name));
                                                    totf++;

                                                }
                                            }

                                            //move zip into dir
                                            File.Move(DirLocal + "\\" + filename.Replace("IN/", ""), ProcessVars.InputDirectory + @"ID_Cards\" + subdirname + "\\" + filename.Replace("IN/", ""));
                                        }
                                        catch (Exception ex)
                                        {
                                            totf = 0;
                                        }

                                        if (totf > 0)
                                        {
                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");


                                        }
                                        else
                                        {
                                            LogWriter logerror = new LogWriter();
                                            logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + info.Host + "/" + filename + " " + filesInzip, "email");
                                        }
                                    }

                                }
                                //other
                           // }
                        }
                    }

                }
                else
                    filestoImport = filestoImport + "\n\n" + filename;
            }


            LogWriter logEndProcess = new LogWriter();
            logEndProcess.WriteLogToTable("end of download", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import  from " + info.Host, "Count:" + countFiles + " __" + listFiles);

            return "";
        }
        public void unzip_ID_Cards()
        {
            string extractPath = ProcessVars.InputDirectory + "From_FTP";
            DirectoryInfo IDCardsZips = new DirectoryInfo(extractPath);
            FileInfo[] filesZ = IDCardsZips.GetFiles("*.zip");
          
            foreach (FileInfo filename in filesZ)
            {
                if ((filename.Name.ToString().IndexOf("GRP2_") != -1 || filename.Name.ToString().IndexOf("CON2_") != -1) ||
                     filename.Name.ToString().IndexOf("Bed Bath and Beyond_") != -1 || filename.Name.ToString().IndexOf("Heavy and General Laborers_") != -1
                     || filename.Name.ToString().IndexOf("OMNIA_") != -1)
                {
                    if (!Directory.Exists(ProcessVars.InputDirectory + "ID_Cards"))
                        Directory.CreateDirectory(ProcessVars.InputDirectory + "ID_Cards");


                    if (filename.Extension.ToString() == ".zip")
                    {
                        string subdirname = filename.Name.ToString().Replace("IN/", "").Replace("_PROCESSED.zip", "");
                        if (!Directory.Exists(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname))
                            Directory.CreateDirectory(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname);
                        int totf = 0;
                        try
                        {
                            //using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("IN/", "")))
                            using (ZipArchive archive = ZipFile.OpenRead(filename.FullName.ToString()))
                            {
                                string newname = "";
                                foreach (ZipArchiveEntry entry in archive.Entries)
                                {
                                    //if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                    //{
                                    if (entry.FullName.Contains("printing instructions") || entry.FullName.Contains("Inserts"))
                                    {
                                        newname = "";
                                        entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName.Replace("printing instructions/", "").Replace("Inserts/", "")));
                                    }
                                    else
                                        entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + @"ID_Cards\" + subdirname, entry.FullName));

                                    totf++;
                                    //}
                                }
                            }
                            //move zip into dir
                            //File.Move(DirLocal + "\\" + filename.Replace("IN/", ""), ProcessVars.InputDirectory + @"ID_Cards\" + subdirname + "\\" + filename.Replace("IN/", ""));
                            File.Move(filename.FullName.ToString(), ProcessVars.InputDirectory + @"ID_Cards\" + subdirname + "\\" + filename.Name.ToString());
                        }
                        catch (Exception ex)
                        {
                            totf = 0;
                        }

                     
                    }


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
            string errorUnzipping = "";
            FileInfo info = new FileInfo(zippedFilePath);
            string unzippedFileNStatus = "ok";
            //string zPath = @"C:\Program Files (x86)\7-Zip\7z.exe";
            string zPath = @"C:\Program Files\7-Zip\\7z.exe";  //
            if (System.IO.File.Exists(zPath))
            { }
            else
            {
                zPath = @"C:\Program Files (x86)\7-Zip\\7z.exe";
                if (System.IO.File.Exists(zPath))
                { }
                else
                {
                    errorUnzipping = "Unzip 7 nor present";
                }
            }
            if (errorUnzipping == "")
            {
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
                try
                {
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
                        if (nameF.IndexOf("tar") == -1)
                        {
                            File.Copy(nameF, Output + "\\" + Path.GetFileName(nameF), true);
                            //File.Copy(nameF, dirLocal + "\\" + Path.GetFileName(nameF), true);
                            fotfiles++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    unzippedFileNStatus = "error " + ex.Message;
                }
            }
            return fotfiles;
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
        }

        public string DownLoadFiles(string ftplocation, string FileName, string NetworkLocation, string user, string pwd)
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


                fileData = request.DownloadData(new Uri(ftplocation ));
             
                FileStream file = File.Create(Dir + "/" + FileName);
                file.Write(fileData, 0, fileData.Length);
                file.Close();
            }
            catch(Exception ex)
            {
                error = ex.Message;
            }

            return error;
        }
    }
}
