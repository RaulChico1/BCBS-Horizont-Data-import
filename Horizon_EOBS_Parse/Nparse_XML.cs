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
using System.Xml.Linq;
using System.Configuration;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{
    public struct SshConnectionInfo
    {
        public string Host;
        public string User;
        public string Pass;
    }
    public class Nparse_XML
    {
        DBUtility dbU;
        int Seqnum = 1;
        private FtpWebRequest ftpRequest = null;
        private FtpWebResponse ftpResponse = null;
        private Stream ftpStream = null;
        private int bufferSize = 2048;

        public string loadXML_Renewal(DateTime DateofProcess , string DirName)
        {
            string result = "";
            DirectoryInfo xmlS = new DirectoryInfo(DirName);
            FileInfo[] files = xmlS.GetFiles("*.xml");
            string errors = "";
            foreach (FileInfo file in files)
            {
                process_xml(file.FullName, file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            }
         
           
            return result;
        }
        public DataTable XElementToDataTable(XElement x)
        {
            DataTable dtable = new DataTable();

            XElement setup = (from p in x.Descendants() select p).First();
            // build your DataTable
            foreach (XElement xe in setup.Descendants())
            {
                DataColumnCollection columns = dtable.Columns;

                if (!columns.Contains(xe.Name.ToString()))
                 {
                     dtable.Columns.Add(new DataColumn(xe.Name.ToString(), typeof(string))); // add columns to your dt
                 }
            }
            var all = from p in x.Descendants(setup.Name.ToString()) select p;
            foreach (XElement xe in all)
            {
                DataRow dr = dtable.NewRow();
                foreach (XElement xe2 in xe.Descendants())
                    dr[xe2.Name.ToString()] = xe2.Value; //add in the values
                dtable.Rows.Add(dr);

            }
            return dtable;
        }
        public string process_xml(string filename, string dateinfile)
        {
            string result = "";
            try
            {
                XElement xele = XElement.Load(filename);//get your file
                // declare a new DataTable and pass your XElement to it
                DataTable dt = XElementToDataTable(xele);
                //dt.Columns.Add("Value", type(System.Int32), 0);
                //dt.Columns.Add("Recnum", typeof(int));
                dt.Columns.Add("dateProcess", typeof(string));
                dt.Columns.Add("ImportDate", typeof(string));
                dt.Columns.Add("FileName", typeof(string));

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                //dbU.ExecuteScalar("delete from HOR_parse_TMP_HCVRLTR");
                foreach (DataRow row in dt.Rows)
                {
                    row["dateProcess"] = dateinfile; // DateofProcess.ToString("yyyy/MM/dd");
                    row["ImportDate"] = dateinfile;  //DateofProcess.ToString("yyyy/MM/dd");
                    row["FileName"] = Path.GetFileName(filename);  //filename;
                }

                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    //bulkCopy.DestinationTableName =
                    //    "[dbo].[Tempo_fsaData]";
                    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_TMP_HCVRLTR]";

                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(dt);
                    }
                    catch (Exception ex)
                    {
                        var errors = ex.Message;
                    }
                }
                Connection.Close();
            }
            catch (Exception ex)
            {
                 LogWriter logerror = new LogWriter();
                 logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import XML",
                     Path.GetFileName(filename));
                                    
            }
            return result;
        }
        public string downloadFiles(DateTime dateProcess, bool Ticket1)
        {
            string filestoImport = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            appSets appsets = new appSets();
            var fileToImport = appsets.getFilesImport("Ticket01");

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
            string ftpSubDir = "sftp.cierant.com/HorizonBCBS/REPORTING RENEWAL MAILING/";
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/REPORTING RENEWAL MAILING"));
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
                            compareValue = lastModifiedDate.CompareTo((DateTime.Today.AddDays(-1)));
                        else
                            compareValue = lastModifiedDate.CompareTo((dateProcess));
                        //responseD.Close();
                        var lastDay = lastModifiedDate.ToShortDateString();
                        string xmlDate = lastDay;
                        // check if is today
                        if (compareValue == 1 || compareValue != 1)
                        {
                            if ((Ticket1 && filename.IndexOf("ElementCards") != -1) ||
                                (!Ticket1))
                            {
                                //check if already uploaded
                                listFiles = listFiles + filename.Replace("REPORTING RENEWAL MAILING/", "") + "____";
                                countFiles++;
                                var fileU = dbU.ExecuteScalar("select FileName from HOR_parse_files_downloaded where FileName = '" + filename.Replace("REPORTING RENEWAL MAILING/", "") + "'");
                                //+ "' and CONVERT(DATE,ImportDate_Start)=CONVERT(DATE,GETDATE()
                                if (fileU != null)
                                { }
                                else
                                {
                                    DirLocal = ProcessVars.InputDirectory + @"RRenewalM";
                                    string ext = result.Ext;

                                    try
                                    {

                                        string resultDownload = DownLoadFiles(info.Host + "/" + filename, filename.Replace("REPORTING RENEWAL MAILING/", ""), DirLocal, info.User, info.Pass);
                                        LogWriter logerror = new LogWriter();
                                        logerror.WriteLogToTable("file downloaded", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", info.Host + "/" + filename + " " + filesInzip, "email", resultDownload);

                                        Seqnum++;
                                    }
                                    catch (Exception ex)
                                    {
                                        LogWriter logerror = new LogWriter();
                                        error = ex.Message;
                                        logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);
                                    }


                                    if (ext == "zip" && filename.IndexOf("HCVRLTR") != -1)
                                    {
                                        string xmlNme = "";
                                        
                                        int totf = 0;
                                        try
                                        {
                                            using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("REPORTING RENEWAL MAILING/", "")))
                                            {

                                                foreach (ZipArchiveEntry entry in archive.Entries)
                                                {

                                                    if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "RRenewalM", entry.FullName));
                                                        totf++;
                                                        xmlNme = ProcessVars.InputDirectory + @"RRenewalM\" + entry.FullName;
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
                                            string resultXml = process_xml(xmlNme, xmlDate);
                                            if (resultXml == "")
                                            {
                                                dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                    Seqnum + ",'" + filename.Replace("REPORTING RENEWAL MAILING/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                    lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                    DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");
                                            }
                                            else
                                            {
                                                LogWriter logerror = new LogWriter();
                                                logerror.WriteLogToTable(resultXml, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error XML " + info.Host + "/" + filename + " " + filesInzip, "email");
                                            }
                                        }
                                        else
                                        {
                                            dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                                                Seqnum + ",'" + filename.Replace("REPORTING RENEWAL MAILING/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                                                lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                                                DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");
                                            LogWriter logerror = new LogWriter();
                                            logerror.WriteLogToTable("no files in xml", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in xml " + info.Host + "/" + filename + " " + filesInzip, "email");
                                        }
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


                fileData = request.DownloadData(new Uri(ftplocation));

                FileStream file = File.Create(Dir + "/" + FileName);
                file.Write(fileData, 0, fileData.Length);
                file.Close();
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            return error;
        }

        //public string unzip_and_process(string directory)
        //{
        //    DirectoryInfo originaZips = new DirectoryInfo(directory);
        //    FileInfo[] filesZ = originaZips.GetFiles("*.zip");

        //     foreach (FileInfo zipFile in filesZ)
        //     {
        //         if (zipFile.Extension == "zip" && zipFile.Name.IndexOf("HCVRLTR") != -1)
        //         {
        //             string xmlNme = "";

        //             int totf = 0;
        //             try
        //             {
        //                 using (ZipArchive archive = ZipFile.OpenRead(DirLocal + @"\" + filename.Replace("REPORTING RENEWAL MAILING/", "")))
        //                 {

        //                     foreach (ZipArchiveEntry entry in archive.Entries)
        //                     {

        //                         if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
        //                         {
        //                             entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "RRenewalM", entry.FullName));
        //                             totf++;
        //                             xmlNme = ProcessVars.InputDirectory + @"RRenewalM\" + entry.FullName;
        //                         }
        //                     }
        //                 }
        //             }
        //             catch (Exception ex)
        //             {
        //                 totf = 0;
        //             }

        //             if (totf > 0)
        //             {
        //                 string resultXml = process_xml(xmlNme, xmlDate);
        //                 if (resultXml == "")
        //                 {
        //                     dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
        //                         Seqnum + ",'" + filename.Replace("REPORTING RENEWAL MAILING/", "") + "','" + ext + "',1,'" + info.Host + "','" +
        //                         lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
        //                         DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");
        //                 }
        //                 else
        //                 {
        //                     LogWriter logerror = new LogWriter();
        //                     logerror.WriteLogToTable(resultXml, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error XML " + info.Host + "/" + filename + " " + filesInzip, "email");
        //                 }
        //             }
        //             else
        //             {
        //                 dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
        //                     Seqnum + ",'" + filename.Replace("REPORTING RENEWAL MAILING/", "") + "','" + ext + "',1,'" + info.Host + "','" +
        //                     lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
        //                     DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");
        //                 LogWriter logerror = new LogWriter();
        //                 logerror.WriteLogToTable("no files in xml", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in xml " + info.Host + "/" + filename + " " + filesInzip, "email");
        //             }
        //         }
        //     }
        //}
    }
}
