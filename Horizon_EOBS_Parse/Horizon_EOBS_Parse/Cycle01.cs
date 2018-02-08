using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;

using System.IO;
using Horizon_EOBS_Parse;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Data.SqlClient;


namespace Horizon_EOBS_Parse
{
    public class Cycle01
    {
        DBUtility dbU;
        int totzipsCycle = 0;
        public string Process_AdditionalLCDS()
        {
            string Results = "";
            MainProcess processParse = new MainProcess();
            processParse.MainProcessParse("3");
            string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var t = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 5);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t.Wait();

            string time2 = t.Result;


            appSets appsets = new appSets();
            appsets.setVars();

            BackCASS processRedturns = new BackCASS();
            Results = processRedturns.ProcessFiles("");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "adtlLCDS");
            //GlobalVar.adtLCDS
            //string[] processed = GlobalVar.adtLCDS.Split(',');
            //foreach (string fileN in processed)
            //{
            //    if (fileN.Length > 1)
            //        createZip_adtLCDS(fileN, fileN);
            //}


            Results = "Additional LCDS ready Create the ZIP files manually for: " + GlobalVar.adtLCDS.ToString();
            return Results;
        }
        public string ProcessTicket01()
        {
            string Results = "";

            appSets checkD = new appSets();
            string drivesOk = checkD.checkDrives();

            while ((drivesOk != ""))
            {
                Thread.Sleep(2 * 60 * 1000);
                if (drivesOk == "")
                {
                    break;
                }
            }

            //var dateProcess = GlobalVar.DateofFilesToProcess;
            System.IO.DirectoryInfo downloadedMessageInfo = new DirectoryInfo(ProcessVars.gODMPs);
            try
            {
                foreach (FileInfo file in downloadedMessageInfo.GetFiles())
                {
                    file.Delete();
                }
            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                LogWriter logerror = new LogWriter();
                logerror.WriteLogToTable("error deletig files from dmps", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Parsing files", "start cycle", ex.Message);


            }
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);
           
            
            //string testFName = "01_HZService_download_Start_" + DateTime.Now.ToString("MM_dd_yyyy__HH_mm_ss") + ".txt";
            //if (File.Exists(ProcessVars.statusDir + testFName))
            //    File.Delete(ProcessVars.statusDir + testFName);
            //    File.WriteAllText(ProcessVars.statusDir + testFName, "Service start at " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + 
            //    Environment.NewLine + "Process date " + GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd"));


            N_loadFromFTP downloadDta = new N_loadFromFTP();
            string result = downloadDta.downloadDataTicket01(GlobalVar.DateofFilesToProcess, "Ticket1");

            int totfilesToProcess = Directory.GetFiles(ProcessVars.InputDirectory + @"\Decrypted", "*", SearchOption.TopDirectoryOnly).Length;
            if (totfilesToProcess > 0)
            {

                string[] arrayEOBS = new string[] { "FIM", "F101" };
                System.IO.DirectoryInfo filestoMove = new DirectoryInfo(ProcessVars.InputDirectory + @"\Decrypted");
                string[] Not_Process = new string[] { "SIT", "UAT" };
                System.IO.DirectoryInfo filestoMoveNP = new DirectoryInfo(ProcessVars.InputDirectory + @"\Decrypted");

                foreach (FileInfo filem in filestoMoveNP.GetFiles())
                {
                    if (Not_Process.Any(filem.Name.Contains))
                    {
                        string moveName = ProcessVars.InputDirectory + @"\notProcessed\" + filem.Name;
                        File.Move(filem.FullName, moveName);
                    }
                }
                foreach (FileInfo filem in filestoMove.GetFiles())
                {
                    if (arrayEOBS.Any(filem.Name.Contains))
                    {
                        // string moveName = ProcessVars.InputDirectory + @"\notProcessed\" + filem.Name;
                        string moveName = ProcessVars.InputDirectory + @"\FEP\" + filem.Name;
                        File.Move(filem.FullName, moveName);
                    }
                }
               


                MainProcess processParse = new MainProcess();
                processParse.MainProcessParse("1");

                
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                string strsqlCheck = "select * from HOR_parse_files_to_CASS where Msg like '%ID%' and convert(date,importdate) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'";
                DataTable nosysid = dbU.ExecuteDataTable(strsqlCheck);
                if (nosysid.Rows.Count > 0)
                {
                    SendMails sendmail = new SendMails();
                    for (int i = 0; i < 10; i++)
                    {
                        sendmail.SendMailFatalError("Horizon Files with NO SYS ID" + nosysid.Rows.Count.ToString(), "Error inProcess", "\n\n" + "Horizon Files with NO SYS ID" + nosysid.Rows.Count.ToString(), "");
                    }
                }
                create_Tickets01(GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));

                string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                var t = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * 5);
                    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                });
                t.Wait();

                string time2 = t.Result;
                int tottimes = 1;

            checkAgain:
                appSets appsets = new appSets();
                appsets.setVars();

                BackCASS processRedturns = new BackCASS();
                Results = processRedturns.ProcessFiles("");

                
                //DataTable FilesReprocess = dbU.ExecuteDataTable("select * from HOR_parse_files_to_CASS where CONVERT(date,ImportDate) = '" + GlobalVar.DateofProcess + "' and CASSReceiveDate is null and Processed is null");
               DataTable FilesReprocess = dbU.ExecuteDataTable("select * from HOR_parse_files_to_CASS where CONVERT(date,DateProcess) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and CASSReceiveDate is null and Processed is null");
                if (FilesReprocess.Rows.Count > 0)
                {



                    foreach (DataRow row in FilesReprocess.Rows)
                    {
                        string fname = row["FileNameCASS"].ToString();
                        if (File.Exists(ProcessVars.InputDirectory + fname))
                            File.Copy(ProcessVars.InputDirectory + fname, ProcessVars.gDMPs + fname);
                    }
                    string time1R = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    var tR = Task.Run(async delegate
                    {
                        await Task.Delay(1000 * 60 * 2);
                        return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    });
                    tR.Wait();
                    tottimes++;
                    if (tottimes > 5)
                    {
                        SendMails sendmail = new SendMails();
                        for (int i = 0; i < 10; i++)
                        {
                            sendmail.SendMailFatalError("Horizon_Checking Directories", "Error inProcess", "\n\n" + FilesReprocess.Rows.Count.ToString() + " files get NO BCC some files", "");
                        }
                    }
                    else
                        goto checkAgain;
                }



                DataTable zipGroup = dbU.ExecuteDataTable("select distinct zipgroup from HOR_parse_Category_Master where zipgroup is not null");
                if (zipGroup.Rows.Count > 0)
                {
                    totzipsCycle = 0;
                    foreach (DataRow row in zipGroup.Rows)
                    {
                        string strgroup = "";
                        DataTable groups = dbU.ExecuteDataTable("select code from HOR_parse_Category_Master where zipgroup = '" + row["zipgroup"].ToString() + "'");
                        foreach (DataRow rowg in groups.Rows)
                        {
                            strgroup = strgroup + rowg[0].ToString() + ",";
                        }
                        createZip_UploadN(row["zipgroup"].ToString(), strgroup.Substring(0, strgroup.Length - 1));
                 
                    }
                }
                parse_FIM_TF101();


                createEmail createemail = new createEmail();

                string[] fileZips = Directory.GetFiles(ProcessVars.InputDirectory + @"\FromCASS", "*.zip", SearchOption.TopDirectoryOnly);
                if (fileZips.Length > 0)
                {
                    createemail.produceSummary_Uploaded();
                }


                //testFName = "09_OK_End_of_Process_2am_" + DateTime.Now.ToString("MM_dd_yyyy__HH_mm_ss") + ".txt";
                //if (File.Exists(ProcessVars.statusDir + testFName))
                //    File.Delete(ProcessVars.statusDir + testFName);
                //File.WriteAllText(ProcessVars.statusDir + testFName, "Service end succesfully at " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") +
                //Environment.NewLine + "Process date " + GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd"));


                createemail.produceSummary_Errors_Cycle_01();
                Results = "Process Ticket 01 ready " + System.Environment.NewLine + time1 + System.Environment.NewLine + time2;

                Nparse_UPPR additionalfiles = new Nparse_UPPR();
                string results = additionalfiles.processUPPRs();
                if (results != "")
                    createemail.specific_error_cycle("Error UPPRs", results);
            }
            else
            {
                string[] fileN = Directory.GetFiles(ProcessVars.InputDirectory + @"\from_FTP", "*.gpg", SearchOption.TopDirectoryOnly);
                SendMails sendmail = new SendMails();
                sendmail.SendMail("NO Files in ZIP  " + DateTime.Now.ToString("yyyy-MM-dd"), "jcioban@apps.cierant.com, rchico@apps.cierant.com,khubner@apps.cierant.com", "noreply@apps.cierant.com", "No files in ZIP ");


                //testFName = "09_NoFiles_End_of_Process_2am_" + DateTime.Now.ToString("MM_dd_yyyy__HH_mm_ss") + ".txt";
                //if (File.Exists(ProcessVars.statusDir + testFName))
                //    File.Delete(ProcessVars.statusDir + testFName);
                //File.WriteAllText(ProcessVars.statusDir + testFName, "Service end succesfully at " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") +
                //Environment.NewLine + "Process date " + GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd"));


            }
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            var totRecs2 = dbU.ExecuteScalar("select count(right(ZipGroup,1)) from HOR_parse_Category_Master where ZipGroup is not null and right(ZipGroup,1) = '2'");
            int totRecsTopUpdate = Convert.ToInt16(totRecs2.ToString());

            switch (System.DateTime.Today.DayOfWeek)
            {
                case DayOfWeek.Saturday:

                    if (totRecsTopUpdate == 0)
                        Directory.Move(ProcessVars.InputDirectory, ProcessVars.EOWDirectory.Substring(0, (ProcessVars.EOWDirectory.Length) - 1) + "_1st");
                    else
                        Directory.Move(ProcessVars.InputDirectory, ProcessVars.EOWDirectory);
                    break;
                case DayOfWeek.Sunday:

                    if (totRecsTopUpdate == 0)
                        Directory.Move(ProcessVars.InputDirectory, ProcessVars.EOWDirectory.Substring(0, (ProcessVars.EOWDirectory.Length) - 1) + "_1st");
                    else
                        Directory.Move(ProcessVars.InputDirectory, ProcessVars.EOWDirectory);
                    break;
                default:
                    if (totRecsTopUpdate == 0)
                        Directory.Move(ProcessVars.InputDirectory, ProcessVars.InputDirectory.Substring(0, (ProcessVars.InputDirectory.Length) - 1) + "_1st");
                    break;
            }



            return Results;
            //essageBox.Show("Download pgp. Process DONE!!!!", "Process Status");
        }

        public void create_Tickets01(string dateofProcess)
        {
            DBUtility dbU;
            SqlParameter[] sqlParams2;
            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@Pdate", dateofProcess) };


            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_date",sqlParams2);
            //DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am");

            Export_XLSX export = new Export_XLSX();
            if (resultsTicket01.Rows.Count > 0)
                export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "01", dateofProcess);

            //DataTable resultsTicket01aD = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_epb");
            //if (resultsTicket01aD.Rows.Count > 0)
            //    export.CreateExcelFile(resultsTicket01aD, ProcessVars.InputDirectory + @"From_FTP\", "01_EPBs", dateofProcess);


            //DataTable resultsTicketTest = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_Test01", sqlParams2);
            //if (resultsTicketTest.Rows.Count > 0)
            //    export.CreateExcelFile(resultsTicketTest, ProcessVars.InputDirectory + @"From_FTP\", "01_Test", dateofProcess);
        }

        public void createZip_UploadN(string gName, string group)
        {
            ZipFiles zipresult = new ZipFiles();
            int totTxt = 0;
            int TotCSV = 0;
            string zipName = zipresult.ManuallyCreateZipFile(gName, group, out totTxt, out TotCSV);
            if (totTxt + TotCSV > 0)
            {
                string justFilename = zipName;//.Substring(0, zipName.LastIndexOf("_"));
                //int filesIN = Convert.ToInt32(zipName.Substring(zipName.LastIndexOf("_") + 1, zipName.Length - zipName.LastIndexOf("_") - 1));
                FileInfo fiConn = new FileInfo(justFilename);

                // copy zip to network
                string NDirectory = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\Con_GRP_Bills\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
                string Network_pName = NDirectory + "\\" + fiConn.Name;
                if (!Directory.Exists(NDirectory))
                    Directory.CreateDirectory(NDirectory);

                if (File.Exists(Network_pName))
                    File.Delete(Network_pName);
                File.Copy(justFilename, Network_pName);

                N_loadFromFTP uploadZip = new N_loadFromFTP();

                string resultUpload = "";
                if (gName.Replace("2", "") == "CONBILL" || gName.Replace("2", "") == "GRPBILL")
                {
                    totzipsCycle++;
                    //resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/Bills/", totTxt, TotCSV);
                    resultUpload = uploadZip.Upload_SFTP(fiConn.Name, justFilename, totTxt + TotCSV, "/Bills/", totTxt, TotCSV);
                }
                else if (gName.Replace("2", "") == "LCDS")
                {
                    totzipsCycle++;
                    //resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/Bills/", totTxt, TotCSV);
                    resultUpload = uploadZip.Upload_SFTP(fiConn.Name, justFilename, totTxt + TotCSV, "/Letters/", totTxt, TotCSV);
                }
                else
                {
                    totzipsCycle++;
                    //resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/EOB_Check/", totTxt, TotCSV);
                    resultUpload = uploadZip.Upload_SFTP(fiConn.Name, justFilename, totTxt + TotCSV, "/EOB_Check/", totTxt, TotCSV);
                }

                LogWriter logEndProcess = new LogWriter();
                logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);
            }
        }

        public void createZip_Upload(string gName, string group)
        {
            ZipFiles zipresult = new ZipFiles();
            int totTxt = 0;
            int TotCSV = 0;
            string zipName = zipresult.ManuallyCreateZipFile(gName, group, out totTxt, out TotCSV);
            if (totTxt + TotCSV > 0)
            {
                string justFilename = zipName;//.Substring(0, zipName.LastIndexOf("_"));
                //int filesIN = Convert.ToInt32(zipName.Substring(zipName.LastIndexOf("_") + 1, zipName.Length - zipName.LastIndexOf("_") - 1));
                FileInfo fiConn = new FileInfo(justFilename);

                // copy zip to network
                string NDirectory = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\Con_GRP_Bills\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
                string Network_pName = NDirectory + "\\" + fiConn.Name;
                if (!Directory.Exists(NDirectory))
                    Directory.CreateDirectory(NDirectory);

                if (File.Exists(Network_pName))
                    File.Delete(Network_pName);
                File.Copy(justFilename, Network_pName);

                N_loadFromFTP uploadZip = new N_loadFromFTP();

                string resultUpload = "";
                if (gName == "CONBILL" || gName == "GRPBILL")
                    resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/Bills/", totTxt, TotCSV);
                else
                    resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/EOB_Check/", totTxt, TotCSV);


                LogWriter logEndProcess = new LogWriter();
                logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);
            }
        }

        public void createZip_adtLCDS(string gName, string group)
        {
            ZipFiles zipresult = new ZipFiles();
            int totTxt = 0;
            int TotCSV = 0;
            string zipName = zipresult.ManuallyCreateADTLZipFile(gName, group, out totTxt, out TotCSV);
            if (totTxt + TotCSV > 0)
            {
                string justFilename = zipName;//.Substring(0, zipName.LastIndexOf("_"));
                //int filesIN = Convert.ToInt32(zipName.Substring(zipName.LastIndexOf("_") + 1, zipName.Length - zipName.LastIndexOf("_") - 1));
                FileInfo fiConn = new FileInfo(justFilename);

                // copy zip to network
                string NDirectory = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\Con_GRP_Bills\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
                string Network_pName = NDirectory + "\\" + fiConn.Name;
                if (!Directory.Exists(NDirectory))
                    Directory.CreateDirectory(NDirectory);

                if (File.Exists(Network_pName))
                    File.Delete(Network_pName);
                File.Copy(justFilename, Network_pName);

                N_loadFromFTP uploadZip = new N_loadFromFTP();

                string resultUpload = "";

                resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/AdditionalLCDS/", totTxt, TotCSV);


                LogWriter logEndProcess = new LogWriter();
                logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);
            }
        }
        public void parse_FIM_TF101()
        {
            string errors = "";
            appSets appsets = new appSets();

            DBUtility dbU;

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string version = "";
             var totRecs2 = dbU.ExecuteScalar("select count(right(ZipGroup,1)) from HOR_parse_Category_Master where ZipGroup is not null and right(ZipGroup,1) = '2'");
                int totRecsTopUpdate = Convert.ToInt16(totRecs2.ToString());
                if (totRecsTopUpdate > 0)
                    version = "2_";


            string jProcess = @"C:\Program Files (x86)\Jprocess\processTextFile.exe";
            appsets.setVars();
            int totfilesToProcess = Directory.GetFiles(ProcessVars.InputDirectory + @"\FEP", "*", SearchOption.TopDirectoryOnly).Length;
            if (totfilesToProcess > 0)
            {
                DirectoryInfo csvs = new DirectoryInfo(ProcessVars.InputDirectory + @"\FEP");

                FileInfo[] CsVfiles = csvs.GetFiles("*FIM*.txt");

               
                foreach (FileInfo file in CsVfiles)
                {
                    errors = "";
                    if (file.Name.IndexOf("__") == -1)
                    {
                        try
                        {
                            ProcessStartInfo pro = new ProcessStartInfo();
                            pro.WindowStyle = ProcessWindowStyle.Hidden;
                            pro.FileName = jProcess;
                            pro.Arguments = " \"" + file.FullName + "\"";
                            //pro.Arguments = "e -so " + source + " | " + zPath + " e -si -ttar -o " + destination;
                            Process x = Process.Start(pro);
                            x.WaitForExit();



                        }

                        catch (Exception ex)
                        {
                            errors = "error " + ex.Message;
                        }
                        if (errors == "")
                        {
                            string cvsfilename = file.Directory + "\\" + file.Name.Replace(".txt", ".csv");
                            NParse_UCDS processUUPS = new NParse_UCDS();
                            //errors = processUUPS.assigRecnums_F101(cvsfilename);
                            errors = processUUPS.DIR_Upload_CSV(cvsfilename);
                            if (errors == "ok")
                            {
                                File.Move(cvsfilename, cvsfilename.Replace(".csv", "_original.csv"));
                                File.Move(cvsfilename.Replace(".csv", "_updated.csv"), cvsfilename);

                                string zipname = file.Directory + "\\FIM_" + version + DateTime.Now.ToString("yyyy_MM_dd") + ".zip";
                                ZipFiles zipIt = new ZipFiles();
                                //zipIt.zipF10_FIM(zipname, cvsfilename, file);
                                try
                                {
                                    zipIt.zipAR06(zipname, cvsfilename, file.Name.Replace(".txt", ".csv"));
                                    zipIt.zipAR06(zipname, cvsfilename.Replace(".csv", ".pdf"), file.Name.Replace(".txt", ".pdf"));
                                    zipIt.zipAR06(zipname, cvsfilename.Replace(".csv", ".txt"), file.Name);


                                    string nfilename = file.Directory + "\\__" + file.Name.Replace(".txt", ".csv");
                                    if (File.Exists(nfilename))
                                        File.Delete(nfilename);
                                    File.Move(cvsfilename, nfilename);

                                    nfilename = file.Directory + "\\__" + file.Name;
                                    if (File.Exists(nfilename))
                                        File.Delete(nfilename);
                                    File.Move(file.FullName, nfilename);
                                }
                                catch (Exception ex)
                                {
                                    errors = "error " + ex.Message;
                                }

                            }
                            else
                            {
                                var stopHere = errors;
                            }
                        }
                        else
                        {
                            var stopHere = errors;
                        }
                    }
                }
                FileInfo[] TF_files = csvs.GetFiles("*F101*.txt");

                errors = "";
                foreach (FileInfo file in TF_files)
                {
                    NParse_UCDS processUUPS = new NParse_UCDS();
                    errors = processUUPS.evaluate_EOB_101_TXT(file.FullName, "", "");
                    if (errors == "")
                    {
                        string zipname = file.Directory + "\\F101_" + version + DateTime.Now.ToString("yyyy_MM_dd") + ".zip";

                        ZipFiles zipIt = new ZipFiles();

                        zipIt.zipAR06(zipname, file.FullName.Replace(".txt", ".csv"), file.Name.Replace(".txt", ".csv"));
                        zipIt.zipAR06(zipname, file.FullName, file.Name);

                        string nfilename = file.Directory + "\\__" + file.Name;
                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(file.FullName, nfilename);
                    }
                }
            }
            DirectoryInfo zips = new DirectoryInfo(ProcessVars.InputDirectory + @"\FEP");

            FileInfo[] Zipfiles = zips.GetFiles("*.zip");

            errors = "";
            foreach (FileInfo file in Zipfiles)
            {
                int totfiles = 0; string FilesCSV = ""; string FilesTXT = "";
                using (ZipArchive archive = ZipFile.OpenRead(file.FullName))
                {
                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))

                            FilesTXT = FilesTXT + entry.Name + "~";
                        else
                            FilesCSV = FilesCSV + entry.Name + "~";
                        // entry.ExtractToFile(Path.Combine(extractPath, entry.FullName));
                        totfiles++;
                    }

                }


                dbU.ExecuteNonQuery("Insert into HOR_parse_Log_Zips (Logdate, Type, Zipname, ZipCount, csvNames, txtNames) Values (getdate(),'Ticket01 zip','" +
                   file.Name + "'," + totfiles + ",'" + FilesCSV + "','" + FilesTXT + "')");


                N_loadFromFTP uploadZip = new N_loadFromFTP();

                string resultUpload = "";
                if (file.Name.IndexOf("AR06") == 0)
                    resultUpload = uploadZip.Upload_SFTP(file.Name, file.FullName, 2, "/Letters/", 1, 1);
                else if (file.Name.IndexOf("F101") == 0 || file.Name.IndexOf("FIM") == 0)
                    resultUpload = uploadZip.Upload_SFTP(file.Name, file.FullName, 2, "/EOB_Check/", 1, 1);


                LogWriter logEndProcess = new LogWriter();
                logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + file.Name);
            }

        }


    }

}
