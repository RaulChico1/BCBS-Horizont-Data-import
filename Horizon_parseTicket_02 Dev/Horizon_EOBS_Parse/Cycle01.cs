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
using System.Threading.Tasks;
namespace Horizon_EOBS_Parse
{
    public class Cycle01
    {
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

            }
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

            N_loadFromFTP downloadDta = new N_loadFromFTP();
            string result = downloadDta.downloadDataTicket01(GlobalVar.DateofFilesToProcess, "Ticket1");


            MainProcess processParse = new MainProcess();
            processParse.MainProcessParse("1");

            //=====

            DBUtility dbU;

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            //string strsql = "delete from HOR_parse_HLGS where CONVERT(DATE,ImportDate)= '" + GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd") + "'";
            //dbU.ExecuteNonQuery(strsql);

            //DirectoryInfo originaZips = new DirectoryInfo(ProcessVars.InputDirectory + @"\From_FTP");
            //FileInfo[] filesZ = originaZips.GetFiles("*.zip");
            //filesZ.Count();

            //string extractPath = ProcessVars.InputDirectory + "From_FTP";


            //NParse_pdfs parse_pdfs = new NParse_pdfs();
            //string ResultsPdf = parse_pdfs.zipFilesinDirService("", extractPath);
            //==

            create_Tickets01();

            string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var t = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 10);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t.Wait();

            string time2 = t.Result;


            appSets appsets = new appSets();
            appsets.setVars();

            BackCASS processRedturns = new BackCASS();
            Results = processRedturns.ProcessFiles("");


            DataTable zipGroup = dbU.ExecuteDataTable("select distinct zipgroup from HOR_parse_Category_Master where zipgroup is not null");
            if (zipGroup.Rows.Count > 0)
            {
               
                foreach (DataRow row in zipGroup.Rows)
                {
                    string strgroup = "";
                    DataTable groups = dbU.ExecuteDataTable("select code from HOR_parse_Category_Master where zipgroup = '" + row["zipgroup"].ToString() + "'");
                    foreach (DataRow rowg in groups.Rows)
                    {
                        strgroup = strgroup + rowg[0].ToString() + ",";
                    }
                    createZip_UploadN(row["zipgroup"].ToString(), strgroup.Substring(0,strgroup.Length -1));
                    //createZip_Upload("CONBILL", "EPB");
                    //createZip_Upload("GRPBILL", "EP0GH");
                    //createZip_Upload("EOBS", "UCDS");
                    //createZip_Upload("CHECKS", "EPA,EPM");
                    //createZip_Upload("CHECKS_Test", "UCDS");
                }
            }

            createEmail createemail = new createEmail();
            createemail.produceSummary_Uploaded();
            createemail.produceSummary_Errors_Cycle_01();
            Results = "Process Ticket 01 ready " + System.Environment.NewLine + time1 + System.Environment.NewLine + time2;

            switch (System.DateTime.Today.DayOfWeek)
            {
                case DayOfWeek.Saturday:
                    Directory.Move(ProcessVars.InputDirectory, ProcessVars.EOWDirectory);
                    break;
                case DayOfWeek.Sunday:
                    Directory.Move(ProcessVars.InputDirectory, ProcessVars.EOWDirectory);
                    break;
                default:
                    break;
            }



            return Results;
            //essageBox.Show("Download pgp. Process DONE!!!!", "Process Status");
        }

        public void create_Tickets01()
        {
           
            DBUtility dbU;

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am");

            Export_XLSX export = new Export_XLSX();
            if (resultsTicket01.Rows.Count > 0)
                export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "01");

            DataTable resultsTicket01aD = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_epb");
            if (resultsTicket01aD.Rows.Count > 0)
                export.CreateExcelFile(resultsTicket01aD, ProcessVars.InputDirectory + @"From_FTP\", "01_EPBs");


            DataTable resultsTicketTest = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_Test01");
            if (resultsTicketTest.Rows.Count > 0)
                export.CreateExcelFile(resultsTicketTest, ProcessVars.InputDirectory + @"From_FTP\", "01_Test");
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
                if (gName == "CONBILL" || gName == "GRPBILL")
                    resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/Bills/", totTxt, TotCSV);
                else
                    resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/EOB_Check/", totTxt, TotCSV);


                LogWriter logEndProcess = new LogWriter();
                logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);
            }
        }

        public void createZip_Upload(string gName, string group)
        {
            ZipFiles zipresult = new ZipFiles();
            int totTxt = 0;
            int TotCSV = 0;
            string zipName = zipresult.ManuallyCreateZipFile(gName,group, out totTxt, out TotCSV);
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
    }
 
}
