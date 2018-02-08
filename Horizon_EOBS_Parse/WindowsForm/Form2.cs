using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;
using Horizon_EOBS_Parse;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Globalization;
using System.Data.SqlClient;


namespace WindowsForm
{

    public partial class Form2 : Form
    {
        DBUtility dbU;
        public Form2()
        {
            InitializeComponent();
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            appSets appsets = new appSets();
            appsets.setVars();
            DirectoryInfo originaZips = new DirectoryInfo(ProcessVars.InputDirectory + @"\From_FTP");

            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\from_FTP");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Decrypted");

            FileInfo[] filesZ = originaZips.GetFiles("*.pdf");
            filesZ.Count();
        }

        private void label1_DoubleClick(object sender, EventArgs e)
        {

            FolderBrowserDialog fbd = new FolderBrowserDialog();
            fbd.SelectedPath = label1.Text;
            DialogResult result = fbd.ShowDialog();

            string[] files = Directory.GetFiles(fbd.SelectedPath);
            System.Windows.Forms.MessageBox.Show("Files found: " + files.Length.ToString(), "Message");

            label1.Text = fbd.SelectedPath;
        }

        public void button1_Click(object sender, EventArgs e)
        {
            MainProcess processParse = new MainProcess();
            processParse.MainProcessParse("NG");

            //int totfilesPrecessed = 0;
            //DBUtility dbU;
            //string Parsed = "";
            //int filesinDir = 0;
            //GlobalVar.dbaseName = "BCBS_Horizon";
            //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            //DirectoryInfo originaTXTs = new DirectoryInfo(ProcessVars.InputDirectory + @"\Decrypted");
            //FileInfo[] files = originaTXTs.GetFiles("*.txt");
            //string errors = "";
            //foreach (FileInfo file in files)
            //{
            //    var ProcessedFName = dbU.ExecuteScalar("select FileName from HOR_parse_files_to_CASS where filename = '" + file.Name + "'");
            //    if ( ProcessedFName == null)
            //    {
            //        if (file.Name.ToUpper().IndexOf("ALGS") == 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_ALGS Algs = new NParse_ALGS();
            //                string error = Algs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("EP005703") == 0)   // final notice
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_EP Eps = new NParse_EP();
            //                string error = Eps.evaluate_EP005703(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("EP") == 0 && file.Name.ToUpper().IndexOf("EP005703") != 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_EP Eps = new NParse_EP();
            //                string error = Eps.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("IM") == 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_IM IMs = new NParse_IM();
            //                string error = IMs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("NAR") == 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_NAR NARs = new NParse_NAR();
            //                string error = NARs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("PND") == 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_PND PNDs = new NParse_PND();
            //                string error = PNDs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("QMLL") == 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_QMLL QMLLs = new NParse_QMLL();
            //                string error = QMLLs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("UCDS") == 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_UCDS UCDSs = new NParse_UCDS();
            //                string error = UCDSs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //        if (file.Name.ToUpper().IndexOf("NPR") == 0)
            //        {
            //            try
            //            {
            //                filesinDir++;
            //                NParse_NPR NPRs = new NParse_NPR();
            //                string error = NPRs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
            //                totfilesPrecessed++;
            //                if (error != "")
            //                    errors = errors + error + "\n\n";
            //                else
            //                    Parsed = Parsed + file.Name;
            //            }
            //            catch (Exception ez)
            //            {
            //                errors = errors + file + "  " + ez.Message + "\n\n";
            //            }
            //        }
            //    }
            //}
            //if(errors != "")
            //{
            //    LogWriter logerror = new LogWriter();

            //    logerror.WriteLogToTable(ex.Message, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "error downloading " + info.Host + "/" + filename);

            //}
            //DirectoryInfo originaZips = new DirectoryInfo(ProcessVars.InputDirectory + @"\From_FTP");
            //FileInfo[] filesZ = originaZips.GetFiles("*.zip");
            //filesZ.Count();

            //string extractPath = ProcessVars.InputDirectory + "From_FTP";

            //foreach (FileInfo zipFile in filesZ)
            //{
            //    string JustFName = zipFile.Name;

            //    if (JustFName.IndexOf("HLGS") != -1)
            //    {
            //        //"Care Radius_") == 0 || JustFName.IndexOf("CRNJLTR_
            //        //zipName = zipFile;

            //        try
            //        {

            //            System.IO.Compression.ZipFile.ExtractToDirectory(zipFile.FullName, extractPath);
            //        }
            //        catch (Exception ex)
            //        {

            //        }
            //    }
            //}
            //NParse_pdfs parse_pdfs = new NParse_pdfs();
            //string ResultsPdf = parse_pdfs.zipFilesinDirService("",extractPath);

            //Results.Text = errors + "\n\n" + "Files processed: " + totfilesPrecessed + "  files in directory: " + filesinDir; 
        }

        private void Form2_Load(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            pd.Text = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            fd.Text = GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd");
            label1.Text = ProcessVars.InputDirectory;
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\from_FTP");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Decrypted");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\MRDF");
            //MessageBox.Show("Download pgp.","Process Status");
            //button4.PerformClick();

        }
        private void Form2_FormClosing(object sender, FormClosingEventArgs e)
        {
            System.Windows.Forms.Application.Exit();
        }
        private void button2_Click(object sender, EventArgs e)
        {
            string result = "";
            //string fromFTP = ProcessVars.InputDirectory + @"\from_FTP";
            string fromFTP = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2015-06-10\from_FTP";
            DirectoryInfo originalFTPs = new DirectoryInfo(fromFTP);
            FileInfo[] files = originalFTPs.GetFiles("*.gpg");

            Encry_Worker encry = new Encry_Worker();

            foreach (var fileP in files)
            {
                try
                {
                    //string upDir = Directory.GetParent(fileP.DirectoryName).FullName;
                    string newFname = fileP.FullName.Substring(0, fileP.FullName.Length - 4) + "tar.bz2";
                    encry.DecryptFile(fileP.FullName, newFname);
                }
                catch (Exception ex)
                {
                    result = ex.Message;
                }
            }
            if (result == "")
                result = "DONE";
            Results.Text = result;
        }

        private void button3_Click(object sender, EventArgs e)
        {
            string fromFTP = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2015-06-10\from_FTP";
            string extractPath = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2015-06-10\Decrypted";
            DirectoryInfo originalFTPs = new DirectoryInfo(fromFTP);
            FileInfo[] files = originalFTPs.GetFiles("*.zip");


            foreach (var zipFile in files)
            {
                try
                {

                    System.IO.Compression.ZipFile.ExtractToDirectory(zipFile.FullName, extractPath);
                }
                catch (Exception ex)
                {

                }
            }
        }
        private void button4_Click(object sender, EventArgs e)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            label4.Text = "ZipGroups: " + dbU.ExecuteScalar("SELECT  ZipGroup = STUFF((SELECT distinct ', ' +  ZipGroup FROM HOR_parse_Category_Master WHERE ZipGroup is not null and status = 'Ticket01' FOR XML PATH(''), TYPE).value('.[1]', 'nvarchar(max)'), 1, 2, '') FROM HOR_parse_Category_Master AS x GROUP BY Status").ToString();
            for (int ntimes = 0; ntimes < 1000; ntimes++)
            {
                var totRecs2 = dbU.ExecuteScalar("select count(right(ZipGroup,1)) from HOR_parse_Category_Master where ZipGroup is not null and right(ZipGroup,1) = '2'");
                int totRecsTopUpdate = Convert.ToInt16(totRecs2.ToString());
                if (totRecsTopUpdate > 0)
                    dbU.ExecuteNonQuery("update HOR_parse_Category_Master set ZipGroup =  substring(ZipGroup,1,len(ZipGroup)-1) where ZipGroup is not null");


                DateTime nextRun1 = DateTime.Today.AddDays(+1).AddHours(2).AddMinutes(40);
                TimeSpan diff = nextRun1.Subtract(DateTime.Now);
                int totalMinutes = (int)diff.TotalMinutes;
                Results.Text = Results.Text + " Waiting " + totalMinutes + " minutes  , cycle: " + ntimes;
                var t0 = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * totalMinutes);
                    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                });
                t0.Wait();
                cycle01();
                System.Threading.Thread.Sleep(40000);

                dbU.ExecuteNonQuery("update HOR_parse_Category_Master set ZipGroup =  ZipGroup + '2' where ZipGroup is not null");

                dbU.ExecuteNonQuery("update HOR_parse_Log_Zips set Type = 'Ticket01 zip 1st' where convert(date,logdate) = convert(date,getdate()) and Type = 'Ticket01 zip'");
                

                DateTime nextRun2 = DateTime.Today.AddDays(+0).AddHours(5).AddMinutes(5);
                TimeSpan diff2 = nextRun2.Subtract(DateTime.Now);
                int totalMinutes2 = (int)diff2.TotalMinutes;
                Results.Text = Results.Text + " Waiting " + totalMinutes2 + " minutes  , cycle: " + ntimes;
                var t2 = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * totalMinutes2);
                    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                });
                t2.Wait();
                cycle01();
                System.Threading.Thread.Sleep(40000);
                dbU.ExecuteNonQuery("update HOR_parse_Category_Master set ZipGroup =  substring(ZipGroup,1,len(ZipGroup)-1) where ZipGroup is not null");
                
                button41_Click(button41, EventArgs.Empty);  //  execute:   dbU.ExecuteScalar("MRDF_Update_IDCards");
                                                            // table:  MRDF_ID_CARDS_Historical
                button43_Click(button41, EventArgs.Empty);

            }
        }
        public void cycle01()
        {
           
            Results.Text = "Processing Ticket 01...";
          
            appSets appsets = new appSets();
            appsets.setVars();
            pd.Text = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            fd.Text = GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd");
            label1.Text = ProcessVars.InputDirectory;
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\from_FTP");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Decrypted");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\notProcessed");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\fromCass");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\FEP");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\fromBCC");
            //System.IO.Directory.CreateDirectory(ProcessVars.statusDir);

            Cycle01 cycle01 = new Cycle01();
            Results.Text = cycle01.ProcessTicket01();
            //objPleaseWait.Close();
        }
        private void button4_OLD_Click(object sender, EventArgs e)
        {
            if (ProcessVars.Prefix == "TEST ")
                MessageBox.Show("Download pgp. T E S T  !!!!", "Process Status");
            else
            {

                System.IO.DirectoryInfo downloadedMessageInfo = new DirectoryInfo(ProcessVars.gODMPs);

                foreach (FileInfo file in downloadedMessageInfo.GetFiles())
                {
                    file.Delete();
                }
                var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

                N_loadFromFTP downloadDta = new N_loadFromFTP();
                string result = downloadDta.downloadData(dateProcess);


                MainProcess processParse = new MainProcess();
                processParse.MainProcessParse("NG");

                //=====
                int totfilesPrecessed = 0;
                DBUtility dbU;

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                string strsql = "delete from HOR_parse_HLGS where CONVERT(DATE,ImportDate)= '" + DateTime.Now.ToString("yyyy-MM-dd") + "'";
                dbU.ExecuteNonQuery(strsql);

                DirectoryInfo originaZips = new DirectoryInfo(ProcessVars.InputDirectory + @"\From_FTP");
                FileInfo[] filesZ = originaZips.GetFiles("*.zip");
                filesZ.Count();

                string extractPath = ProcessVars.InputDirectory + "From_FTP";


                NParse_pdfs parse_pdfs = new NParse_pdfs();
                string ResultsPdf = parse_pdfs.zipFilesinDirService("", extractPath);
                //==

                DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am");

                Export_XLSX export = new Export_XLSX();
                export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "01", GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));

                DataTable resultsTicket01aD = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_epb");


                export.CreateExcelFile(resultsTicket01aD, ProcessVars.InputDirectory + @"From_FTP\", "01_EPBs", GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));


                DataTable resultsTicketTest = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_Test01");
                export.CreateExcelFile(resultsTicketTest, ProcessVars.InputDirectory + @"From_FTP\", "01_Test", GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));


                string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                var t = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * 15);
                    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                });
                t.Wait();

                string time2 = t.Result;


                //appSets appsets = new appSets();
                //appsets.setVars();

                BackCASS processRedturns = new BackCASS();
                Results.Text = processRedturns.ProcessFiles("");
                N_loadFromFTP uploadZip = new N_loadFromFTP();

                ZipFiles zipresult = new ZipFiles();
                int totTxt = 0;
                int TotCSV = 0;
                string zipName = zipresult.ManuallyCreateZipFile("CONBILL", "EPB", out totTxt, out TotCSV);
                string justFilename = zipName;//.Substring(0, zipName.LastIndexOf("_"));
                //int filesIN = Convert.ToInt32(zipName.Substring(zipName.LastIndexOf("_") + 1, zipName.Length - zipName.LastIndexOf("_") - 1));
                FileInfo fiConn = new FileInfo(justFilename);

                // copy zip to network
                string NDirectory = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\Con_GRP_Bills\" + DateTime.Now.ToString("yyyy-MM-dd");
                string Network_pName = NDirectory + "\\" + fiConn.Name;
                if (!Directory.Exists(NDirectory))
                    Directory.CreateDirectory(NDirectory);

                if (File.Exists(Network_pName))
                    File.Delete(Network_pName);
                File.Copy(justFilename, Network_pName);

                string resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/Bills/", totTxt, TotCSV);


                LogWriter logEndProcess = new LogWriter();
                logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);


                zipName = zipresult.ManuallyCreateZipFile("GRPBILL", "EP0GH", out totTxt, out TotCSV);
                FileInfo fileGrp = new FileInfo(zipName);
                justFilename = fileGrp.Name;

                Network_pName = NDirectory + "\\" + justFilename;

                if (File.Exists(Network_pName))
                    File.Delete(Network_pName);
                File.Copy(ProcessVars.InputDirectory + @"FromCASS\" + justFilename, Network_pName);


                //filesIN = Convert.ToInt32(zipName.Substring(zipName.LastIndexOf("_") + 1, zipName.Length - zipName.LastIndexOf("_") - 1));

                //FileInfo fiGrp = new FileInfo(justFilename);
                resultUpload = uploadZip.uploadftp(justFilename, zipName, totTxt + TotCSV, "/Bills/", totTxt, TotCSV);
                logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);

                createEmail createemail = new createEmail();
                createemail.produceSummary_Uploaded();
                createemail.produceSummary_Errors_Cycle_01();
                Results.Text = "Process Ticket 01 ready " + System.Environment.NewLine + time1 + System.Environment.NewLine + time2;
                MessageBox.Show("Download pgp. Process DONE!!!!", "Process Status");
            }
        }


        private void button5_Click(object sender, EventArgs e)
        {

            appSets appsets = new appSets();
            appsets.setVars();

            NParse_pdfs parse_pdfs = new NParse_pdfs();
            string ResultsPdf = parse_pdfs.zipFilesinDir_Cr2(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var t = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 4);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t.Wait();

            string time2 = t.Result;

            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");


            BackCASS processRedturns = new BackCASS();
            string ResultsBack_CASS = processRedturns.ProcessFiles("CareRadius_2");

            Results.Text = "Done pdf's only";

        }

        private void button6_Click(object sender, EventArgs e)
        {

            System.IO.DirectoryInfo downloadedMessageInfo = new DirectoryInfo(ProcessVars.gODMPs);

            foreach (FileInfo file in downloadedMessageInfo.GetFiles())
            {
                file.Delete();
            }
        }

        private void button7_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Closing Ticket 02 ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            string erros = "";

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");


            Export_XLSX export = new Export_XLSX();
            export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02", GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));

            DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim");
            if (resultstoInterim.Rows.Count > 0)
            {
                string colnames = "";
                for (int index = 0; index < resultstoInterim.Columns.Count; index++)
                {
                    string colname = resultstoInterim.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }
                string recnumError = "";
                string insertCommand1 = "Insert into CIE_Interim_JobReceipt_Manual (" + colnames.Substring(1, colnames.Length - 1) + ") VALUES ('";
                foreach (DataRow row in resultstoInterim.Rows)
                {
                    DateTime cycleDate = DateTime.Parse(row[0].ToString());
                    string cDate = cycleDate.Year + "-" + cycleDate.Month.ToString("00") + "-" + cycleDate.Day.ToString("00");

                    var resultUpd = dbU.ExecuteScalar("select filename from CIE_Interim_JobReceipt_Manual where filename = '" + row[1].ToString() + "' and Cycledate = '" + cDate + "'");
                    if (resultUpd == null)
                    {
                        string insertCommand2 = "";
                        for (int index = 0; index < resultstoInterim.Columns.Count; index++)
                        {
                            insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''").Trim() + "','";
                        }
                        try
                        {
                            recnumError = row[0].ToString();
                            var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                        }
                        catch (Exception ex)
                        {

                            erros = erros + ex.Message + "\n\n";
                        }
                    }
                    else
                    {
                        var here = "";
                    }
                }
                Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=receive&Date=" + GlobalVar.DateofProcess.ToString("yyyyMMdd"));
            }
            Ticket createTicket = new Ticket();
            createTicket.createTicket(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");
            Results.Text = "Ticket 02 Closed" + erros;
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
        }

        private void button8_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Downloading Files for Ticket 02 ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            // check unzip HLGS get extra character....
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

            string result = downloadDta.downloadData(GlobalVar.DateofFilesToProcess);

            MainProcess processParse = new MainProcess();
            processParse.MainProcessParse("2");

            //=====
            int totfilesPrecessed = 0;
            DBUtility dbU;

            int filesinDir = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            //string strsql = "delete from HOR_parse_HLGS where CONVERT(DATE,ImportDate)= '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'";
            //dbU.ExecuteNonQuery(strsql);

            DirectoryInfo originaZips = new DirectoryInfo(ProcessVars.InputDirectory + @"\From_FTP");
            FileInfo[] filesZ = originaZips.GetFiles("*.zip");
            filesZ.Count();

            string extractPath = ProcessVars.InputDirectory + "From_FTP";


            NParse_pdfs parse_pdfs = new NParse_pdfs();
            string ResultsPdf = parse_pdfs.zipFilesinDirService("", extractPath);
            Results.Text = "Process for Ticket 02 ready";
            Cursor.Current = Cursors.Default;
            ProcessTicket02();
            objPleaseWait.Close();
        }

        private void button9_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");



            Export_XLSX export = new Export_XLSX();
            export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02", GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));
            Results.Text = "Ticket 02 ready";
        }

        private void button10_Click(object sender, EventArgs e)
        {
            Cycle01 cycle01 = new Cycle01();
            Results.Text = cycle01.Process_AdditionalLCDS();
        }

        private void button11_Click(object sender, EventArgs e)
        {
            Inq1 newprocess = new Inq1();
            newprocess.Show();
        }

        private void button12_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            Nparse_XML parse_xmls = new Nparse_XML();
            string ResultsPdf = parse_xmls.loadXML_Renewal(GlobalVar.DateofFilesToProcess, ProcessVars.InputDirectory + @"\RRenewalM");

            Results.Text = "Done pdf's only";
        }

        private void button13_Click(object sender, EventArgs e)
        {
            NParse_AbilTO abilto = new NParse_AbilTO();
            abilto.printCSV();

        }

        private void button14_Click(object sender, EventArgs e)
        {
            //Cycle01 cycle01 = new Cycle01();
            //cycle01.create_Tickets01();
            createEmail createemail = new createEmail();
            createemail.produceSummary_Uploaded();
            //createemail.produceSummary_Errors_Cycle_01();
        }

        private void button15_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            BackCASS processRedturns = new BackCASS();
            string Results = processRedturns.ProcessFiles("CR2");
        }

        private void button16_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            NParse_pdfs parse_pdfs = new NParse_pdfs();
            string ResultsPdf = parse_pdfs.zipFilesinDir_MBA(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var t = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 4);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t.Wait();

            string time2 = t.Result;

            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");


            BackCASS processRedturns = new BackCASS();
            string ResultsBack_CASS = processRedturns.ProcessFiles("MBA_SMN");

            Results.Text = "MBA SMN Done";
        }

        private void button17_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            Parse_Inv_pdf parse_inv_pdf = new Parse_Inv_pdf();
            string ResultsPdf = parse_inv_pdf.zipFilesinDir_INV(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            //string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //var t = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 4);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //t.Wait();

            //string time2 = t.Result;

            //System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");


            //BackCASS processRedturns = new BackCASS();
            //string ResultsBack_CASS = processRedturns.ProcessFiles("MBA_SMN");

            Results.Text = "INV  Done";
        }

        private void button18_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            NParse_pdfs parse_pdfs = new NParse_pdfs();
            string ResultsPdf = parse_pdfs.zipFilesinDir_SBC(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var t = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 4);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t.Wait();

            string time2 = t.Result;

            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");


            BackCASS processRedturns = new BackCASS();
            string ResultsBack_CASS = processRedturns.ProcessFiles("SBC");

            Results.Text = "SBC Done";
        }

        private void button21_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();
            string starttime = DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
            BackCASS processRedturns = new BackCASS();
            Results.Text = processRedturns.ProcessFiles("");
            Results.Text = starttime + "    Done at" + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");

            //createEmail createemail = new createEmail();
            //createemail.produceSummary_Uploaded();
        }

        private void button19_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing ID Cards ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            appSets appsets = new appSets();
            appsets.setVars();
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            string result2 = downloadDta.downloadFiles_ID_Cards(GlobalVar.DateofProcess);

            //unzip any CON2 or GRP2 in from_FTP dirdctory  (manually a file may have dropped here)

            downloadDta.unzip_ID_Cards();

            Parse_IDCards processFiles = new Parse_IDCards();
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

            string DirLocal = ProcessVars.InputDirectory + @"ID_Cards";
            string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), DirLocal);


            var t0 = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t0.Wait();
            createEmail createemail = new createEmail();

            createemail.produceSummary_ID_NON_Maintenence(DirLocal);

            Results.Text = "ID Cards done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
            objPleaseWait.Close();
        }

        private void button20_Click(object sender, EventArgs e)
        {
            NParse_1099 parse1099 = new NParse_1099();
            string result = parse1099.ProcessFiles(GlobalVar.DateofProcess.ToShortDateString());
            Results.Text = "1099 done";
        }

        private void button22_Click(object sender, EventArgs e)
        {
            NParse_Fraud nparseFraud = new NParse_Fraud();
            nparseFraud.create_csv_Fraud(GlobalVar.DateofProcess.ToShortDateString());
            Results.Text = "Fraud done";
        }

        private void button23_Click(object sender, EventArgs e)
        {
        }
        public void ProcessTicket02()
        {
            //Cursor.Current = Cursors.WaitCursor;
            //Results.Text = "Processing Files for Ticket 02 ...";
            //PleaseWait objPleaseWait = new PleaseWait();
            //objPleaseWait.Show();


            appSets appsets = new appSets();
            appsets.setVars();

            NParse_pdfs parse_pdfs = new NParse_pdfs();
            string ResultsPdf = parse_pdfs.zipFilesinDir_Cr2(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

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

            label4.Text = label4.Text + Environment.NewLine + "CR2";


            Parse_Inv_pdf parse_inv_pdf = new Parse_Inv_pdf();
            string ResultsPdf2 = parse_inv_pdf.zipFilesinDir_INV(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            label4.Text = label4.Text + Environment.NewLine + "INV";

            NParse_pdfs parse_pdfsS = new NParse_pdfs();
            string ResultsPdfS = parse_pdfsS.zipFilesinDir_SBC(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            string time1S = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var tS = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            tS.Wait();


            BackCASS processRedturnsS = new BackCASS();
            string ResultsBack_CASS_S = processRedturns.ProcessFiles("SBC");

            label4.Text = label4.Text + Environment.NewLine + "SBC";

            NParse_pdfs parse_pdfsM = new NParse_pdfs();
            string ResultsPdfM = parse_pdfsM.zipFilesinDir_MBA(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            string time1M = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            var tM = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            tM.Wait();


            BackCASS processRedturnsM = new BackCASS();
            string ResultsBack_CASSM = processRedturnsM.ProcessFiles("MBA_SMN");

            label4.Text = label4.Text + Environment.NewLine + "MBA, SMN PNO" + Environment.NewLine + "Done at " + DateTime.Now.ToString("yyyy_MM_dd   HH:mm");
            Cursor.Current = Cursors.Default;
            //objPleaseWait.Close();
        }

        private void button24_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Downloading Files for Ticket 02 ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            // check unzip HLGS get extra character....
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

            string result = downloadDta.downloadDataCR(GlobalVar.DateofFilesToProcess);

        }

        private void button25_Click(object sender, EventArgs e)
        {
            Cycle01 cycle01 = new Cycle01(); //DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss")
            string input = DateTime.Now.ToString("yyyy-MM-dd"); //"2017-09-04";//Microsoft.VisualBasic.Interaction.InputBox("Enter Date for Ticket", "Enter like 2017-09-01", "", -1, -1);

          
            cycle01.create_Tickets01(input);

        }

        private void button26_Click(object sender, EventArgs e)
        {
            cycle01();
        }

        private void button27_Click(object sender, EventArgs e)
        {

        }

        private void button28_Click(object sender, EventArgs e)
        {
            string dirDecripted = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2016-04-30\Decrypted\";
            string dirReprocess = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2016-04-30\reprocess\";
            appSets appsets = new appSets();
            appsets.setVars();
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable FilesReprocess = dbU.ExecuteDataTable("select * from HOR_parse_files_to_CASS where CONVERT(date,ImportDate) = '2016-04-30' and CASSReceiveDate is null and Processed is null");
            foreach (DataRow row in FilesReprocess.Rows)
            {
                string fname = row["Filename"].ToString();
                File.Copy(dirDecripted + fname, dirReprocess + fname);
            }

        }

        private void button29_Click(object sender, EventArgs e)
        {
            IMBProcess_Back updateFile = new IMBProcess_Back();
            string results1 = updateFile.update_IMB_back(DateTime.Now.ToString("yyyy-MM-dd"));
            string results = updateFile.Update_IdCards(DateTime.Now.ToString("yyyy-MM-dd"));
            Results.Text = results1 + ' ' + results;
        }

        private void button30_Click(object sender, EventArgs e)
        {
            createEmail createemail = new createEmail();
            createemail.produceSummary_Uploaded();
        }

        private void button41_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results2.Text = "Downloading Files MRDF ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            appSets appsets = new appSets();
            appsets.setVars();
            N_loadFromFTP downloadDta = new N_loadFromFTP();

            string resultD = downloadDta.downloadDataMRDF(GlobalVar.DateofProcess);

            //string location = ProcessVars.InputDirectory + @"MRDF\";
            string locationLocal = ProcessVars.InputDirectory + @"MRDF\";
            HNJH_WK_Medicaid procesWKits = new HNJH_WK_Medicaid();

            DirectoryInfo txts = new DirectoryInfo(locationLocal);

            FileInfo[] files = txts.GetFiles("*.out");
            string errorsHre = "";
            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    Process_MRDF_IMB processMRDF = new Process_MRDF_IMB();
                    errors = processMRDF.Process_MRDF(file.FullName, locationLocal);
                    if (errors == "")
                    {

                        string result = processMRDF.Upd_MRDF_to_ID_Cards(file.FullName);
                        string nfilename = file.Directory + "\\__" + file.Name;

                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(file.FullName, nfilename);
                    }
                    else
                    {
                        errorsHre = errorsHre + " " + errors;
                    }
                }
            }


            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results2.BringToFront();
            Results2.Text = Results2.Text +  errorsHre + Environment.NewLine + "Files MRDF DONE ...";
        }

        private void button31_Click(object sender, EventArgs e)
        {
            Nparse_pdf_addrs_to_csv import = new Nparse_pdf_addrs_to_csv();
            import.extract_info_from_pdf();
        }

        private void button43_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results2.Text = Results2.Text.ToString() + Environment.NewLine +  "Downloading Files IMB ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            appSets appsets = new appSets();
            appsets.setVars();

            string locationLocal = @"\\FREENAS\mailProcessingData\IMB\IMB-ID-106845";

            DirectoryInfo txts = new DirectoryInfo(locationLocal);

            FileInfo[] files = txts.GetFiles("*.CSV");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    Process_MRDF_IMB processMRDF = new Process_MRDF_IMB();
                    errors = processMRDF.Process_IMB(file.FullName, locationLocal);
                    if (errors == "")
                    {
                        IMBProcess_Back updateFile = new IMBProcess_Back();
                        string results1 = updateFile.update_IMB_back(file.CreationTime.ToString("yyyy-MM-dd"));   //DateTime.Now.ToString("yyyy-MM-dd")
                        string results = updateFile.Update_IdCards(file.CreationTime.ToString("yyyy-MM-dd"));   //DateTime.Now.ToString("yyyy-MM-dd")
                        string results2 = updateFile.Update_DirectMail(file.CreationTime.ToString("yyyy-MM-dd"));   //DateTime.Now.ToString("yyyy-MM-dd")
                        Results.Text = "IMB " + results1 + ' ' + results + Environment.NewLine +
                            "Direct Mail " + results1 + ' ' + results2;

                        string nfilename = file.Directory + "\\__" + file.Name;
                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(file.FullName, nfilename);
                    }
                    else
                    {
                        Results.Text = Results.Text.ToString() + " errors:  " + errors;
                    }
                }
            }

            Results2.Text = Results2.Text.ToString() + Environment.NewLine + "Files IMB Done...";
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results2.BringToFront();
        }

        private void button32_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing UPPR ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            appSets appsets = new appSets();
            appsets.setVars();

            string locationLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Upprs\2016-11-03\";

            DirectoryInfo txts = new DirectoryInfo(locationLocal);

            FileInfo[] files = txts.GetFiles("*.txt");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    Nparse_UPPR processUPPR = new Nparse_UPPR();
                    errors = processUPPR.evaluate_TXT(file.FullName);
                    if (errors == "")
                    {


                        string nfilename = file.Directory + "\\__" + file.Name;
                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(file.FullName, nfilename);
                    }
                    else
                    {
                        Results.Text = Results.Text.ToString() + " errors:  " + errors;
                    }
                }
            }
            Results.Text = Results.Text.ToString() + Environment.NewLine + "Files UPPR Done...";
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();


        }

        private void button33_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            string locationLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Upprs\2016-11-03\";
            string strsql = "SELECT  [Recnum]      ,[FileName]      ,[ImportDate]      ,[Sysout]      ,[Sheet_count]      ,[Jobname]" +
                            ",[PrintDate]      ,[ArchiveDate]      ,[C_Recnum]      ,[Seq]      ,[mailStop]      ,[JobID]      ,[DE_Flag]" +
                            ",[Field2]      ,[Field3]      ,[Field4]      ,[Field5]      ,[Field6]      ,[Addr1]      ,[Addr2]      ,[Addr3]" +
                            ",[Addr4]      ,[Addr5]      ,[Addr6]           ,[MED_Flag]      ,[MemberID]      ,[ProviderID], paymentNbr" +
                            ",c.AltRecNumCount,'' as MemberID,'' as ProviderID,'' as Alt_addr1,'' as Alt_addr2,'' as Alt_addr3,'' as Alt_addr4,'' as Alt_addr5,'' as Alt_addr6 " +
                            ", c.code1, c.code2, c.code3, c.code4, c.code5, c.code6, c.code7 " +
                            " FROM [BCBS_Horizon].[dbo].[HOR_parse_UCDS]" +
                            " left join HOR_parse_UPPR_FinalPieceContent C on HOR_parse_UCDS.Recnum = C.TransactionRecNum " +
                            " where Recnum in (select Recnum from HOR_parse_UPPR_recnumsss) order by recnum";
            DataTable resultsUPPRs = dbU.ExecuteDataTable(strsql);
            createCSV createFilecsv = new createCSV();
            //string pName1 = locationLocal + "New_Recnums_test" + DateTime.Now.ToString("yyyy_MM_dd") + ".csv";
            //createFilecsv.printCSV_fullProcess(pName1, resultsUPPRs, "", "N");


            DataTable working_resultsUPPRs = resultsUPPRs.Clone();
            int i = 0;

            foreach (DataRow row in resultsUPPRs.Rows)
            {


                if (row[37].ToString().Length > 1)
                {
                    updaterow( resultsUPPRs, i, working_resultsUPPRs, row[37].ToString());
                }
                else
                {
                    working_resultsUPPRs.ImportRow(resultsUPPRs.Rows[i]);
                }
                if (row[38].ToString().Length > 1)
                {
                    updaterow(resultsUPPRs, i, working_resultsUPPRs, row[38].ToString());
                }
                if (row[39].ToString().Length > 1)
                {
                    updaterow(resultsUPPRs, i, working_resultsUPPRs, row[39].ToString());
                }
                if (row[40].ToString().Length > 1)
                {
                    updaterow(resultsUPPRs, i, working_resultsUPPRs, row[40].ToString());
                }
                if (row[41].ToString().Length > 1)
                {
                    updaterow(resultsUPPRs, i, working_resultsUPPRs, row[41].ToString());
                }
                if (row[42].ToString().Length > 1)
                {
                    updaterow(resultsUPPRs, i, working_resultsUPPRs, row[42].ToString());
                }
                if (row[43].ToString().Length > 1)
                {
                    updaterow(resultsUPPRs, i, working_resultsUPPRs, row[43].ToString());
                }

                i++;
            }

            working_resultsUPPRs.Columns.Remove("Code2");
            working_resultsUPPRs.Columns.Remove("Code3");
            working_resultsUPPRs.Columns.Remove("Code4");
            working_resultsUPPRs.Columns.Remove("Code5");
            working_resultsUPPRs.Columns.Remove("Code6");

            string pName = locationLocal + "UUUPS_data_Revised_" + DateTime.Now.ToString("yyyy_MM_dd") + ".csv";
            createFilecsv.printCSV_fullProcess(pName, working_resultsUPPRs, "", "N");
        }

        public void updaterow( DataTable resultsUPPRs, int i, DataTable working_resultsUPPRs, string reccnum)
        {
            string strsql2 = "select [MemberID]      ,[ProviderID], [Addr1]      ,[Addr2]      ,[Addr3],[Addr4]      ,[Addr5]      ,[Addr6], recnum from HOR_parse_UCDS where recnum = " + reccnum;
            DataTable newrecord1 = dbU.ExecuteDataTable(strsql2);
            if (newrecord1.Rows.Count > 0)
            {
                foreach (DataRow row2 in newrecord1.Rows)
                {
                    resultsUPPRs.Rows[i][29] = row2[0].ToString();
                    resultsUPPRs.Rows[i][30] = row2[1].ToString();
                    resultsUPPRs.Rows[i][31] = row2[2].ToString();
                    resultsUPPRs.Rows[i][32] = row2[3].ToString();
                    resultsUPPRs.Rows[i][33] = row2[4].ToString();
                    resultsUPPRs.Rows[i][34] = row2[5].ToString();
                    resultsUPPRs.Rows[i][35] = row2[6].ToString();
                    resultsUPPRs.Rows[i][36] = row2[7].ToString();
                    resultsUPPRs.Rows[i][37] = row2[8].ToString();

                    working_resultsUPPRs.ImportRow(resultsUPPRs.Rows[i]);
                }
            }
            else
            {
                resultsUPPRs.Rows[i][29] = "xxxxxxx";
                resultsUPPRs.Rows[i][30] = "xxxxxxx";
                resultsUPPRs.Rows[i][31] = "xxxxxxx";
                resultsUPPRs.Rows[i][32] = "xxxxxxx";
                resultsUPPRs.Rows[i][33] = "xxxxxxx";
                resultsUPPRs.Rows[i][34] = "xxxxxxx";
                resultsUPPRs.Rows[i][35] = "xxxxxxx";
                resultsUPPRs.Rows[i][36] = "xxxxxxx";
                resultsUPPRs.Rows[i][37] = reccnum;
                working_resultsUPPRs.ImportRow(resultsUPPRs.Rows[i]);
            }
        }

        private void button34_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing Files Claim  ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            appSets appsets = new appSets();
            appsets.setVars();

            string locationLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\test_upps\";

            DirectoryInfo txts = new DirectoryInfo(locationLocal);

            FileInfo[] files = txts.GetFiles("*.txt");
            string[] arrayEOBS = new string[] { "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X" };

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {

                    string error = "";
                    string Type = file.Name.ToUpper().ToString().Substring(4, 1);
                    if (file.Name.ToUpper().ToString().Substring(0, 5) == "UCDSI")
                        Type = file.Name.ToUpper().ToString().Substring(4, 1);
                    if (arrayEOBS.Any(Type.Contains))
                    {
                        NParse_UCDS processUUPS = new NParse_UCDS();
                        errors = processUUPS.evaluate_EOB_uups(file.FullName, "", "");
                        if (errors == "")
                        {
                            string nfilename = file.Directory + "\\__" + file.Name;
                            if (File.Exists(nfilename))
                                File.Delete(nfilename);
                            File.Move(file.FullName, nfilename);
                        }
                    }
                }
            }

            Results.Text = Results.Text.ToString() + Environment.NewLine + "Files Claim code Done...";
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();

        }

        private void button36_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing Reports Claim  ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            appSets appsets = new appSets();
            appsets.setVars();

            //string locationLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\test_upps\";
            string locationLocal = @"\\CIERANT-TAPER\Clients\Horizon BCBS\REPORTS\EOB_Investigate_11-2016";
            string strsql2 = "SELECT  D.[Recnum], D.[FileName],D.[ImportDate],D.[Sysout],D.[Sheet_count],D.[Jobname],D.[PrintDate],D.[ArchiveDate],D.[C_Recnum],D.[Seq], " +
                            "D.[mailStop],D.[JobID],D.[DE_Flag],D.[Field2],D.[Field3],D.[Field4],D.[Field5],D.[Field6],D.[Addr1],D.[Addr2],D.[Addr3],D.[Addr4],D.[Addr5],D.[Addr6],[MED_Flag],[MemberID]      ,[ProviderID], paymentNbr " +
	                        ",c.NA_Number FROM HOR_parse_UCDS D left join HOR_parse_UPPR_Claim_numbers C on D.Sheet_count = C.Sheet_count " +
                            "where D.Recnum in (select Recnum from HOR_parse_UPPR_recnumsss_blanks) order by D.recnum";

            string strsql = "SELECT  D.[Recnum], D.[FileName],D.[ImportDate],D.[Sysout],D.[Sheet_count],D.[Jobname],D.[PrintDate],D.[ArchiveDate],D.[C_Recnum],D.[Seq], " +
                           "D.[mailStop],D.[JobID],D.[DE_Flag],D.[Field2],D.[Field3],D.[Field4],D.[Field5],D.[Field6],D.[Addr1],D.[Addr2],D.[Addr3],D.[Addr4],D.[Addr5],D.[Addr6],[MED_Flag],[MemberID]      ,[ProviderID], paymentNbr " +

                           " FROM HOR_parse_UCDS D where D.Recnum in (select [DISTINCT RECORDS] from HOR_parse_UPPR_recnumsss_nov_14) order by D.recnum";


            DataTable resultsUPPRs = dbU.ExecuteDataTable(strsql);
            createCSV createFilecsv = new createCSV();
            string pName1 = locationLocal + "\\Results_" + DateTime.Now.ToString("yyyy_MM_dd") + ".csv";
            createFilecsv.printCSV_fullProcess(pName1, resultsUPPRs, "", "N");

            Results.Text = Results.Text.ToString() + Environment.NewLine + "Reports Claim code Done...";
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
        }

        private void button37_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing Files TEST F101  ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            appSets appsets = new appSets();
            appsets.setVars();

            //string locationLocal = @"\\CIERANT-TAPER\Clients\Horizon BCBS\TEST FILES\SECURE DATA\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\test_SIT_UAT";
             string locationLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2017-08-10\test";

            DirectoryInfo txts = new DirectoryInfo(locationLocal);

            FileInfo[] files = txts.GetFiles("*.txt");
            //string[] arrayEOBS = new string[] { "E001","E002","Q001","101","M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X" };

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {

                    string error = "";
                    string Type = file.Name.ToUpper().ToString().Substring(4, 1);
                    if (file.Name.ToUpper().ToString().Substring(2, 5) == "TF101" )
                    {
                        Type = file.Name.ToUpper().ToString().Substring(4, 3);
                        
                            NParse_UCDS processUUPS = new NParse_UCDS();
                            errors = processUUPS.evaluate_EOB_101_TXT(file.FullName, "", "");
                            if (errors == "")
                            {
                                string nfilename = file.Directory + "\\__" + file.Name;
                                if (File.Exists(nfilename))
                                    File.Delete(nfilename);
                                File.Move(file.FullName, nfilename);
                            }
                    }
                    else if (file.Name.ToUpper().ToString().Substring(2, 4) == "TQ00" || file.Name.ToUpper().ToString().Substring(2, 4) == "TE00")
                    {
                      
                            NParse_UCDS processUUPS = new NParse_UCDS();
                            errors = processUUPS.evaluate_EOB_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),""); 
                                //processUUPS.evaluate_EOB_101_TXT(file.FullName, "", "");
                            if (errors == "")
                            {
                                string tablename = "UCDS";
                                string fResultName = file.Directory + "\\" + file.Name.Replace(".txt", ".csv");
                                SqlParameter[] sqlParams;
                                sqlParams = null;
                                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", file.Name), new SqlParameter("@table", "HOR_parse_" + tablename) };

                                DataTable datato_SCI = dbU.ExecuteDataTable("HOR_rpt_PARSE_F101_SCI", sqlParams);
                                createCSV createcsv = new createCSV();
                                if (datato_SCI.Rows.Count > 0)
                                {
                                    createcsv.printCSV_fullProcess(fResultName, datato_SCI, "", "");
                                }
                                string nfilename = file.Directory + "\\__" + file.Name;
                                if (File.Exists(nfilename))
                                    File.Delete(nfilename);
                                File.Move(file.FullName, nfilename);
                            }
                    }

                    else if (file.Name.ToUpper().ToString().Substring(2, 5) == "TAR06")
                    {
                          NParse_ALGS Algs = new NParse_ALGS();
                          string errorA = Algs.evaluate_TXTAR06(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),"");
                            
                            if (errorA == "")
                            {
                                string nfilename = file.Directory + "\\__" + file.Name;
                                if (File.Exists(nfilename))
                                    File.Delete(nfilename);
                                File.Move(file.FullName, nfilename);
                            }
                    }
                }
            }

            locationLocal = @"\\CIERANT-TAPER\Clients\Horizon BCBS\TEST FILES\SECURE DATA\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd"); // +@"\SITFIM_and_UATFIM_CSVand PDFs.d";
            //@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\test_KarlsFiles\";

            DirectoryInfo csvs = new DirectoryInfo(locationLocal);

            FileInfo[] CsVfiles = csvs.GetFiles("*FIM*.csv");
            
            errors = "";
            foreach (FileInfo file in CsVfiles)
            {
                if (file.Name.IndexOf("__") == -1)
                {

                    
                        NParse_UCDS processUUPS = new NParse_UCDS();
                        errors = processUUPS.assigRecnums_F101(file.FullName);
                        if (errors == "")
                        {
                            string nfilename = file.Directory + "\\__" + file.Name;
                            if (File.Exists(nfilename))
                                File.Delete(nfilename);
                            File.Move(file.FullName, nfilename);
                        }
                   
                }
            }



            Results.Text = Results.Text.ToString() + Environment.NewLine + "Files F101 Done...";
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
        }

        private void button38_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results2.Text = "Processing Files ftp Activity ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            appSets appsets = new appSets();
            appsets.setVars();
         
            string locationLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2017_ftpActivity";
            HNJH_WK_Medicaid procesWKits = new HNJH_WK_Medicaid();

            DirectoryInfo txts = new DirectoryInfo(locationLocal);

            FileInfo[] files = txts.GetFiles("*.txt");
            string errorsHre = "";
            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    ftpActivity processFtpActivity = new ftpActivity();
                    errors = processFtpActivity.evaluate_TXT(file.FullName);
                    if (errors == "")
                    {

                       
                        string nfilename = file.Directory + "\\__" + file.Name;

                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(file.FullName, nfilename);
                    }
                    else
                    {
                        errorsHre = errorsHre + " " + errors;
                    }
                }
            }


            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results2.BringToFront();
            Results2.Text = Results2.Text + errorsHre + Environment.NewLine + "Process Files ftp Activity  DONE ...";
        }

    }
}
