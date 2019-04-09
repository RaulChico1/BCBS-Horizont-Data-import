using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.IO;

using System.IO.Compression;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Globalization;
using System.Security;
using System.Security.Principal;
using System.Data.SqlClient;
using Tamir.SharpSsh;
using System.Collections;
using System.Web.Services;
using System.Configuration;
using System.Threading;
using Horizon_EOBS_Parse;

namespace WindowsForm
{
    public partial class Form2 : Form
    {
        
        DBUtility dbU;
        private string JobIDs = "";
        public string JobID = "";
        
        public Form2()
        {
            InitializeComponent();
            
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
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket02 = dbU.ExecuteDataTable("HOR_scr_DailyUpload_Cycle_02");
            dataGridView1.DataSource = resultsTicket02;
            DataGridViewColumn column = dataGridView1.Columns[0];
            column.Width = 160;
            List<string> listinDrive = new List<string>();
            DirectoryInfo originaFiles = new DirectoryInfo(ProcessVars.InputDirectory + @"\From_FTP");
            FileInfo[] filesZ = originaFiles.GetFiles("*.pdf");
            if (filesZ.Count() > 0)
            {
                foreach (FileInfo filename in filesZ)
                {
                    if (filename.Name.IndexOf("__") != 0)
                        listinDrive.Add(filename.Name);
                }
            }
            FileInfo[] filesx = originaFiles.GetFiles("*.xml");
            if (filesx.Count() > 0)
            {
                foreach (FileInfo filename in filesx)
                {
                    if (filename.Name.IndexOf("__") != 0)
                        listinDrive.Add(filename.Name);
                }
            }
            dataGridView2.DataSource = ConvertListToDataTable(listinDrive);
            //N_loadFromFTP downloadDta = new N_loadFromFTP();
            //string result = downloadDta.FileNamesFtp(GlobalVar.DateofFilesToProcess);
            //string[] words = result.Split('~');
            //List<string> listinFtp = new List<string>();
            //foreach (string s in words)
            //{
            //    listinFtp.Add(s);
            //}
            //dataGridView3.DataSource = ConvertListToDataTable(listinFtp);

        }
        static DataTable ConvertListToDataTable(List<string> list)
        {
            // New table.
            DataTable table = new DataTable();

            // Get max columns.
            int columns = 0;
            foreach (var array in list)
            {
                if (array.Length > columns)
                {
                    columns = array.Length;
                }
            }

            // Add columns.
            for (int i = 0; i < columns; i++)
            {
                table.Columns.Add();
            }

            // Add rows.
            foreach (var array in list)
            {
                table.Rows.Add(array);
            }

            return table;
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
            PleaseWait objPleaseWait = new PleaseWait();
            DialogResult dialogResult = MessageBox.Show("Run inmediatelly??", "Ticket 01", MessageBoxButtons.YesNo);
            if (dialogResult == DialogResult.Yes)
            {
                //no wait
            }
            else if (dialogResult == DialogResult.No)
            {
                Results.Text = "Processing Ticket 01...";
                objPleaseWait.Show();
                Application.DoEvents();
                        DateTime nextRun2 = DateTime.Today.AddDays(+1).AddHours(2).AddMinutes(10);
                        TimeSpan diff = nextRun2.Subtract(DateTime.Now);
                        int totalMinutes = (int)diff.TotalMinutes;
                        var t0 = Task.Run(async delegate
                        {
                            await Task.Delay(1000 * 60 * totalMinutes);
                            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        });
                        t0.Wait();
            }
            appSets appsets = new appSets();
            appsets.setVars();
            pd.Text = GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
            fd.Text = GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd");
            label1.Text = ProcessVars.InputDirectory;
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\from_FTP");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Decrypted");


            Cycle01 cycle01 = new Cycle01();
            Results.Text = cycle01.ProcessTicket01();
            objPleaseWait.Close();
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
                export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "01");

                DataTable resultsTicket01aD = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_epb");


                export.CreateExcelFile(resultsTicket01aD, ProcessVars.InputDirectory + @"From_FTP\", "01_EPBs");


                DataTable resultsTicketTest = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_Test01");
                export.CreateExcelFile(resultsTicketTest, ProcessVars.InputDirectory + @"From_FTP\", "01_Test");
                
                
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
                string NDirectory = @"\\freenas\Clients\Horizon BCBS\NoticeLetters\Con_GRP_Bills\" + DateTime.Now.ToString("yyyy-MM-dd");
                string Network_pName = NDirectory + "\\" + fiConn.Name;
                if (!Directory.Exists(NDirectory))
                    Directory.CreateDirectory(NDirectory);

                if (File.Exists(Network_pName))
                    File.Delete(Network_pName);
                File.Copy(justFilename, Network_pName);

                string resultUpload = uploadZip.uploadftp(fiConn.Name, justFilename, totTxt + TotCSV, "/Bills/",totTxt,TotCSV);


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
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");


            Export_XLSX export = new Export_XLSX();
            export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");

            DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim");
            //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_only_HIX_Inv");
            if(resultstoInterim.Rows.Count > 0)
            {
                string colnames = "";
                for (int index = 0; index < resultstoInterim.Columns.Count; index++)
                {
                    string colname = resultstoInterim.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }
                string recnumError = "";
                string insertCommand1 = "Insert into CIE_Interim_JobReceipt_Manual (" + colnames.Substring(1,colnames.Length -1) + ") VALUES ('";
                foreach (DataRow row in resultstoInterim.Rows)
                {
                    DateTime cycleDate = DateTime.Parse( row[0].ToString());
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

            string resultD = downloadDta.downloadData(GlobalVar.DateofFilesToProcess);

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
            ProcessTicket02();
            if (resultD == "")
                Results.Text = resultD + "Process for Ticket 02 ready";
            else
                Results.Text = resultD;

            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
        }

        private void button9_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");

           
            
            Export_XLSX export = new Export_XLSX();
            export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");
            Results.Text = "Ticket 02 ready";
        }

        private void button10_Click(object sender, EventArgs e)
        {
            Cycle01 cycle01 = new Cycle01();
            Results.Text =  cycle01.Process_AdditionalLCDS();
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
            createemail.produceSummary_Errors_Cycle_01();
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
            Results.Text = "Checking FTP ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            N_loadFromFTP downloadDta = new N_loadFromFTP();
            string result = downloadDta.FileNamesFtp(GlobalVar.DateofFilesToProcess);
            string[] words = result.Split('~');
            List<string> listinFtp = new List<string>();
            foreach (string s in words)
            {
                listinFtp.Add(s);
            }
            dataGridView3.DataSource = ConvertListToDataTable(listinFtp);
            objPleaseWait.Close();
        }

        private void button25_Click(object sender, EventArgs e)
        {
            NparseHNJH_Panel processFiles = new NparseHNJH_Panel();
            //string DirLocal = ProcessVars.InputDirectory + @"ID_Cards";
            string DirLocal = @"\\freenas\Clients\Horizon BCBS\15-0475_HOR-NJH_Medicaid Setup\Panel Roster\SOURCE";
            string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), DirLocal);

        }


       




        private void button26_Click(object sender, EventArgs e)
        {
       
            string errors = "";
            
           // string JobIDs = "";
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing NjHID Cards ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            dbU = ProcessVars.oDBUtility();

          
               
                string DirNetwork = ProcessVars.NJHIDCardsDirectory;
                string DirToMoveTo = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                if (!Directory.Exists(DirToMoveTo))
                    Directory.CreateDirectory(DirToMoveTo);


                DirectoryInfo DirNetworkInfo = new DirectoryInfo(DirNetwork);
                FileInfo[] FilesTxtNetwork = DirNetworkInfo.GetFiles("*.Txt");
                string[] fileNames = FilesTxtNetwork.Select(f => f.Name).ToArray();
                if (fileNames.Length > 0)
                {
                    foreach (string File in fileNames)
                    {
                        try
                        {


                            string txtFileName = File;

                            if (txtFileName.IndexOf("HNJHID") >= 0 && txtFileName.IndexOf(".txt") > 0 && !(txtFileName.Contains("Medicare")))
                            {
                                //check if file loaded already

                                string txtfileNameNJHID = "";
                                int fileExtPos = txtFileName.LastIndexOf(".");
                                if (fileExtPos >= 0)
                                    txtfileNameNJHID = txtFileName.Substring(0, fileExtPos);

                                String Sql = "select count(*) from  [HNJH_IDCards] where  SUBSTRING(FileName, 1, 12)='" + txtfileNameNJHID + "'";



                                //  String Sql = "SELECT count(*) FROM HNJH_IDCards WHERE FileName like '" + txtfileNameNJHID + "_ID_" + "%'";
                                var numrec = dbU.ExecuteScalar(Sql);
                                int NumRecs = Convert.ToInt32(numrec);


                                if (NumRecs.Equals(0))//New file
                                {

                                    CopyFile(DirNetwork, txtFileName, DirToMoveTo);


                                }
                                else
                                {

                                    errors = "File Already Exists In Database/delete the file and try again";
                                    Results.Text = errors;
                                    StreamWriter sw = new StreamWriter(DirNetwork + "log.txt", true);
                                    sw.WriteLine("File Already Exists In Database: " + txtFileName);
                                    sw.Close();
                                }

                            }
                            //else
                            //{
                            //    errors = "File not found";
                            //}

                        }


                        catch (Exception ez)
                        {
                            errors = errors + File + "  " + ez.Message + "\n\n";
                        }

                    }

                }
                else
                {
                     errors = "File Not Found";
                     Results.Text = "";
                     Results.Text = "NjhId cards- File Not Found";
                     objPleaseWait.Close();
                     SendMails sendmail = new SendMails();
                     sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com", "noreply@apps.cierant.com", "Njhid File Not Found..." + DateTime.Now.ToString("yyyy-mm-dd"));
                     Application.Exit();
                }
          ////  }

             //////// CONTINUING AFTER TXTFILE PUSHED TO DATED FOLDER
                if (errors == "")
                {

                    string DirLocal = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                    DirectoryInfo DirNetworkDatedInfo = new DirectoryInfo(DirLocal);
                    FileInfo[] FilesTxtDatedNetwork = DirNetworkDatedInfo.GetFiles("HNJH*.Txt");


                    Parse_IDCards HnjhIdcards = new Parse_IDCards();

                    //////dELETE FROM TMP TABLE AND XMPIE TABLE And Temp TO KEEP ONLY THAT DAYS DATA .



                    foreach (FileInfo file in FilesTxtDatedNetwork)
                    {



                        //start

                        string txtFileName = file.Name;

                        if (txtFileName.IndexOf("HNJHID") >= 0 && txtFileName.IndexOf(".txt") > 0 && !(txtFileName.Contains("Medicare")))
                        {
                            //check if file loaded already

                            string txtfileNameNJHID = "";
                            int fileExtPos = txtFileName.LastIndexOf(".");
                            if (fileExtPos >= 0)
                                txtfileNameNJHID = txtFileName.Substring(0, fileExtPos);

                            String Sql = "select count(*) from  [HNJH_IDCards] where  SUBSTRING(FileName, 1, 12)='" + txtfileNameNJHID + "'";



                            //  String Sql = "SELECT count(*) FROM HNJH_IDCards WHERE FileName like '" + txtfileNameNJHID + "_ID_" + "%'";
                            var numrec = dbU.ExecuteScalar(Sql);
                            int NumRecs = Convert.ToInt32(numrec);


                            if (NumRecs.Equals(0))//New file
                            {


                                //end
                                try
                                {
                                    dbU.ExecuteScalar("Delete from HNJH_IDCards_Temp");
                                    dbU.ExecuteScalar("Delete from HNJH_IDCards_Xmpie");
                                    dbU.ExecuteScalar("Delete from HNJH_IDCards_Xmpie_MLTSS");
                                    dbU.ExecuteScalar("Delete from  HNJH_IDCards_OutputCasTemp");

                                    errors = HnjhIdcards.evaluate_HNJHIDCards(file.FullName, DirLocal);
                                    if (errors == "")
                                    {

                                        string[] ProcessDocs = ProcessVars.DocumentIDs.Split(',');
                                        foreach (string _DocumentID in ProcessDocs)
                                        {
                                            ID_Cards(_DocumentID);
                                        }
                                        //Check if each Job is complete.If yes, then do the rest of pdf processing, else give some time.


                                        CheckJobStatus(JobIDs);

                                        var t = Task.Run(async delegate
                                        {
                                            await Task.Delay(1000 * 60 * 1);
                                            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                                        });
                                        t.Wait();




                                        string txtfilename = Path.GetFileName(file.FullName);
                                        string[] fileNametxt = FilesTxtDatedNetwork.Select(f => f.Name).ToArray();
                                        foreach (string File in fileNametxt)
                                            if (File == txtfilename)
                                            {

                                                CreateZipFileForNJFamilyCare(txtfilename);
                                                CreateZipFileForMLTSS(txtfilename);
                                                MoveXmpiePdfsToProcessedFolder();
                                            }

                                    }
                                    else
                                    {
                                        Results.Text = "";
                                        Results.Text = "NJHID Cards not processed-Bcc failed/Data not in correct format " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
                                        SendMails sendmail = new SendMails();
                                        sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "Njhid cards -Bcc Failed/File Not in Correct Format..." + DateTime.Now.ToString("yyyy-mm-dd"));
                                        Application.Exit();
                                    }


                                }
                                catch (Exception ez)
                                {
                                    errors = errors + file + "  " + ez.Message + "\n\n";
                                }

                            }
                        }
                    }

                        if (errors == "")
                            {

                                //--move the HNJHID.TXT FILES TO PROCESSED FOLDER--//


                                string DirToMoveDailyTXTfILES = ProcessVars.NJHIDCardsDirectory + "Processed" + @"\";
                                if (!Directory.Exists(DirToMoveDailyTXTfILES))
                                    Directory.CreateDirectory(DirToMoveDailyTXTfILES);

                                string SourceDirectory = ProcessVars.NJHIDCardsDirectory;
                                DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SourceDirectory);
                                FileInfo[] FilesTxtInSourceD = DirSourceDirectoryInfo.GetFiles("HNJH*.txt");
                                string[] fileNames1 = FilesTxtInSourceD.Select(f => f.Name).ToArray();
                                foreach (string File in fileNames1)
                                {
                                    string Txtfilename = File;
                                    if (Txtfilename.IndexOf("HNJHID") >= 0 && Txtfilename.IndexOf(".txt") > 0 && !(Txtfilename.Contains("Medicare")))
                                    {
                                        String SourceFile = Path.Combine(ProcessVars.NJHIDCardsDirectory, Txtfilename);
                                        String DestinationFile = Path.Combine(DirToMoveDailyTXTfILES, Txtfilename);
                                        System.IO.File.Move(SourceFile, DestinationFile);
                                    }
                                }



                                // MoveFilesFromSciFolderToFTP();







                                Results.Text = "";
                                Results.Text = "NJHID Cards done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
                                objPleaseWait.Close();


                                SendMails sendmail = new SendMails();
                                sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com", "noreply@apps.cierant.com", "Finished processing NjhId cards at  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

                                uploadNjhIdCardsToFtp();





                            }
                            else
                            {
                                Results.Text = "";
                                Results.Text = "NjhId cards is not processed.";
                                objPleaseWait.Close();
                                SendMails sendmail = new SendMails();
                                sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "Njhid cards finished processing with errors..." + DateTime.Now.ToString("yyyy-mm-dd"));
                            }


                        }

                        else
                        {
                            Results.Text = "";
                            Results.Text = "NjhId cards is not processed.Pls put the file in the correct location";
                            objPleaseWait.Close();
                            SendMails sendmail = new SendMails();
                            sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com", "noreply@apps.cierant.com", "Njhid cards finished processing with errors..." + DateTime.Now.ToString("yyyy-mm-dd"));
                        }
                    }
             

        private void uploadNjhIdCardsToFtp()
        {
            String error = "Error uploading";
            dbU = ProcessVars.oDBUtility();
            string SciFolder = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + @"SCI\";
            DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SciFolder);
            string[] filefullPath = Directory.GetFiles(SciFolder, "*.ZIP");
            FileInfo[] fileDetail = DirSourceDirectoryInfo.GetFiles("*.ZIP");
            string[] fileNames = fileDetail.Select(f => f.Name).ToArray();
            int _Port = 22;
            string FileName = "";
            Sftp oSftp = new Sftp(ProcessVars.gHNJHIDCards_FTP_URL, ProcessVars.gHNJHIDCards_FTPUserNameProd, ProcessVars.gHNJHIDCards_FTPPwdProd);
            string HtmlBody = "";
            HtmlBody += "<table  id=\"SciUploadTable\" bgcolor='#ffffcc' style='width:100%;border:1px solid black ;border-collapse: collapse'  <tr><th align='center' valign='middle'>FileName </th><th align='center' valign='middle'>Import_Date </th><th align='center' valign='middle'>Status</th></tr>";
            string status = "";


            if (filefullPath != null && filefullPath.Length > 0)
            {

                try
                {
                    oSftp.Connect(_Port);
                    if (oSftp.Connected)
                    {
                        for (int i = 0; i < filefullPath.Length; i++)
                        {
                            string fullfilepath = filefullPath[i];
                            string filenameonly = fileNames[i];

                            try
                            {

                                oSftp.Put(filefullPath[i], ProcessVars.gHNJHIDCards_FTPLocationProd + filenameonly);
                                status = "Uploaded/ok";
                            }

                            catch (Exception ex)
                            {

                                SendMails sendmail = new SendMails();
                                sendmail.SendMail("Error uploading files to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com,cgaytan@apps.cierant.com", "noreply@apps.cierant.com", error);

                            }

                            HtmlBody += "<tr><td align='center' valign='middle' style='border:1px solid black'>" + filenameonly + "</td><td align='center' valign='middle' style='border:1px solid black'>" + DateTime.Now.ToString() + "</td><td align='center' valign='middle' style='border:1px solid black'> " + status + "</td></tr>";



                        }
                        HtmlBody += "</table>";

                    }

                }
                catch (Exception ex)
                {

                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("Error uploading files to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com", error);

                }
                finally
                {
                    oSftp.Close();

                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("NJHID Cards Posted to CaptainCrunch on " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,tclinton@apps.cierant.com,rchico@apps.cierant.com,jcioban@cierant.com,cgaytan@apps.cierant.com,pgnecco@sciimage.com,edymek@sciimage.com,jnunez@sciimage.com,mscherman@sciimage.com,msundburg@sciimage.com,todonnell@sciimage.com", "noreply@apps.cierant.com", HtmlBody);
                    Results.Text = "";
                    Results.Text = "Posting Successful!";

                }


            }

            else
            {
                string Error = "Could not find zip files to upload";
                SendMails sendmail = new SendMails();
                sendmail.SendMail("CouldNot find njhId zip files to upload  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com", Error);

            }






        }











         private void MoveXmpieDSNPPdfsToProcessedFolder()
        {
            
            string DirToMoveDailyXmpiePdfFiles = ProcessVars.XmpiepdfProcessedPath + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\DSNP\";
            if (!Directory.Exists(DirToMoveDailyXmpiePdfFiles))
                Directory.CreateDirectory(DirToMoveDailyXmpiePdfFiles);
           
            string SourceDirectory = ProcessVars.XmpiepdfPath;
            DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SourceDirectory);
            FileInfo[] FilesPdfInSourceD = DirSourceDirectoryInfo.GetFiles("*.pdf");
            string[] fileNames = FilesPdfInSourceD.Select(f => f.Name).ToArray();
                foreach (string File in fileNames)
                {
                    string Pdffilename = File;
                    String SourceFile = Path.Combine(ProcessVars.XmpiepdfPath, Pdffilename);
                    String DestinationFile = Path.Combine(DirToMoveDailyXmpiePdfFiles, Pdffilename);
                    System.IO.File.Move(SourceFile, DestinationFile);
                }
            

            




            }


         //private void MoveFilesFromSciFolderToFTP()
         //{
         //  
         //}







        private void CreateZipFileForNJFamilyCare(string InputFileName)
        {
            //csvfilename= HNJHID030916_ID_20160331140500_1.csv
            //pdffilename=HNJHID031016_ID_20160330124408_1.pdf
            //string filename="HNJHID031016_PACKAGE_20160331140608.ZIP";

            dbU = ProcessVars.oDBUtility();
           // string scidirectory = @"\\10.0.200.248\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\TEST_INBOUND\2016-03-31\SCI";
            string scidirectory = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + "SCI";
            if (!Directory.Exists(scidirectory))
                Directory.CreateDirectory(scidirectory);


            //string NJHIDCsvFormatNjCard = "_1.csv,_10001.csv,_20001.csv,_30001.csv,_40001.csv,_50001.csv,_60001.csv,_70001.csv,_80001.csv,_90001.csv,_100001.csv,_110001.csv";
           

            string[] NJHIDCsvFormatNjCards =ProcessVars.NJHIDCsvFormatNjCard.Split(',');
            //string[] NJHIDPdfFormatNjCards =ProcessVars.NJHIDPdfFormatNjCard.Split(',');


            string inputFileNameWithouttxt = InputFileName.Replace(".txt", "");//ex:-HNJHID030916


            string fullNamesCsvFilePath = "";
            string fullNamesPDFFilePath = "";
            string zipname = "";
            string zipfullnamepath = "";
            string finalCSVfilename = "";
            string finalPDFfilename = "";
            string tempCSVNAME = "";

            foreach (string NJHIDCsvFormatNjCardt in NJHIDCsvFormatNjCards)
            {
                List<string> files = new List<string>();
                string DirCsvFilePath = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";

                DirectoryInfo DirCsvFilePathInfo = new DirectoryInfo(DirCsvFilePath);

                FileInfo[] FilesNjCardCsv = DirCsvFilePathInfo.GetFiles(inputFileNameWithouttxt + "*" + NJHIDCsvFormatNjCardt);//_1.csv
                
                foreach (FileInfo _filenameCSV1 in FilesNjCardCsv)
                {
                    fullNamesCsvFilePath = _filenameCSV1.FullName;
                }
                string[] fileNameCsv = FilesNjCardCsv.Select(f => f.Name).ToArray();


                foreach (string _filenameCSV in fileNameCsv)
                {
                    int first_pos = _filenameCSV.IndexOf("_");
                    string _filename = _filenameCSV.Substring(0, first_pos);
                    //check if the csv filenames (first few letters) and input txtfilename are same like 'HNJHID030916=HNJHID030916'.checking this  if there are multiple csvs created from running more than one input txt file in the datedfolder.
                    if (_filename == inputFileNameWithouttxt)
                    {
                        int indexpos = _filenameCSV.LastIndexOf("_");
                        if (indexpos >= 0)
                        {
                            tempCSVNAME = _filenameCSV.Substring(0, indexpos);
                            finalCSVfilename = _filenameCSV.Substring(0, indexpos) + ".csv";
                            zipname = finalCSVfilename.Replace("_ID_", "_PACKAGING_").Replace(".csv", ".zip");
                            zipfullnamepath = Path.Combine(scidirectory, zipname);
                            if (File.Exists(zipfullnamepath))
                            {
                                break;
                            }

                        }

                    }

                  
                        string DirXmpiePdfFilePath = @ProcessVars.XmpiepdfPath;

                        DirectoryInfo DirPdfPathInfo = new DirectoryInfo(DirXmpiePdfFilePath);
                        string NJHIDPdfFormatNjCardt = NJHIDCsvFormatNjCardt.Replace("csv", "pdf");//"_1.csv" to "_1.pdf"

                       FileInfo[] FilesNjCardsPdf = DirPdfPathInfo.GetFiles("*" + NJHIDPdfFormatNjCardt);
                       if (FilesNjCardsPdf.Length == 0)
                       {
                           fullNamesPDFFilePath = "";
                       }
                       else
                       {
                           foreach (FileInfo _filenamePDF1 in FilesNjCardsPdf)
                           {
                               fullNamesPDFFilePath = _filenamePDF1.FullName;
                           }
                       }

                        string[] fileNamePdf = FilesNjCardsPdf.Select(f => f.Name).ToArray();

                        foreach (string _filenamepdf in fileNamePdf)
                        {
                            int indexpospdf = _filenamepdf.LastIndexOf("_");
                            if (indexpospdf >= 0)
                            {
                                finalPDFfilename = tempCSVNAME + ".pdf";
                              

                            }
                        }
                        if (fullNamesPDFFilePath != "")
                   
                        { 
                            createzipfile(fullNamesCsvFilePath, fullNamesPDFFilePath, finalCSVfilename, finalPDFfilename, zipname, zipfullnamepath);
                        }


                }

            }
        }


        private void CreateZipFileForMLTSS(string InputFileName)
        {

            string scidirectory = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + "SCI";

            if (!Directory.Exists(scidirectory))
                Directory.CreateDirectory(scidirectory);



            string[] NJHIDCsvFormatMLTSSs = ProcessVars.NJHIDCsvFormatMLTSS.Split(',');
            string[] NJHIDPdfFormatMLTSSs = ProcessVars.NJHIDPdfFormatMLTSS.Split(',');


            string inputFileNameWithouttxt = InputFileName.Replace(".txt", "");

            string fullNamesCsvFilePath = "";
            string fullNamesPDFFilePath = "";
            string zipname = "";
            string zipfullnamepath = "";
            string finalCSVfilename = "";
            string finalPDFfilename = "";
            string tempCSVNAME = "";

            foreach (string NJHIDCsvFormatMLTSSst in NJHIDCsvFormatMLTSSs)
            {
                List<string> files = new List<string>();
                string DirCsvFilePath = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";

                DirectoryInfo DirCsvFilePathInfo = new DirectoryInfo(DirCsvFilePath);
                FileInfo[] FilesMLTSSCsv = DirCsvFilePathInfo.GetFiles(inputFileNameWithouttxt+"*" + NJHIDCsvFormatMLTSSst);
                foreach (FileInfo _filenameCSV1 in FilesMLTSSCsv)
                {
                    fullNamesCsvFilePath = _filenameCSV1.FullName;
                }
                string[] fileNameCsv = FilesMLTSSCsv.Select(f => f.Name).ToArray();


                foreach (string _filenameCSV in fileNameCsv)
                {
                    int first_pos = _filenameCSV.IndexOf("_");
                    string _filename = _filenameCSV.Substring(0, first_pos);
                    //check if the csv filenames (first few letters) and input txtfilename are same like 'HNJHID030916=HNJHID030916'.checking this  if there are multiple csvs created from running more than one input txt file in the datedfolder.
                    if (_filename == inputFileNameWithouttxt)
                    {
                        int indexpos = _filenameCSV.LastIndexOf("_");
                        if (indexpos >= 0)
                        {
                            tempCSVNAME = _filenameCSV.Substring(0, indexpos);
                            finalCSVfilename = _filenameCSV.Substring(0, indexpos) + ".csv";
                            zipname = finalCSVfilename.Replace("_ID_", "_PACKAGING_").Replace(".csv", ".zip");
                            zipfullnamepath = Path.Combine(scidirectory, zipname);
                            if (File.Exists(zipfullnamepath))
                            {
                                break;
                            }

                        }
                    }



                    string DirXmpiePdfFilePath = @ProcessVars.XmpiepdfPath;
                    string NJHIDPdfFormatMLTSSst = NJHIDCsvFormatMLTSSst.Replace("csv", "pdf");
                    DirectoryInfo DirPdfPathInfo = new DirectoryInfo(DirXmpiePdfFilePath);
                    FileInfo[] FilesMLTSSPdf = DirPdfPathInfo.GetFiles("*" + NJHIDPdfFormatMLTSSst);
                    if (FilesMLTSSPdf.Length == 0)
                    {
                        fullNamesPDFFilePath = "";
                    }
                    else
                    {
                        foreach (FileInfo _filenamePDF1 in FilesMLTSSPdf)
                        {
                            fullNamesPDFFilePath = _filenamePDF1.FullName;
                        }
                    }

                    string[] fileNamePdf = FilesMLTSSPdf.Select(f => f.Name).ToArray();

                    foreach (string _filenamepdf in fileNamePdf)
                    {
                        int indexpospdf = _filenamepdf.LastIndexOf("_");
                        if (indexpospdf >= 0)
                        {

                            finalPDFfilename = tempCSVNAME + ".pdf";


                        }
                    }

                    if (fullNamesPDFFilePath != "")

                    { createzipfile(fullNamesCsvFilePath, fullNamesPDFFilePath, finalCSVfilename, finalPDFfilename, zipname, zipfullnamepath); }
                }

            }

        }


        private void createzipfile(string fullNamesCsvFilePath, string fullNamesPDFFilePath, string finalCSVfilename, string finalPDFfilename, string zipname, string zipfullnamepath)
        {


            if (File.Exists(zipfullnamepath))
            {
                File.Delete(zipfullnamepath);
            }

            if (File.Exists(fullNamesPDFFilePath) && File.Exists(fullNamesCsvFilePath))
            {
                using (ZipArchive newFile = ZipFile.Open(zipfullnamepath, ZipArchiveMode.Create))
                {

                    newFile.CreateEntryFromFile(@fullNamesCsvFilePath, finalCSVfilename);
                    newFile.CreateEntryFromFile(@fullNamesPDFFilePath, finalPDFfilename, CompressionLevel.Fastest);
                }
            }

        }




        protected void ID_Cards(string _DocumentID)
        {
            
            
            try
            {
                // Create the job ticket web service object    

                xmpiedirector_JobTicket.JobTicket_SSP jobTicketWS = new xmpiedirector_JobTicket.JobTicket_SSP();

                // Create a new job ticket
                string jobTicketID = jobTicketWS.CreateNewTicketForDocument(ProcessVars.uName, ProcessVars.Password, _DocumentID, "", false);

                // jobTicketWS.AddDestinationByID(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"], "", true);
                jobTicketWS.AddDestinationByID(ProcessVars.uName,ProcessVars.Password,jobTicketID,ProcessVars.DestinationID_IDCARDS,"",true);
                   

                //
                // Set a recipient ID
                xmpiedirector_JobTicket.RecipientsInfo recipientInfo = new xmpiedirector_JobTicket.RecipientsInfo();
                recipientInfo.m_FilterType = 1;     // 1 = Query
                int documentid = Convert.ToInt32(_DocumentID);
                if (documentid == 1882)
                {
                    recipientInfo.m_Filter = ProcessVars.RecipientsDataSourceQueryMLTSS;
                }
                else
                    recipientInfo.m_Filter = ProcessVars.RecipientsDataSourceQueryNJFamily;

                //jobTicketWS.SetOutputFolder(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"]); //"Horizon NJH Production"
                jobTicketWS.SetOutputFolder(ProcessVars.uName, ProcessVars.Password,jobTicketID,ProcessVars.DestinationID_IDCARDS); //"Horizon NJH Production"

                //// Set the job output type  ..PDF name from FileName ADOR
                xmpiedirector_JobTicket.Parameter[] my_params = new xmpiedirector_JobTicket.Parameter[2];
                my_params[0] = new xmpiedirector_JobTicket.Parameter();
                my_params[0].m_Name = "PDF_MULTI_SINGLE_RECORD";
                my_params[0].m_Value = "false";// single PDF for all records. Else set true. 

                my_params[1] = new xmpiedirector_JobTicket.Parameter();
                my_params[1].m_Name = "FILE_NAME_ADOR";
                my_params[1].m_Value = "FileName";

                jobTicketWS.SetOutputParameters(ProcessVars.uName, ProcessVars.Password, jobTicketID, my_params);
                jobTicketWS.SetOutputInfo(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PDFO", 1, ProcessVars.OutputFolderName, null, null);
                jobTicketWS.SetJobType(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PRINT");

                xmpiedirector_Production.Production_SSP productionWS = new xmpiedirector_Production.Production_SSP();
                if (documentid == 1882)
                {
                    jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.DataSourceID_1882);
                }
                else
                    jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.DataSourceID_1880);

                // Submit the job
                // jobID = productionWS.SubmitJob(uName, Password, jobTicketID, "0", "", null);
                string[] jobid = productionWS.SubmitSplittedJob(ProcessVars.uName, ProcessVars.Password, jobTicketID, "0", ProcessVars.SplittedJobBatchSize, "Highest", null, null); //splitted         
               for(int i=0;i<jobid.Length;i++) 
                    {
                      JobIDs += jobid[i]+ ";";

                    }

            }
            catch (Exception ex)
            {
                //Handle error
            }


        }


        private void CheckJobStatus(string JobID)
        {

            string[] JobIDb = JobIDs.Split(';');
            xmpiedirector_Job.Job_SSP Job = new xmpiedirector_Job.Job_SSP();
            foreach (string JID in JobIDb)
            {
                if (JID != "")
                {
                    int status = Job.GetStatus(ProcessVars.uName, ProcessVars.Password, JID);

                    if (status == 3)
                    {
                        // Job Completed
                        JobIDs = JobIDs.Replace(JID, "");
                    }
                    else if (status == 4) //"failed"
                    {

                        //FAILED
                      JobIDs = JobIDs.Replace(JID, "");
                      SendMails sendmail = new SendMails();
                     sendmail.SendMail("NJHId Cards Xmpie Job FAILED for " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,", "noreply@apps.cierant.com", JID);  
                      
                    }
                    else if (status == 2) //In progress
                    {
                        //Wait for some time then do next step
                        var t = Task.Run(async delegate
                        {
                            await Task.Delay(1000 * 60 * 1);
                            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        });
                        t.Wait();
                        SendMails sendmail = new SendMails();
                        sendmail.SendMail("NJHId Cards Xmpie Job PAUSED " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,", "noreply@apps.cierant.com", JID);
                        CheckJobStatus(JobIDs);

                    }

                }

            }
        }
        private static void CopyFile(string sourcefolder, string SourceFile, string DestinationFilePath)
        {
            try
            {

                //code added to check if fileexists donot copy
                if( !(File.Exists(DestinationFilePath + SourceFile)))
                { if (File.Exists(sourcefolder+SourceFile))
                    File.Copy(sourcefolder + SourceFile, DestinationFilePath + SourceFile, true);
                }
            }
            catch (Exception ex)
            {
                StreamWriter sw = new StreamWriter(sourcefolder + "log.txt", true);
                sw.WriteLine("Error occurred while copying this file to another location: " + SourceFile);
                sw.Close();
            }

        }

        private void btnSAPD_Click(object sender, EventArgs e)
        {
            string errors = "";
            string JobIDs = "";
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing SAPD renewal Letters ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            dbU = ProcessVars.oDBUtility();

            //// Download txtfile from ftp

            //////Horizon_EOBS_Parse.N_loadFromFTP DownloadData = new N_loadFromFTP();
            //////N_loadFromFTP downloadDta = new N_loadFromFTP();
            //////errors= downloadDta.FetchFromFTP(GlobalVar.DateofProcess);

            //////if (errors == "")
            //////{

            /////////// if data pushed to network from ftp

            string DirNetwork = ProcessVars.NJHSAPDDirectory;
            string DirToMoveTo = ProcessVars.NJHSAPDDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
            if (!Directory.Exists(DirToMoveTo))
                Directory.CreateDirectory(DirToMoveTo);


            DirectoryInfo DirNetworkInfo = new DirectoryInfo(DirNetwork);
            FileInfo[] FilesTxtNetwork = DirNetworkInfo.GetFiles("*.Txt");
            string[] fileNames = FilesTxtNetwork.Select(f => f.Name).ToArray();
            foreach (string File in fileNames)
            {
                try
                {


                    string txtFileName = File;

                    if (txtFileName.IndexOf("SGSAPD") >= 0 && txtFileName.IndexOf(".txt") > 0)
                    {
                        //check if file loaded already

                        string txtfileNameNJHSAPD = "";
                        int fileExtPos = txtFileName.LastIndexOf(".");
                        if (fileExtPos >= 0)
                            txtfileNameNJHSAPD = txtFileName.Substring(0, fileExtPos);
                        String Sql = "SELECT count(*) FROM HNJH_SAPD_Master WHERE FileName like '" + txtfileNameNJHSAPD + "%'";
                        var numrec = dbU.ExecuteScalar(Sql);
                        int NumRecs = Convert.ToInt32(numrec);


                        if (NumRecs.Equals(0))//New file
                        {

                            CopyFile(DirNetwork, txtFileName, DirToMoveTo);


                        }


                    }

                }


                catch (Exception ez)
                {
                    errors = errors + File + "  " + ez.Message + "\n\n";
                }

            }




            //////// CONTINUING AFTER TXTFILE PUSHED TO DATED FOLDER


            string DirLocal = ProcessVars.NJHSAPDDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
            DirectoryInfo DirNetworkDatedInfo = new DirectoryInfo(DirLocal);
            FileInfo[] FilesTxtDatedNetwork = DirNetworkDatedInfo.GetFiles("*.Txt");


            Parse_IDCards SAPD = new Parse_IDCards();

            ////////dELETE FROM TMP TABLE AND XMPIE TABLE And Temp TO KEEP ONLY THAT DAYS DATA .

            dbU.ExecuteScalar("Delete from HNJH_SAPD_Temp");
            dbU.ExecuteScalar("Delete from HNJH_SAPD_Xmpie");
            dbU.ExecuteScalar("Delete from  HNJH_SAPD_OutputCasTemp");

            foreach (FileInfo file in FilesTxtDatedNetwork)
            {
                try
                {
                    dbU.ExecuteScalar("Delete from HNJH_SAPD_Temp");
                    dbU.ExecuteScalar("Delete from HNJH_SAPD_Xmpie");
                    dbU.ExecuteScalar("Delete from  HNJH_SAPD_OutputCasTemp");

                    errors = SAPD.evaluate_SAPD(file.FullName, DirLocal);

                    if (errors == "")
                    {
                        string __DocumentID = ProcessVars.SAPD_DocumentID;
                        SAPDPDF(__DocumentID);
                        //Check if each Job is complete.If yes, then do the rest of pdf processing, else give some time.

                        CheckSAPDJobStatus(JobID);

                        var t = Task.Run(async delegate
                        {
                            await Task.Delay(1000 * 60 * 10);
                            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        });



                        string txtfilename = Path.GetFileName(file.FullName);



                        // string txtfilename1 = @"\\10.0.200.248\Internal_Production\Horizon_Production_Mngmt\SECURE\TEST_INBOUND\2016-05-09\SGSAPD60DAYRENW_20160401001328.txt";
                        string txtfilename2 = Path.GetFileName(txtfilename);
                        string[] fileNametxt = FilesTxtDatedNetwork.Select(f => f.Name).ToArray();
                        DataSet ds = null;

                        string sql = "select distinct [BROKER ID] from [BCBS_Horizon].[dbo].[HNJH_SAPD_Xmpie] ";
                        ds = dbU.ExecuteDataSet(sql);
                        DataTable dtBroker = ds.Tables[0];
                        List<string> _BrokerId = new List<string>();

                        foreach (DataRow row in dtBroker.Rows)
                        {
                            _BrokerId.Add((string)Convert.ToString(row["BROKER ID"]));
                        }


                        string[] BrokerIds = _BrokerId.ToArray();
                        foreach (string BrokerId in BrokerIds)
                        {
                            CreateZipFileByBrokerId(txtfilename2, BrokerId);
                        }
                        var t1 = Task.Run(async delegate
                        {
                            await Task.Delay(1000 * 60 * 30);
                            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        });
                        //createZipForSci(txtfilename2);
                        //var t2 = Task.Run(async delegate
                        //{
                        //    await Task.Delay(1000 * 60 * 30);
                        //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        //});
                        //moveXmpieSAPDPdfToProcessed();
                        //var t3 = Task.Run(async delegate
                        //{
                        //    await Task.Delay(1000 * 60 * 30);
                        //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        //});


                    }

                }



                catch (Exception ez)
                {
                    errors = errors + ez.Message + "\n\n";
                }

            }

                Results.Text = "";
                Results.Text = "SAPD done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
                objPleaseWait.Close();


              //  SendMails sendmail = new SendMails();
              //  sendmail.SendMail("SAPD DONE AT " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,tkarintholil@apps.cierant.com,cgaytan@apps.cierant.com", "noreply@apps.cierant.com", "");  

        }


        protected void SAPDPDF(string _DocumentID)
        {


            try
            {
                // Create the job ticket web service object    

                xmpiedirector_JobTicket.JobTicket_SSP jobTicketWS = new xmpiedirector_JobTicket.JobTicket_SSP();

                // Create a new job ticket
                string jobTicketID = jobTicketWS.CreateNewTicketForDocument(ProcessVars.uName, ProcessVars.Password, _DocumentID, "", false);

                // jobTicketWS.AddDestinationByID(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"], "", true);
                jobTicketWS.AddDestinationByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_SAPD, "", true);


                //
                // Set a recipient ID
                xmpiedirector_JobTicket.RecipientsInfo recipientInfo = new xmpiedirector_JobTicket.RecipientsInfo();
                recipientInfo.m_FilterType = 3;     // 3 = TableName
                int documentid = Convert.ToInt32(_DocumentID);
                recipientInfo.m_Filter = "HNJH_SAPD_Xmpie";

                //jobTicketWS.SetOutputFolder(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"]); //"Horizon NJH Production"
                jobTicketWS.SetOutputFolder(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_SAPD); //"Horizon NJH Production"

                //// Set the job output type  ..PDF name from FileName ADOR
                xmpiedirector_JobTicket.Parameter[] my_params = new xmpiedirector_JobTicket.Parameter[2];
                my_params[0] = new xmpiedirector_JobTicket.Parameter();
                my_params[0].m_Name = "PDF_MULTI_SINGLE_RECORD";
                my_params[0].m_Value = "true";// single PDF for all records. Else set true. 

                my_params[1] = new xmpiedirector_JobTicket.Parameter();
                my_params[1].m_Name = "FILE_NAME_ADOR";
                my_params[1].m_Value = "Broker_FileName";

                jobTicketWS.SetOutputParameters(ProcessVars.uName, ProcessVars.Password, jobTicketID, my_params);
                jobTicketWS.SetOutputInfo(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PDFO", 1, ProcessVars.OutputFolderNameSAPD, null, null);
                jobTicketWS.SetJobType(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PRINT");

                xmpiedirector_Production.Production_SSP productionWS = new xmpiedirector_Production.Production_SSP();

                jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.DataSourceID_2010);


                // Submit the job
                JobID = productionWS.SubmitJob(ProcessVars.uName, ProcessVars.Password, jobTicketID, "Highest", "", null);




            }
            catch (Exception ex)
            {
                JobID = "";
                SendMails sendmail = new SendMails();
                sendmail.SendMail("SAPD Automation PDF Error " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", ex.Message);


            }


        }

        private void CheckSAPDJobStatus(string JobID)
        {
            xmpiedirector_Job.Job_SSP Job = new xmpiedirector_Job.Job_SSP();
            if (JobID != "")
            {
                int status = Job.GetStatus(ProcessVars.uName, ProcessVars.Password, JobID);
                if (status == 3)//"COMPLETED"
                {
                    JobID = "";
                }
                else if (status == 4) //"failed"
                {
                    JobID = "";
                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("SAPD Xmpie Job FAILED " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", JobID);

                }
                else if (status == 2) //In progress
                {
                    var t = Task.Run(async delegate
                    {
                        await Task.Delay(1000 * 60 * 2);
                        return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    });


                    CheckSAPDJobStatus(JobID);
                }

            }


        }



        private void CreateZipFileByBrokerId(string InputFileName, string BrokerId)
        {


            string HorizonDir = @ProcessVars.NJHSAPDDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + "Horizon";
            if (!Directory.Exists(HorizonDir))
                Directory.CreateDirectory(HorizonDir);



            string inputFileNameWithouttxt = InputFileName.Replace(".txt", "");//ex:SGSAPD60DAYRENW_20160401001328

            string fullNamesPDFFilePath = "";
            string zipname = "";
            string zipfullnamepath = "";
            string finalPDFfilename = "";

            string DirXmpieSAPDpdfFilePath = @ProcessVars.XmpieSAPDpdfPath;
            DirectoryInfo DirPdfPathInfo = new DirectoryInfo(DirXmpieSAPDpdfFilePath);
            FileInfo[] FilesSAPDPdf = DirPdfPathInfo.GetFiles(BrokerId + "*.PDF");
            string[] fileNamesSAPD = FilesSAPDPdf.Select(f => f.Name).ToArray();
            if (fileNamesSAPD.Length > 0)
            {
                int indexpospdf = fileNamesSAPD[0].LastIndexOf("_");
                zipname = fileNamesSAPD[0].Substring(0, indexpospdf) + ".ZIP";
                zipfullnamepath = Path.Combine(HorizonDir, zipname);
            }
            foreach (FileInfo _filenameSAPDPdf in FilesSAPDPdf)
            {
                fullNamesPDFFilePath = _filenameSAPDPdf.FullName;
                string _filenamepdf = _filenameSAPDPdf.Name;
                AddToArchive(zipfullnamepath, fullNamesPDFFilePath, _filenamepdf, CompressionLevel.Optimal);





            }

        }

        private void AddToArchive(string archiveFullName, string fullNamesPDFFilePath, string _filenamepdf, CompressionLevel compression = CompressionLevel.Optimal)
        {
            //Identifies the mode we will be using - the default is Create
            ZipArchiveMode mode = ZipArchiveMode.Create;

            //Determines if the zip file even exists
            bool archiveExists = File.Exists(archiveFullName);

            //Figures out what to do based upon our specified overwrite method

            if (archiveExists)
            {
                mode = ZipArchiveMode.Update;
            }



            //Opens the zip file in the mode we specified
            using (ZipArchive zipFile = ZipFile.Open(archiveFullName, mode))
            {

                if (mode == ZipArchiveMode.Create)
                {

                    //Adds the file to the archive
                    zipFile.CreateEntryFromFile(fullNamesPDFFilePath, _filenamepdf, CompressionLevel.Fastest);

                }
                else
                {
                    zipFile.CreateEntryFromFile(fullNamesPDFFilePath, _filenamepdf, CompressionLevel.Fastest);
                }

            }

        }

        private void createZipForSci(string InputFileName)
        {
            string zipname = "";
            string zipfullnamepath = "";
            string fullNamesPDFFilePath = "";
            string Scidirectory = @ProcessVars.NJHSAPDDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + "Sci";
            if (!Directory.Exists(Scidirectory))
                Directory.CreateDirectory(Scidirectory);
            string DirXmpieSAPDpdfFilePath = @ProcessVars.XmpieSAPDpdfPath;
            DirectoryInfo DirPdfPathInfo = new DirectoryInfo(DirXmpieSAPDpdfFilePath);
            FileInfo[] FilesSAPDPdf = DirPdfPathInfo.GetFiles("*.pdf");
            zipname = InputFileName.Replace(".txt", ".ZIP");
            zipfullnamepath = Path.Combine(Scidirectory, zipname);
            foreach (FileInfo _filenameSAPDPdf in FilesSAPDPdf)
            {
                fullNamesPDFFilePath = _filenameSAPDPdf.FullName;
                string _filenamepdf = _filenameSAPDPdf.Name;
                AddToArchive(zipfullnamepath, fullNamesPDFFilePath, _filenamepdf, CompressionLevel.Optimal);

            }
        }

        private void moveXmpieSAPDPdfToProcessed()
        {
            string DirToMoveMonthlyXmpieSAPDPdfFiles = ProcessVars.XmpieSAPDpdfProcessedPath + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
            if (!Directory.Exists(DirToMoveMonthlyXmpieSAPDPdfFiles))
                Directory.CreateDirectory(DirToMoveMonthlyXmpieSAPDPdfFiles);

            string SourceDirectory = ProcessVars.XmpieSAPDpdfPath;
            DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SourceDirectory);
            FileInfo[] FilesPdfInSourceD = DirSourceDirectoryInfo.GetFiles("*.pdf");
            string[] fileNames = FilesPdfInSourceD.Select(f => f.Name).ToArray();
            foreach (string File in fileNames)
            {
                string Pdffilename = File;
                String SourceFile = Path.Combine(ProcessVars.XmpieSAPDpdfPath, Pdffilename);
                String DestinationFile = Path.Combine(DirToMoveMonthlyXmpieSAPDPdfFiles, Pdffilename);
                System.IO.File.Move(SourceFile, DestinationFile);
            }

        }
       private void uploadDsnpNjhIdCardsToFtp()
        {
            String error = "Error uploading";
            dbU = ProcessVars.oDBUtility();
            string SciFolder = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\DSNP\" + @"SCI\";
            DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SciFolder);
            string[] filefullPath = Directory.GetFiles(SciFolder, "*.ZIP");
            FileInfo[] fileDetail = DirSourceDirectoryInfo.GetFiles("*.ZIP");
            string[] fileNames = fileDetail.Select(f => f.Name).ToArray();
            int _Port = 22;
            string FileName = "";
            Sftp oSftp = new Sftp(ProcessVars.gHNJHIDCards_FTP_URL, ProcessVars.gHNJHIDCards_FTPUserNameProd, ProcessVars.gHNJHIDCards_FTPPwdProd);
            string HtmlBody = "";
            HtmlBody += "<table  id=\"SciUploadTable\" bgcolor='#ffffcc' style='width:100%;border:1px solid black ;border-collapse: collapse'  <tr><th align='center' valign='middle'>FileName </th><th align='center' valign='middle'>Import_Date </th><th align='center' valign='middle'>Status</th></tr>";
            string status = "";


            if (filefullPath != null && filefullPath.Length > 0)
            {

                try
                {
                    oSftp.Connect(_Port);
                    if (oSftp.Connected)
                    {
                        for (int i = 0; i < filefullPath.Length; i++)
                        {
                            string fullfilepath = filefullPath[i];
                            string filenameonly = fileNames[i];

                            try
                            {

                                oSftp.Put(filefullPath[i], ProcessVars.gHNJHIDCards_FTPLocationProd + filenameonly);
                                status = "Uploaded/ok";
                            }

                            catch (Exception ex)
                            {

                                SendMails sendmail = new SendMails();
                                sendmail.SendMail("Error uploading Dsnpfiles to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com,cgaytan@apps.cierant.com", "noreply@apps.cierant.com", error);

                            }

                            HtmlBody += "<tr><td align='center' valign='middle' style='border:1px solid black'>" + filenameonly + "</td><td align='center' valign='middle' style='border:1px solid black'>" + DateTime.Now.ToString() + "</td><td align='center' valign='middle' style='border:1px solid black'> " + status + "</td></tr>";



                        }
                        HtmlBody += "</table>";

                    }

                }
                catch (Exception ex)
                {

                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("Error uploading files to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com", error);

                }
                finally
                {
                    oSftp.Close();

                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("NJHID DsnpCards Posted to CaptainCrunch on " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,tclinton@apps.cierant.com,rchico@apps.cierant.com,jcioban@cierant.com,cgaytan@apps.cierant.com,pgnecco@sciimage.com,edymek@sciimage.com,jnunez@sciimage.com,mscherman@sciimage.com,msundburg@sciimage.com,todonnell@sciimage.com", "noreply@apps.cierant.com", HtmlBody);
                    Results.Text = "";
                    Results.Text = "Dsnp Posting Successful!";

                }


            }

            else
            {
                string Error = "Could not find zip files to upload";
                SendMails sendmail = new SendMails();
                sendmail.SendMail("CouldNot find njhId Dsnpzip files to upload  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com", Error);

            }






    }





       private void MoveXmpiePdfsToProcessedFolder()
       {

           string DirToMoveDailyXmpiePdfFiles = ProcessVars.XmpiepdfProcessedPath + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
           if (!Directory.Exists(DirToMoveDailyXmpiePdfFiles))
               Directory.CreateDirectory(DirToMoveDailyXmpiePdfFiles);

           string SourceDirectory = ProcessVars.XmpiepdfPath;
           DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SourceDirectory);
           FileInfo[] FilesPdfInSourceD = DirSourceDirectoryInfo.GetFiles("*.pdf");
           string[] fileNames = FilesPdfInSourceD.Select(f => f.Name).ToArray();
           foreach (string File in fileNames)
           {
               string Pdffilename = File;
               String SourceFile = Path.Combine(ProcessVars.XmpiepdfPath, Pdffilename);
               String DestinationFile = Path.Combine(DirToMoveDailyXmpiePdfFiles, Pdffilename);
               System.IO.File.Move(SourceFile, DestinationFile);
           }







       }





        private void hnjhdsnp_Click(object sender, EventArgs e)
        {
          // incoming=HNJHID_Medicare_12162016.txt
           
           StringBuilder sb = new StringBuilder();
           
           string errors = "";
           Cursor.Current = Cursors.WaitCursor;
           Results.Text = "Processing NjHDsnpID Cards ...";
           PleaseWait objPleaseWait = new PleaseWait();
           objPleaseWait.Show();
           dbU = ProcessVars.oDBUtility();

           string DirNetwork = ProcessVars.NJHIDDsnpCardsDirectory;
           string DirToMoveTo = ProcessVars.NJHIDDsnpCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\DSNP\";
           if (!Directory.Exists(DirToMoveTo))
               Directory.CreateDirectory(DirToMoveTo);

           DirectoryInfo DirNetworkInfo = new DirectoryInfo(DirNetwork);
           FileInfo[] FilesTxtNetwork = DirNetworkInfo.GetFiles("*_Medicare_*.Txt");
           string[] fileNames = FilesTxtNetwork.Select(f => f.Name).ToArray();
           if (fileNames.Length > 0)
           {


               foreach (string File in fileNames)
               {
                   try
                   {


                       string txtFileName = File;

                       if (txtFileName.IndexOf("HNJHID") >= 0 && txtFileName.IndexOf("_Medicare") >= 0 && txtFileName.IndexOf(".txt") > 0)
                       {
                           //check if file loaded already

                           string txtfileNameNJHID = "";
                           int fileExtPos = txtFileName.IndexOf("_");
                           if (fileExtPos >= 0)
                               txtfileNameNJHID = txtFileName.Substring(0, fileExtPos);

                           txtfileNameNJHID = GetFileNameAfterSplit(txtFileName);





                           String Sql = "select count(*) from  [HNJH_DSNPIDCards] where SUBSTRING(FileName, 1, CHARINDEX('_', FileName) - 1)='" + txtfileNameNJHID + "'";

                           var numrec = dbU.ExecuteScalar(Sql);
                           int NumRecs = Convert.ToInt32(numrec);
                           if (NumRecs.Equals(0))//New file
                           {

                               CopyFile(DirNetwork, txtFileName, DirToMoveTo);


                           }
                           else
                           {
                               errors = "File Already Exists In Database/delete the file and try again";
                               Results.Text = errors;
                               StreamWriter sw = new StreamWriter(DirNetwork + "log.txt", true);
                               sw.WriteLine("File Already Exists In Database: " + txtFileName);
                               sw.Close();
                               SendMails sendmail = new SendMails();
                               sendmail.SendMail("NJHDSNPId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "NJHDSNP- " + File +" Already Exists in the database ..." + DateTime.Now.ToString("yyyy-mm-dd"));


                           }


                       }

                   }


                   catch (Exception ez)
                   {
                       errors = errors + File + "  " + ez.Message + "\n\n";
                   }

               }



               ////// CONTINUING AFTER TXTFILE PUSHED TO DATED\DSNP FOLDER
               if (errors == "")
               {

                   string DirLocal = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\DSNP\";
                   DirectoryInfo DirNetworkDatedInfo = new DirectoryInfo(DirLocal);
                   FileInfo[] FilesTxtDatedNetwork = DirNetworkDatedInfo.GetFiles("HNJHID_Medicare_*.Txt");

                   ////dELETE FROM TMP TABLE AND XMPIE TABLE And Temp TO KEEP ONLY THAT DAYS DATA .
                   

                   NJHIDParse_new HnjhIdcards = new NJHIDParse_new();
                   //Horizon_EOBS_Parse.NJHIDParse_new HnjhIdcards = new Horizon_EOBS_Parse.NJHIDParse_new();
                   //NJHIDParse HnjhIdcards = new NJHIDParse();
                   foreach (FileInfo file in FilesTxtDatedNetwork)
                   {







                       ////  //start recheck 

                       string txtfileNameNJHID = "";
                       int fileExtPos = file.Name.IndexOf("_");
                       if (fileExtPos >= 0)
                           txtfileNameNJHID = file.Name;

                       txtfileNameNJHID = GetFileNameAfterSplit(txtfileNameNJHID);

                       //String Sql = "select count(*) from  [HNJH_DSNPIDCards] where  SUBSTRING(FileName, 1, 12)='" + txtfileNameNJHID + "'";
                       String Sql = "select count(*) from  [HNJH_DSNPIDCards] where SUBSTRING(FileName, 1, CHARINDEX('_', FileName) - 1)='" + txtfileNameNJHID + "'";
                       var numrec = dbU.ExecuteScalar(Sql);
                       int NumRecs = Convert.ToInt32(numrec);

                       if (NumRecs.Equals(0))//New file
                       {


                           try
                           {
                               dbU.ExecuteScalar("Delete from HNJH_DSNPIDCards_Temp");
                               dbU.ExecuteScalar("Delete from HNJH_IDDsnpCards_Xmpie");
                               dbU.ExecuteScalar("Delete from HNJH_DSNPIDCards_Temp_Held");
                               dbU.ExecuteScalar("delete from HNJH_IDDsnpCards_OutputCasTemp");

                               errors = HnjhIdcards.evaluate_HNJHIDDsnpCards(file.FullName, DirLocal);
                               if (errors == "")
                               {

                                   string _DocumentID = ProcessVars.DSNPDocumentId;
                                   {
                                       DSNPID_Cards(_DocumentID);
                                   }
                                   //Check if each Job is complete.If yes, then do the rest of pdf processing, else give some time.


                                   CheckJobStatus(JobIDs);

                                   var t = Task.Run(async delegate
                                   {
                                       await Task.Delay(1000 * 60 * 1);
                                       return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                                   });
                                   t.Wait();



                                   string errorInCSV = "";
                                   string txtfilename = Path.GetFileName(file.FullName);//WILL GIVE YOU HNJHID_Medicare_12162016.TXT
                                   //MAKE THE FILE NAME TO BE HNJHID121616 
                                   string FilenameForCsv = "";
                                   FilenameForCsv = GetFileNameAfterSplit(txtfilename);

                                   FileInfo[] FilesCSVDatedNetwork = DirNetworkDatedInfo.GetFiles(FilenameForCsv + "_*.csv");

                                   string[] fileNamecsv = FilesCSVDatedNetwork.Select(f => f.Name).ToArray();

                                   //HNJHID121616_ID_DSNP_20161216174704_1
                                   if (fileNamecsv.Count() == 0)
                                       errorInCSV = "1";
                                   foreach (string Filecsv in fileNamecsv)
                                   {


                                       CreateZipFileForDSNP(Filecsv); //HNJHID121616_ID_DSNP_20161216174704_1.csv



                                   }

                                   try
                                   {
                                       MoveXmpieDSNPPdfsToProcessedFolder();
                                   }

                                   catch (Exception ez)
                                   {
                                       errors = errors + file + "  " + ez.Message + "\n\n";
                                   }
                               }
                               else
                               {
                                   var msg2 = "";
                               }


                           }
                           catch (Exception ez)
                           {
                               errors = errors + file + "  " + ez.Message + "\n\n";
                           }



                       }

                       ////   //end recheck























                       /////start




                       //try
                       //{
                       //    dbU.ExecuteScalar("Delete from HNJH_DSNPIDCards_Temp");
                       //    dbU.ExecuteScalar("Delete from HNJH_IDDsnpCards_Xmpie");

                       //    dbU.ExecuteScalar("delete from HNJH_IDDsnpCards_OutputCasTemp");

                       //    errors = HnjhIdcards.evaluate_HNJHIDDsnpCards(file.FullName, DirLocal);
                       //    if (errors == "")
                       //    {

                       //        string _DocumentID = ProcessVars.DSNPDocumentId;
                       //        {
                       //            DSNPID_Cards(_DocumentID);
                       //        }
                       //        //Check if each Job is complete.If yes, then do the rest of pdf processing, else give some time.


                       //        CheckJobStatus(JobIDs);

                       //        var t = Task.Run(async delegate
                       //        {
                       //            await Task.Delay(1000 * 60 * 1);
                       //            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                       //        });
                       //        t.Wait();




                       //        string txtfilename = Path.GetFileName(file.FullName);//WILL GIVE YOU HNJHID_Medicare_12162016.TXT
                       //        //MAKE THE FILE NAME TO BE HNJHID121616 
                       //        string FilenameForCsv = "";
                       //        FilenameForCsv = GetFileNameAfterSplit(txtfilename);

                       //        FileInfo[] FilesCSVDatedNetwork = DirNetworkDatedInfo.GetFiles(FilenameForCsv + "_*.csv");

                       //        string[] fileNamecsv = FilesCSVDatedNetwork.Select(f => f.Name).ToArray();

                       //        //HNJHID121616_ID_DSNP_20161216174704_1
                       //        foreach (string Filecsv in fileNamecsv)
                       //        {


                       //            CreateZipFileForDSNP(Filecsv); //HNJHID121616_ID_DSNP_20161216174704_1.csv



                       //        }


                       //        MoveXmpieDSNPPdfsToProcessedFolder();

                       //    }



                       //}
                       //catch (Exception ez)
                       //{
                       //    errors = errors + file + "  " + ez.Message + "\n\n";
                       //}


                       ////end

                   }

                   if (errors == "")
                   {

                       //--move the HNJHID.TXT FILES TO PROCESSED FOLDER--//


                       string DirToMoveDailyTXTfILES = ProcessVars.NJHIDCardsDirectory + "Processed" + @"\";
                       if (!Directory.Exists(DirToMoveDailyTXTfILES))
                           Directory.CreateDirectory(DirToMoveDailyTXTfILES);

                       string SourceDirectory = ProcessVars.NJHIDCardsDirectory;
                       DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SourceDirectory);

                       //HNJHID_Medicare_12162016
                       FileInfo[] FilesTxtInSourceD = DirSourceDirectoryInfo.GetFiles("HNJHID_Medicare_*.txt");
                       string[] fileNames1 = FilesTxtInSourceD.Select(f => f.Name).ToArray();
                       foreach (string File in fileNames1)
                       {
                           string Txtfilename = File;
                           String SourceFile = Path.Combine(ProcessVars.NJHIDCardsDirectory, Txtfilename);
                           String DestinationFile = Path.Combine(DirToMoveDailyTXTfILES, Txtfilename);
                           System.IO.File.Move(SourceFile, DestinationFile);
                       }





                       Results.Text = "";
                       Results.Text = "NJHDSNPID Cards done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
                       objPleaseWait.Close();


                       SendMails sendmail = new SendMails();
                       sendmail.SendMail("NJHDSNPId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com", "noreply@apps.cierant.com", "Finished processing NjhDsnpId cards at  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

                       uploadDsnpNjhIdCardsToFtp();





                   }
                   else
                   {
                       Results.Text = "";
                       Results.Text = "NjhId cards is not processed.";
                       objPleaseWait.Close();
                       SendMails sendmail = new SendMails();
                       sendmail.SendMail("NJHDSNPId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "NJHDSNPId Cards cards finished processing with errors..." + DateTime.Now.ToString("yyyy-mm-dd"));
                   }
               }
               else
               {
                   Results.Text = errors;
               }
           }
           else
           {
               Results.Text = "";
               Results.Text = "Input Dsnp file Not Found";
               SendMails sendmail = new SendMails();
               sendmail.SendMail("NJHDSNPId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "NJHDSNPId file not found in the path ..." + DateTime.Now.ToString("yyyy-mm-dd"));
           }
        }





        protected void DSNPID_Cards(string _DocumentID)
        {


            try
            {
                // Create the job ticket web service object    

                xmpiedirector_JobTicket.JobTicket_SSP jobTicketWS = new xmpiedirector_JobTicket.JobTicket_SSP();

                // Create a new job ticket
                string jobTicketID = jobTicketWS.CreateNewTicketForDocument(ProcessVars.uName, ProcessVars.Password, _DocumentID, "", false);

                // jobTicketWS.AddDestinationByID(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"], "", true);
                jobTicketWS.AddDestinationByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_IDCARDS, "", true);


                //
                // Set a recipient ID
                xmpiedirector_JobTicket.RecipientsInfo recipientInfo = new xmpiedirector_JobTicket.RecipientsInfo();
                recipientInfo.m_FilterType = 1;     // 1 = Query
                int documentid = Convert.ToInt32(_DocumentID);
                if (documentid == 2764)
                {
                    recipientInfo.m_Filter = ProcessVars.RecipientsDataSourceQueryDSNP;
                }
               

                //jobTicketWS.SetOutputFolder(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"]); //"Horizon NJH Production"
                jobTicketWS.SetOutputFolder(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_IDCARDS); //"Horizon NJH Production"

                //// Set the job output type  ..PDF name from FileName ADOR
                xmpiedirector_JobTicket.Parameter[] my_params = new xmpiedirector_JobTicket.Parameter[2];
                my_params[0] = new xmpiedirector_JobTicket.Parameter();
                my_params[0].m_Name = "PDF_MULTI_SINGLE_RECORD";
                my_params[0].m_Value = "false";// single PDF for all records. Else set true. 

                my_params[1] = new xmpiedirector_JobTicket.Parameter();
                my_params[1].m_Name = "FILE_NAME_ADOR";
                my_params[1].m_Value = "FileName";

                jobTicketWS.SetOutputParameters(ProcessVars.uName, ProcessVars.Password, jobTicketID, my_params);
                jobTicketWS.SetOutputInfo(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PDFO", 1, ProcessVars.OutputFolderName, null, null);
                jobTicketWS.SetJobType(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PRINT");

                xmpiedirector_Production.Production_SSP productionWS = new xmpiedirector_Production.Production_SSP();
                if (documentid == 2764)
                {
                    jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.DataSourceID_2764);
                }
               

                // Submit the job
                // jobID = productionWS.SubmitJob(uName, Password, jobTicketID, "0", "", null);
                string[] jobid = productionWS.SubmitSplittedJob(ProcessVars.uName, ProcessVars.Password, jobTicketID, "0", ProcessVars.SplittedJobBatchSize, "Highest", null, null); //splitted         
                for (int i = 0; i < jobid.Length; i++)
                {
                    JobIDs += jobid[i] + ";";

                }

            }
            catch (Exception ex)
            {
                //Handle error
            }


        }































        private void CreateZipFileForDSNP(string File1)//HNJHID121616_ID_DSNP_20161216174704_1.csv
        {


             //HNJHID121616_ID-CD_DSNP_20161216174704_1.csv
               //HNJHID031016_ID_DSNP_YYYMMDDHHMMSS_1.PDF

            //FINALPDFTOGOINZIP=HNJHID031016_ID_DSNP_YYYMMDDHHMMSS.PDF //SO REMOVE _1.PDF TO .PDF
            //FINALCSVTOGOINZIP=HNJHID121616_ID_DSNP_20161216174704.CSV
            //ZIP NAME=HNJHID031016_DSNP_PACKAGE_DATETIME.ZIP




            dbU = ProcessVars.oDBUtility();
            string fullNamesPDFFilePath = "";
            string finalcsvtogoinzip="";
            string finalpdftogoinzip="";
            
            string zipfilename = File1.Substring(0, 12) + "_DSNP_PACKAGE_" + DateTime.Now.ToString("yyyyMMddhhmmss") + ".ZIP";
            int indexpos=File1.LastIndexOf("_");
            if (indexpos >= 0)
                        {
                            finalcsvtogoinzip = File1.Remove(indexpos)+".csv";
                            finalpdftogoinzip = File1.Remove(indexpos) + ".pdf";

                        }


         
            string FullNamesCSVFilePath = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\DSNP\"+File1;
            string scidirectory = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\DSNP\" + "SCI";
            if (!Directory.Exists(scidirectory))
                Directory.CreateDirectory(scidirectory);

            string zipfilepath = Path.Combine(scidirectory, zipfilename);

            string[] NJHIDDSNPCsvFormat = ProcessVars.NJHIDDSNPCsvFormat.Split(',');
            string[] NJHIDDSNPPdfFormat =ProcessVars.NJHIDDSNPPdfFormat.Split(',');
            string DirXmpiePdfFilePath = @ProcessVars.XmpiepdfPath;
            DirectoryInfo DirPdfPathInfo = new DirectoryInfo(DirXmpiePdfFilePath);
            DirectoryInfo DirCsvPathInfo = new DirectoryInfo(FullNamesCSVFilePath);

            for (int i = 0; i < NJHIDDSNPCsvFormat.Length; i++)
            {



                if (File1.IndexOf(NJHIDDSNPCsvFormat[i]) >= 0)
                {

                    FileInfo[] FilesNjCardsPdf = DirPdfPathInfo.GetFiles("*" + NJHIDDSNPPdfFormat[i]);
                    if (FilesNjCardsPdf.Length == 0)
                    {
                        fullNamesPDFFilePath = "";
                    }
                    else
                    {
                        foreach (FileInfo _filenamePDF1 in FilesNjCardsPdf)
                        {
                            fullNamesPDFFilePath = _filenamePDF1.FullName;

                            




                        }

                        createzipfile(FullNamesCSVFilePath, fullNamesPDFFilePath, finalcsvtogoinzip, finalpdftogoinzip, zipfilename, zipfilepath);


                    }





                }


            }


         





        }



        public string GetFileNameAfterSplit(string txtFileName)
        {



            //incoming file name=HNJHID_Medicare_12162016.txt make it HNJHID121616
            string[] stringaftersplit;
            stringaftersplit = (txtFileName.Split('_'));
            string stringafterremovingtxt = stringaftersplit[2].ToString().Remove(4) + DateTime.Now.ToString("yy");
            string dateFileName = stringaftersplit[2].ToString();
            string finalstring = "";
            if (dateFileName.ToString().Substring(0, 4) == DateTime.Today.Year.ToString() && dateFileName.Length == 12)
            {
                finalstring = string.Concat(stringaftersplit[0].ToString(), dateFileName.ToString().Replace(".txt",""));
            }
            else
            {
                DateTime dt = new DateTime(Int32.Parse(dateFileName.Substring(4, 4)), Int32.Parse(dateFileName.Substring(0, 2)), Int32.Parse(dateFileName.Substring(2, 2)));
                int julianF = dt.Year * 1000 + dt.DayOfYear;
                finalstring = string.Concat(stringaftersplit[0].ToString(), stringafterremovingtxt);
                if (stringaftersplit.Length == 4)
                    finalstring = string.Concat(stringaftersplit[0].ToString(), julianF, stringaftersplit[3].ToString().Replace(".txt", ""));
                else
                    finalstring = string.Concat(stringaftersplit[0].ToString(), julianF, "0");
            }
            return finalstring;
        }

        private void button28_Click(object sender, EventArgs e)
        {
            uploadNjhIdCardsToFtp();
        }

      

        private void HnjhReprocess_Click_1(object sender, EventArgs e)
        {
            string errors = "";

            // string JobIDs = "";
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing NjHID Cards ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            dbU = ProcessVars.oDBUtility();



            string DirNetwork = ProcessVars.NJHIDCardsDirectory;
            string DirToMoveTo = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
            if (!Directory.Exists(DirToMoveTo))
                Directory.CreateDirectory(DirToMoveTo);



            if (errors == "")
            {

                string DirLocal = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
                // DirectoryInfo DirNetworkDatedInfo = new DirectoryInfo(DirLocal);
                // FileInfo[] FilesTxtDatedNetwork = DirNetworkDatedInfo.GetFiles("HNJH*.Txt");


                Parse_IDCards HnjhIdcards = new Parse_IDCards();

                //////dELETE FROM TMP TABLE AND XMPIE TABLE And Temp TO KEEP ONLY THAT DAYS DATA .



                // foreach (FileInfo file in FilesTxtDatedNetwork)
                {
                    try
                    {
                        string fileName = "HNJHIDReprocess" + GlobalVar.DateofProcess.ToString("MMddyyyy");
                        dbU.ExecuteScalar("Delete from HNJH_IDCards_Temp");
                        dbU.ExecuteScalar("Delete from HNJH_IDCards_Xmpie");
                        dbU.ExecuteScalar("Delete from HNJH_IDCards_Xmpie_MLTSS");
                        dbU.ExecuteScalar("Delete from  HNJH_IDCards_Reprocess");

                        errors = ""; //HnjhIdcards.evaluate_HNJHIDCards_Reprocess(fileName, DirLocal);
                        if (errors == "")
                        {

                            string[] ProcessDocs = ProcessVars.DocumentIDs.Split(',');
                            foreach (string _DocumentID in ProcessDocs)
                            {
                                ID_Cards(_DocumentID);
                            }
                            //Check if each Job is complete.If yes, then do the rest of pdf processing, else give some time.


                            CheckJobStatus(JobIDs);

                            var t = Task.Run(async delegate
                            {
                                await Task.Delay(1000 * 60 * 1);
                                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                            });
                            t.Wait();




                            //string txtfilename = Path.GetFileName(file.FullName);
                            //string[] fileNametxt = FilesTxtDatedNetwork.Select(f => f.Name).ToArray();
                            //foreach (string File in fileNametxt)
                            //    if (File == txtfilename)
                            {

                                CreateZipFileForNJFamilyCare_Reprocess(fileName);
                                CreateZipFileForMLTSS_Reprocess(fileName);
                                MoveXmpiePdfsToProcessedFolder();
                            }

                        }
                        else
                        {
                            Results.Text = "";
                            Results.Text = "NJHID Cards not processed-Bcc failed/Data not in correct format " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
                            SendMails sendmail = new SendMails();
                            sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "Njhid cards -Bcc Failed/File Not in Correct Format..." + DateTime.Now.ToString("yyyy-mm-dd"));
                            Application.Exit();
                        }


                    }
                    catch (Exception ez)
                    {
                        errors = errors + "  " + ez.Message + "\n\n";
                    }

                }


                if (errors == "")
                {








                    Results.Text = "";
                    Results.Text = "NJHID Reprocessed Cards done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
                    objPleaseWait.Close();


                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("NJHId Reprocessed Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com", "noreply@apps.cierant.com", "Finished processing NjhId Reprocessed cards at  " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

                    uploadNjhIdCardsToFtp_Reprocess();





                }
                else
                {
                    Results.Text = "";
                    Results.Text = "NjhId Reprocessed cards is not processed.";
                    objPleaseWait.Close();
                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("NJHId Reprocessed Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "Njhid Reprocessed cards finished processing with errors..." + DateTime.Now.ToString("yyyy-mm-dd"));
                }


            }

            else
            {
                Results.Text = "";
                Results.Text = "NjhId Reprocessed cards is not processed.Pls put the file in the correct location";
                objPleaseWait.Close();
                SendMails sendmail = new SendMails();
                sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "Njhid cards finished processing with errors..." + DateTime.Now.ToString("yyyy-mm-dd"));
            }



        }


        private void CreateZipFileForNJFamilyCare_Reprocess(string InputFileName)
        {
            //csvfilename= HNJHID030916_ID_20160331140500_1.csv
            //pdffilename=HNJHID031016_ID_20160330124408_1.pdf
            //string filename="HNJHID031016_PACKAGE_20160331140608.ZIP";

            dbU = ProcessVars.oDBUtility();
            // string scidirectory = @"\\10.0.200.248\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\TEST_INBOUND\2016-03-31\SCI";
            string scidirectory = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + "SCI_Reprocess";
            if (!Directory.Exists(scidirectory))
                Directory.CreateDirectory(scidirectory);


            //string NJHIDCsvFormatNjCard = "_1.csv,_10001.csv,_20001.csv,_30001.csv,_40001.csv,_50001.csv,_60001.csv,_70001.csv,_80001.csv,_90001.csv,_100001.csv,_110001.csv";


            string[] NJHIDCsvFormatNjCards = ProcessVars.NJHIDCsvFormatNjCard.Split(',');
            //string[] NJHIDPdfFormatNjCards =ProcessVars.NJHIDPdfFormatNjCard.Split(',');


            string inputFileNameWithouttxt = InputFileName;//ex:-HNJHID030916


            string fullNamesCsvFilePath = "";
            string fullNamesPDFFilePath = "";
            string zipname = "";
            string zipfullnamepath = "";
            string finalCSVfilename = "";
            string finalPDFfilename = "";
            string tempCSVNAME = "";

            foreach (string NJHIDCsvFormatNjCardt in NJHIDCsvFormatNjCards)
            {
                List<string> files = new List<string>();
                string DirCsvFilePath = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";

                DirectoryInfo DirCsvFilePathInfo = new DirectoryInfo(DirCsvFilePath);

                FileInfo[] FilesNjCardCsv = DirCsvFilePathInfo.GetFiles(inputFileNameWithouttxt + "*" + NJHIDCsvFormatNjCardt);//_1.csv

                foreach (FileInfo _filenameCSV1 in FilesNjCardCsv)
                {
                    fullNamesCsvFilePath = _filenameCSV1.FullName;
                }
                string[] fileNameCsv = FilesNjCardCsv.Select(f => f.Name).ToArray();


                foreach (string _filenameCSV in fileNameCsv)
                {
                    int first_pos = _filenameCSV.IndexOf("_");
                    string _filename = _filenameCSV.Substring(0, first_pos);
                    //check if the csv filenames (first few letters) and input txtfilename are same like 'HNJHID030916=HNJHID030916'.checking this  if there are multiple csvs created from running more than one input txt file in the datedfolder.
                    if (_filename == inputFileNameWithouttxt)
                    {
                        int indexpos = _filenameCSV.LastIndexOf("_");
                        if (indexpos >= 0)
                        {
                            tempCSVNAME = _filenameCSV.Substring(0, indexpos);
                            finalCSVfilename = _filenameCSV.Substring(0, indexpos) + ".csv";
                            zipname = finalCSVfilename.Replace("_ID_", "_PACKAGING_").Replace(".csv", ".zip");
                            zipfullnamepath = Path.Combine(scidirectory, zipname);
                            if (File.Exists(zipfullnamepath))
                            {
                                break;
                            }

                        }

                    }


                    string DirXmpiePdfFilePath = @ProcessVars.XmpiepdfPath;

                    DirectoryInfo DirPdfPathInfo = new DirectoryInfo(DirXmpiePdfFilePath);
                    string NJHIDPdfFormatNjCardt = NJHIDCsvFormatNjCardt.Replace("csv", "pdf");//"_1.csv" to "_1.pdf"

                    FileInfo[] FilesNjCardsPdf = DirPdfPathInfo.GetFiles("*" + NJHIDPdfFormatNjCardt);
                    if (FilesNjCardsPdf.Length == 0)
                    {
                        fullNamesPDFFilePath = "";
                    }
                    else
                    {
                        foreach (FileInfo _filenamePDF1 in FilesNjCardsPdf)
                        {
                            fullNamesPDFFilePath = _filenamePDF1.FullName;
                        }
                    }

                    string[] fileNamePdf = FilesNjCardsPdf.Select(f => f.Name).ToArray();

                    foreach (string _filenamepdf in fileNamePdf)
                    {
                        int indexpospdf = _filenamepdf.LastIndexOf("_");
                        if (indexpospdf >= 0)
                        {
                            finalPDFfilename = tempCSVNAME + ".pdf";


                        }
                    }
                    if (fullNamesPDFFilePath != "")
                    {
                        createzipfile(fullNamesCsvFilePath, fullNamesPDFFilePath, finalCSVfilename, finalPDFfilename, zipname, zipfullnamepath);
                    }


                }

            }
        }

        private void CreateZipFileForMLTSS_Reprocess(string InputFileName)
        {

            string scidirectory = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + "SCI_Reprocess";

            if (!Directory.Exists(scidirectory))
                Directory.CreateDirectory(scidirectory);



            string[] NJHIDCsvFormatMLTSSs = ProcessVars.NJHIDCsvFormatMLTSS.Split(',');
            string[] NJHIDPdfFormatMLTSSs = ProcessVars.NJHIDPdfFormatMLTSS.Split(',');


            string inputFileNameWithouttxt = InputFileName;

            string fullNamesCsvFilePath = "";
            string fullNamesPDFFilePath = "";
            string zipname = "";
            string zipfullnamepath = "";
            string finalCSVfilename = "";
            string finalPDFfilename = "";
            string tempCSVNAME = "";

            foreach (string NJHIDCsvFormatMLTSSst in NJHIDCsvFormatMLTSSs)
            {
                List<string> files = new List<string>();
                string DirCsvFilePath = @ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";

                DirectoryInfo DirCsvFilePathInfo = new DirectoryInfo(DirCsvFilePath);
                FileInfo[] FilesMLTSSCsv = DirCsvFilePathInfo.GetFiles(inputFileNameWithouttxt + "*" + NJHIDCsvFormatMLTSSst);
                foreach (FileInfo _filenameCSV1 in FilesMLTSSCsv)
                {
                    fullNamesCsvFilePath = _filenameCSV1.FullName;
                }
                string[] fileNameCsv = FilesMLTSSCsv.Select(f => f.Name).ToArray();


                foreach (string _filenameCSV in fileNameCsv)
                {
                    int first_pos = _filenameCSV.IndexOf("_");
                    string _filename = _filenameCSV.Substring(0, first_pos);
                    //check if the csv filenames (first few letters) and input txtfilename are same like 'HNJHID030916=HNJHID030916'.checking this  if there are multiple csvs created from running more than one input txt file in the datedfolder.
                    if (_filename == inputFileNameWithouttxt)
                    {
                        int indexpos = _filenameCSV.LastIndexOf("_");
                        if (indexpos >= 0)
                        {
                            tempCSVNAME = _filenameCSV.Substring(0, indexpos);
                            finalCSVfilename = _filenameCSV.Substring(0, indexpos) + ".csv";
                            zipname = finalCSVfilename.Replace("_ID_", "_PACKAGING_").Replace(".csv", ".zip");
                            zipfullnamepath = Path.Combine(scidirectory, zipname);
                            if (File.Exists(zipfullnamepath))
                            {
                                break;
                            }

                        }
                    }



                    string DirXmpiePdfFilePath = @ProcessVars.XmpiepdfPath;
                    string NJHIDPdfFormatMLTSSst = NJHIDCsvFormatMLTSSst.Replace("csv", "pdf");
                    DirectoryInfo DirPdfPathInfo = new DirectoryInfo(DirXmpiePdfFilePath);
                    FileInfo[] FilesMLTSSPdf = DirPdfPathInfo.GetFiles("*" + NJHIDPdfFormatMLTSSst);
                    if (FilesMLTSSPdf.Length == 0)
                    {
                        fullNamesPDFFilePath = "";
                    }
                    else
                    {
                        foreach (FileInfo _filenamePDF1 in FilesMLTSSPdf)
                        {
                            fullNamesPDFFilePath = _filenamePDF1.FullName;
                        }
                    }

                    string[] fileNamePdf = FilesMLTSSPdf.Select(f => f.Name).ToArray();

                    foreach (string _filenamepdf in fileNamePdf)
                    {
                        int indexpospdf = _filenamepdf.LastIndexOf("_");
                        if (indexpospdf >= 0)
                        {

                            finalPDFfilename = tempCSVNAME + ".pdf";


                        }
                    }

                    if (fullNamesPDFFilePath != "")

                    { createzipfile(fullNamesCsvFilePath, fullNamesPDFFilePath, finalCSVfilename, finalPDFfilename, zipname, zipfullnamepath); }
                }

            }

        }



        private void uploadNjhIdCardsToFtp_Reprocess()
        {
            String error = "Error uploading";
            dbU = ProcessVars.oDBUtility();
            string SciFolder = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + @"SCI_Reprocess\";
            DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SciFolder);
            string[] filefullPath = Directory.GetFiles(SciFolder, "*.ZIP");
            FileInfo[] fileDetail = DirSourceDirectoryInfo.GetFiles("*.ZIP");
            string[] fileNames = fileDetail.Select(f => f.Name).ToArray();
            int _Port = 22;
            string FileName = "";
            Sftp oSftp = new Sftp(ProcessVars.gHNJHIDCards_FTP_URL, ProcessVars.gHNJHIDCards_FTPUserNameProd, ProcessVars.gHNJHIDCards_FTPPwdProd);
            string HtmlBody = "";
            HtmlBody += "<table  id=\"SciUploadTable\" bgcolor='#ffffcc' style='width:100%;border:1px solid black ;border-collapse: collapse'  <tr><th align='center' valign='middle'>FileName </th><th align='center' valign='middle'>Import_Date </th><th align='center' valign='middle'>Status</th></tr>";
            string status = "";


            if (filefullPath != null && filefullPath.Length > 0)
            {

                try
                {
                    oSftp.Connect(_Port);
                    if (oSftp.Connected)
                    {
                        for (int i = 0; i < filefullPath.Length; i++)
                        {
                            string fullfilepath = filefullPath[i];
                            string filenameonly = fileNames[i];

                            try
                            {

                                oSftp.Put(filefullPath[i], ProcessVars.gHNJHIDCards_FTPLocationProd + filenameonly);
                                status = "Uploaded/ok";
                            }

                            catch (Exception ex)
                            {

                                SendMails sendmail = new SendMails();
                                sendmail.SendMail("Error uploading files to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com,cgaytan@apps.cierant.com", "noreply@apps.cierant.com", error);

                            }

                            HtmlBody += "<tr><td align='center' valign='middle' style='border:1px solid black'>" + filenameonly + "</td><td align='center' valign='middle' style='border:1px solid black'>" + DateTime.Now.ToString() + "</td><td align='center' valign='middle' style='border:1px solid black'> " + status + "</td></tr>";



                        }
                        HtmlBody += "</table>";

                    }

                }
                catch (Exception ex)
                {

                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("Error uploading files to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com", error);

                }
                finally
                {
                    oSftp.Close();

                    SendMails sendmail = new SendMails();
                    sendmail.SendMail("NJHID Cards Posted to CaptainCrunch on " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,tclinton@apps.cierant.com,rchico@apps.cierant.com,jcioban@cierant.com,cgaytan@apps.cierant.com,pgnecco@sciimage.com,edymek@sciimage.com,jnunez@sciimage.com,mscherman@sciimage.com,msundburg@sciimage.com", "noreply@apps.cierant.com", HtmlBody);
                    Results.Text = "";
                    Results.Text = "Posting Successful!";

                }


            }

            else
            {
                string Error = "Could not find zip files to upload";
                SendMails sendmail = new SendMails();
                sendmail.SendMail("CouldNot find njhId zip files to upload  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com", Error);

            }






        }

        private void button29_Click(object sender, EventArgs e)
            {
            ID_Cards("1882");        
            }

        private void button30_Click(object sender, EventArgs e)
            {
            for (int ntimes = 0; ntimes < 1000; ntimes++)
                {
                string hours = DateTime.Now.ToString("HH");
                int hoursInt = int.Parse(hours);

                DateTime nextRun1 = (hoursInt < 11) ? DateTime.Today.AddDays(+0).AddHours(10).AddMinutes(35) : 
                    DateTime.Today.AddDays(+1).AddHours(10).AddMinutes(35);
                //DateTime nextRun1 = DateTime.Today.AddDays(+1).AddHours(14).AddMinutes(15);
                TimeSpan diff = nextRun1.Subtract(DateTime.Now);
                int totalMinutes = (int)diff.TotalMinutes;
                label1.Text = " Waiting " + totalMinutes + " minutes  , HNJH cards next: " + nextRun1;
                label1.Update();
                var t1 = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * totalMinutes);
                    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                });
                t1.Wait();


                DayOfWeek day = DateTime.Now.DayOfWeek;
                if ((day >= DayOfWeek.Monday) && (day <= DayOfWeek.Friday))
                    {
                    button26_Click(sender, e);
                    hnjhdsnp_Click(sender, e);
                    }
                }

            System.Threading.Thread.Sleep(60 * 60 * 1000);
            }
       
        //private void button27_Click(object sender, EventArgs e)
        //{
        //    String error="Error uploading";
        //    dbU = ProcessVars.oDBUtility();
        //    string SciFolder = ProcessVars.NJHIDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + @"SCI\";
        //    DirectoryInfo DirSourceDirectoryInfo = new DirectoryInfo(SciFolder);
        //    string[] filefullPath = Directory.GetFiles(SciFolder, "*.ZIP");
        //    FileInfo[] fileDetail = DirSourceDirectoryInfo.GetFiles( "*.ZIP");
        //    string[] fileNames = fileDetail.Select(f => f.Name).ToArray();
        //    int _Port = 22;
        //    string FileName = "";
        //    Sftp oSftp = new Sftp(ProcessVars.gHNJHIDCards_FTP_URL, ProcessVars.gHNJHIDCards_FTPUserNameProd, ProcessVars.gHNJHIDCards_FTPPwdProd);
        //    string HtmlBody="";
        //    HtmlBody += "<table  id=\"SciUploadTable\" bgcolor='#ffffcc' style='width:100%;border:1px solid black ;border-collapse: collapse'  <tr><th align='center' valign='middle'>FileName </th><th align='center' valign='middle'>Import_Date </th><th align='center' valign='middle'>Status</th></tr>";
        //    string status="";
           
    
        //    if (filefullPath != null && filefullPath.Length > 0)
        //    {

        //        try
        //        {
        //            oSftp.Connect(_Port);
        //            if(oSftp.Connected)
        //            {
        //            for (int i = 0; i < filefullPath.Length; i++)
        //            {
        //                string fullfilepath = filefullPath[i];
        //                string filenameonly = fileNames[i];
                       
        //                try
        //               {
                       
        //                   oSftp.Put(filefullPath[i], ProcessVars.gHNJHIDCards_FTPLocationProd + filenameonly);
        //                   status="Uploaded/ok";
        //                }

        //                catch(Exception ex)
                      
        //                {
                            
        //                    SendMails sendmail = new SendMails();
        //                    sendmail.SendMail("Error uploading files to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com,cgaytan@apps.cierant.com", "noreply@apps.cierant.com",error);

        //                }

        //                HtmlBody += "<tr><td align='center' valign='middle' style='border:1px solid black'>" + filenameonly + "</td><td align='center' valign='middle' style='border:1px solid black'>" + DateTime.Now.ToString() + "</td><td align='center' valign='middle' style='border:1px solid black'> " + status + "</td></tr>";



        //            }
        //           HtmlBody+="</table>";
        
        //        }

        //        }
        //        catch (Exception ex)
        //        {

        //            SendMails sendmail = new SendMails();
        //            sendmail.SendMail("Error uploading files to CaptainCrunch on  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com", error);
        
        //        }
        //        finally
        //        {
        //            oSftp.Close();

        //            SendMails sendmail = new SendMails();
        //            sendmail.SendMail("NJHID Cards Posted to CaptainCrunch on " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com,jcioban@cierant.com,cgaytan@apps.cierant.com,pgnecco@sciimage.com,edymek@sciimage.com,jnunez@sciimage.com,mscherman@sciimage.com,msundburg@sciimage.com", "noreply@apps.cierant.com", HtmlBody);
        //            Results.Text = "";
        //            Results.Text = "Posting Successful!";
               
        //        }


        //    }

        //    else
        //    {
        //        string Error="Could not find zip files to upload";
        //        SendMails sendmail = new SendMails();
        //        sendmail.SendMail("CouldNot find njhId zip files to upload  " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,rchico@apps.cierant.com", "noreply@apps.cierant.com",Error );
      
        //    }

              
        //}




        
    }
}
