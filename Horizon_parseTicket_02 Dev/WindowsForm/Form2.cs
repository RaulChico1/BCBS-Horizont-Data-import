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
using System.Data.OleDb;
using System.Data.SqlClient;
using Microsoft.VisualBasic;
using System.Configuration;

namespace WindowsForm
{
    public partial class Form2 : Form
    {
        DBUtility dbU;
        DataTable dataNOPdfs = data_Table();

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
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\AbilTo");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\from_FTP");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\fromCass");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Decrypted");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\HNJH\Chams");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\HNJH\WK");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\HNJH\DSNP_WK");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards\ID_Cards_Omnia");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards\ID_Cards_Reg");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\ID_Cards\ID_Cards_Test");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Renewals");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\Errors");
            System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + @"\w_Process");
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            SqlParameter[] sqlParams;

            sqlParams = new SqlParameter[] { new SqlParameter("@Date", GlobalVar.DateofProcess.ToString("yyyy-MM-dd")),
                                             new SqlParameter("@type", "ALL")};

            DataTable processedData = dbU.ExecuteDataTable("HOR_rpt_CR_Radius_to_email", sqlParams);
            //Ticket02 zip 1st
            
            
            
            //DataTable resultsTicket02 = dbU.ExecuteDataTable("HOR_scr_DailyUpload_Cycle_02");
            dataGridView1.DataSource = processedData;
            DataGridViewColumn column = dataGridView1.Columns[0];
            column.Width = 160;
            DataGridViewColumn column2 = dataGridView1.Columns[3];
            column2.Width = 160;

            foreach (DataGridViewRow row in dataGridView1.Rows)
            {
                if ((int)row.Cells["Records in CSV"].Value != (int)row.Cells["PDFs"].Value)
                    row.DefaultCellStyle.BackColor = Color.Red;
            } 

            //List<string> listinDrive = new List<string>();
            //DirectoryInfo originaFiles = new DirectoryInfo(ProcessVars.InputDirectory + @"\From_FTP");
            //FileInfo[] filesZ = originaFiles.GetFiles("*.pdf");
            //if (filesZ.Count() > 0)
            //{
            //    foreach (FileInfo filename in filesZ)
            //    {
            //        if (filename.Name.IndexOf("__") != 0)
            //            listinDrive.Add(filename.Name);
            //    }
            //}
            //FileInfo[] filesx = originaFiles.GetFiles("*.xml");
            //if (filesx.Count() > 0)
            //{
            //    foreach (FileInfo filename in filesx)
            //    {
            //        if (filename.Name.IndexOf("__") != 0)
            //            listinDrive.Add(filename.Name);
            //    }
            //}
            


            //dataGridView2.DataSource = ConvertListToDataTable(listinDrive);
            
            
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
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");


            Export_XLSX export = new Export_XLSX();
            export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");

            DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim");
            //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_toFix");
            //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_only_HIX_Inv");
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
                string insertLog = "Insert into CIE_Interim_JobReceipt_log (" + colnames.Substring(1, colnames.Length - 1) + ") VALUES ('";
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
                            var resultSql2 = dbU.ExecuteScalar(insertLog + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
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
                dbU.ExecuteNonQuery("UPDATE CIE_Interim_JobReceipt_Manual SET Time_Received = convert(varchar(25), CycleDate, 120)  + ' 02:00:00.000' where convert(date,Time_Received) like '1900-01-01'");
                Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=receive&Date=" + GlobalVar.DateofProcess.ToString("yyyyMMdd"));
            }

            createEmail createemail = new createEmail();
            createemail.produceSummary_Uploaded_RegCRR();

            SendMails sendmail2 = new SendMails();
            sendmail2.SendMail("Ticket 02 closed " + DateTime.Now.ToString("yyyy-MM-dd"), "bdumont@apps.cierant.com, rchico@apps.cierant.com,cgaytan@apps.cierant.com" +
                ",kcarpenter@apps.cierant.com,stilford@apps.cierant.com,kmcnamara@apps.cierant.com,snelson@apps.cierant.com,dgannuscio@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                         "Please check files in ticket 02  " + Environment.NewLine + "          Directories: " + Environment.NewLine + "\\CARE RADIUS SENT" + Environment.NewLine + "   and       \\__Other files processed");

          
            Results.Text = "Ticket 02 Closed" + erros;
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
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
        private void button8_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Downloading Files for Ticket 02 ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            string extractPath = ProcessVars.InputDirectory + "From_FTP";
            string ResultsPdf = "";
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            NParse_pdfs parse_pdfs = new NParse_pdfs();
            downloadDta.MoveFilesFrom_VLTrader();
            string[] files = GetFiles(ProcessVars.InputDirectory + @"From_FTP", "CRC*.zip|CRN*.zip", SearchOption.TopDirectoryOnly);

            if (files.Length > 0)
            {
                downloadDta.expand_CRNJLTR_ZIP(GlobalVar.DateofFilesToProcess);
            }

            string errorsTck2a = ProcessTicket02();


            string[] files2 = System.IO.Directory.GetFiles(ProcessVars.InputDirectory + @"\From_FTP", "HLGS_*.zip");
            //string[] files3 = System.IO.Directory.GetFiles(GlobalVar.DateofFilesToProcess.ToString(), "COBA*", System.IO.SearchOption.TopDirectoryOnly);
            if (files2.Length > 0 )
            {
                ResultsPdf = ResultsPdf + " " + parse_pdfs.zipFilesinDirService("", extractPath);
            }
            // check unzip HLGS get extra character....
            
            //var dateProcess = DateTime.Now.DayOfWeek == DayOfWeek.Monday ? DateTime.Today.AddDays(-3) : DateTime.Today.AddDays(-1);

            string resultD = downloadDta.downloadData(GlobalVar.DateofFilesToProcess);
            resultD = resultD + downloadDta.downloadDataC(GlobalVar.DateofFilesToProcess);
            downloadDta.expand_CRNJLTR_ZIP(GlobalVar.DateofFilesToProcess);


            NparseFiles_with_xml parse_allfiles_OEInv = new NparseFiles_with_xml();
            parse_allfiles_OEInv.parse_all_OEINV(ProcessVars.InputDirectory);

            Nparse_SHBPMA parseSHBPMA = new Nparse_SHBPMA();
            //parseSHBPMA.Load_SHBPMA_txt();

            //parse_allfiles_OEInv.parse_all_SVNJCD(ProcessVars.InputDirectory);

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


            ResultsPdf = ResultsPdf + " " + parse_pdfs.zipFilesinDirService("", extractPath);

            //process_misc(GlobalVar.DateofProcess, ProcessVars.InputDirectory + "From_FTP\\Misc");

            string errorsTck2 = ProcessTicket02();

            if (resultD == "")
                Results.Text = resultD + "Process for Ticket 02 ready " + "\\n" + ResultsPdf + "\\n" + errorsTck2;
            else
                Results.Text = resultD + "\\n" + ResultsPdf + Environment.NewLine + errorsTck2;




            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
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
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing ABILTO ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            NParse_AbilTO abilto = new NParse_AbilTO();
            string DirLocal = @"\\freenas\Internal_Production\Horizon_Production_Mngmt\SECURE\PROD_INBOUND\" + DateTime.Now.ToString("yyyy-MM-dd") + @"\AbilTo\";
            //ProcessVars.InputDirectory + @"AbilTo";
            string result = abilto.importAbilTo(DirLocal);

            //HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
            //processXMPie.pritnSummary(reportdate, "HOR_rpt_HNJH_Involuntary__Summary_Date", xName);


            objPleaseWait.Close();
            Results.Text = "AbilTo Done: tables:  HOR_XMPIE_AbilTO_COM  HOR_XMPIE_AbilTO_FEP";
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
        }

        private void button19_Click(object sender, EventArgs e)
        {

            //Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=Print&Date=20150715");
            //Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=Insert&Date=20150715");
            //Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=Mail&Date=20150715");
            //N_loadFromFTP uploadZip = new N_loadFromFTP();

           // string ftplocation = "ftp://sftp.cierant.com//IN//";
           // string info_User = "Horizon";
           // string info_Pass = "CyRyk1al";
            
          
           //GlobalVar.dbaseName = "BCBS_Horizon";
           //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
           //string strsql = "select distinct zipname from HOR_parse_files_to_CASS where TableName = 'HOR_parse_Maintenance_ID_Cards' and convert(date,ImportDate) > '2018-01-01'";
           //DataTable filesToPurge = dbU.ExecuteDataTable(strsql);
           //foreach (DataRow row in filesToPurge.Rows)
           //{
           //    string zipName = row["zipname"].ToString();
           //    uploadZip.NotDownLoadFile_just_Move(ftplocation, zipName, info_User, info_Pass);
           //}

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
                await Task.Delay(1000 * 60 * 1);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t0.Wait();
          
                var directory = new DirectoryInfo(DirLocal);
                var masks = new[] { "*.zip" };
                var files = masks.SelectMany(directory.EnumerateFiles);
                string forMSG = "";

                foreach (var fileName in files)
                    {
                    if(fileName.ToString().Substring(0,2) != "__")
                        {
                        forMSG = forMSG + fileName.ToString() + "<br />";
                        }
                    }
            if(forMSG.Length > 2)
                {
                SendMails sendmail2 = new SendMails();
                sendmail2.SendMail("ID Cards process some files not processed " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,bdumont@apps.cierant.com" +  //
                    "",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             "Files not processed: <br /> <br />" + forMSG);
                }
            createEmail createemail = new createEmail();

            createemail.produceSummary_ID_NON_Maintenence(DirLocal);

            closeIDCards();

            create_rptGroupBundles(GlobalVar.DateofProcess.ToShortDateString());

            Results.Text = "ID Cards done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
            objPleaseWait.Close();
        }
        private void create_rptGroupBundles(string dateProcess)
        {
            int timeSummary = 0;
            string pNameTXTSum = ProcessVars.InputDirectory + @"ID_Cards\Summary_Bundle_" + DateTime.Now.AddDays(0).ToString("yyyy_MM_dd") + ".csv";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable groups = dbU.ExecuteDataTable("select distinct filename from HOR_parse_Maintenance_ID_Cards where  convert(date,importdate) = '" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "' and FileName like 'GRP%'");
            foreach (DataRow row in groups.Rows)
            {
                try
                {
                    string bundleDirNetwork = @"\\CIERANT-TAPER\Clients\Horizon BCBS\ID Cards\Bundle\";
                    System.IO.Directory.CreateDirectory(bundleDirNetwork + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd"));
                    
                    string fname = row[0].ToString();
                    SqlParameter[] sqlParams;
                    sqlParams = null;
                    sqlParams = new SqlParameter[] { new SqlParameter("@filename", fname) };
                    DataTable grpData = dbU.ExecuteDataTable("HOR_rpt_idCards_BundleGroups", sqlParams);
                    if (grpData.Rows.Count > 0)
                    {
                        FileInfo fileInfo = new System.IO.FileInfo(fname);
                        string pNameTXT2 = ProcessVars.InputDirectory + @"ID_Cards\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv";
                        if (File.Exists(pNameTXT2))
                            File.Delete(pNameTXT2);
                        createCSV createcsvS = new createCSV();
                        createcsvS.printCSV_fullProcess(pNameTXT2, grpData, "", "");
                        string bundleDir = bundleDirNetwork + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
                        if (File.Exists(bundleDir + "\\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv"))
                            File.Delete(bundleDir + "\\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv");
                        File.Copy(pNameTXT2, bundleDir + "\\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv");
                        if (timeSummary == 0)
                        {
                            createcsvS.printCSV_fullProcess(pNameTXTSum, grpData, "", "");
                            timeSummary++;
                        }
                            
                        else
                        {
                            createcsvS.printCSV_fullProcessNoHeader(pNameTXTSum, grpData, "", "");
                            timeSummary++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    var errFix = ex.Message;
                }
            }
            //SqlParameter[] sqlParams2;
            //sqlParams2 = null;
            //sqlParams2 = new SqlParameter[] { new SqlParameter("@Date", DateTime.Now.AddDays(0).ToString("yyyyMMdd")) };
            //DataTable summaryData = dbU.ExecuteDataTable("HOR_rpt_idCards_BundleGroupsSummary", sqlParams2);
            //if (summaryData.Rows.Count > 0)
            //{
            //    string pNameTXT2 = ProcessVars.InputDirectory + @"ID_Cards\Summary_Bundle_" + DateTime.Now.AddDays(0).ToString("yyyy_MM_dd") + ".csv";
            //    if (File.Exists(pNameTXT2))
            //        File.Delete(pNameTXT2);
            //    createCSV createcsvS = new createCSV();
            //    createcsvS.printCSV_fullProcess(pNameTXT2, summaryData, "", "");
                string bundleDirS = @"\\CIERANT-TAPER\Clients\Horizon BCBS\ID Cards\Bundle\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
                if (File.Exists(bundleDirS + "\\" + @"Summary_Bundle_" + DateTime.Now.AddDays(0).ToString("yyyy_MM_dd") + ".csv"))
                    File.Delete(bundleDirS + "\\" + @"Summary_Bundle_" + DateTime.Now.AddDays(0).ToString("yyyy_MM_dd") + ".csv");
                if (File.Exists(pNameTXTSum))
                File.Copy(pNameTXTSum, bundleDirS + "\\" + @"Summary_Bundle_" + DateTime.Now.AddDays(0).ToString("yyyy_MM_dd") + ".csv");
            //}

        }
        private void closeIDCards()
        {

            string erros = "";
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");


            //Export_XLSX export = new Export_XLSX();
            //export.CreateExcelFile(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");

            DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_ID_Cards");
            //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_only_HIX_Inv");
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
            //Ticket createTicket = new Ticket();
            //createTicket.createTicket(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");
            //Results.Text = "Ticket 02 Closed" + erros;
           
            
            //DataTable summaryIDCards =  dbU.ExecuteDataTable("select RecordsNum, DateProcess, FileName, ImportDate from HOR_parse_files_to_CASS where TableName = 'HOR_parse_Maintenance_ID_Cards' and convert(date,importdate) > '2016-10-30' order by convert(date,importdate)");
            //Export_XLSX exportslx = new Export_XLSX();
            //string tabname = "Summary_" + DateTime.Now.ToString("yyyy_MM_dd");
            //exportslx.CreateExcelFileOneTables(summaryIDCards, tabname, ProcessVars.InputDirectory + @"ID_Cards\" + "ID_Cards_" + tabname + ".xlsx");
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
        public void process_misc(DateTime dateProcess, string direcTory)
        {
            if (Directory.Exists(direcTory))
            {
                string[] subdirectoryEntries = Directory.GetDirectories(direcTory);
                // Loop through them to see if they have any other subdirectories
                foreach (string subdirectory in subdirectoryEntries)
                    process_subdir_misc(dateProcess, subdirectory);

            }

        }
        public void process_subdir_misc(DateTime dateProcess, string subdir)
        {
            DirectoryInfo originalZIPs = new DirectoryInfo(subdir);
            string unzipDirName = "";
            foreach (FileInfo f in originalZIPs.GetFiles("*.txt"))
            {
                if (f.Name.IndexOf("_") == 0)
                { //processed already
                }
                else
                {
                    string extractPath = "";
                    string zipName = "";
                    string Code = "";
                    StreamReader reader = File.OpenText(f.FullName);
                    string s = reader.ReadLine();
                    string[] words = s.Split('~');
                    if (words[0].ToString().Length > 0)
                        extractPath = words[0].ToString();
                    if (words[1].ToString().Length > 0)
                        zipName = words[1].ToString();
                    if (words[2].ToString().Length > 0)
                        Code = words[2].ToString();
                    NParse_pdfs parse_pdfs = new NParse_pdfs();
                    string ResultsPdf = parse_pdfs.zipFilesinDirMISC(zipName, extractPath, f.Directory.ToString(), f.Name.ToString(), Code);
                }

            }





        }
        public string ProcessTicket02()
        {
            //Cursor.Current = Cursors.WaitCursor;
            //Results.Text = "Processing Files for Ticket 02 ...";
            //PleaseWait objPleaseWait = new PleaseWait();
            //objPleaseWait.Show();
            string results = "";

            appSets appsets = new appSets();
            appsets.setVars();

            NparseFiles_with_xml parse_allfiles_mapdp = new NparseFiles_with_xml();
            parse_allfiles_mapdp.parse_all_MAPDP(ProcessVars.InputDirectory);

            NParse_to_Merge_XMPie parse = new NParse_to_Merge_XMPie();
             parse.Load_SMB_txt();

            NParse_pdfs parse_pdfs = new NParse_pdfs();
            string ResultsPdf = parse_pdfs.zipFilesinDir_Cr2(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");
            results = (ResultsPdf.Length == 0) ? results : results + ResultsPdf + Environment.NewLine;
            //string time1 = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //var t = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 2);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //t.Wait();

            //string time2 = t.Result;

            //System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");
            //BackCASS processRedturns = new BackCASS();
            //string ResultsBack_CASS = processRedturns.ProcessFiles("CareRadius_2");
            label4.Text = label4.Text + Environment.NewLine + "CR2";


            Parse_Inv_pdf parse_inv_pdf = new Parse_Inv_pdf();
            string ResultsPdf2 = parse_inv_pdf.zipFilesinDir_INV(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");
            results = (ResultsPdf2.Length == 0) ? results : results + ResultsPdf2 + Environment.NewLine;
            label4.Text = label4.Text + Environment.NewLine + "INV";


            NParse_pdfs parse_pdfsS = new NParse_pdfs();
            string ResultsPdfS = parse_pdfsS.zipFilesinDir_SBC(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");
            results = (ResultsPdfS.Length == 0) ? results : results + ResultsPdfS + Environment.NewLine;

         
            
            Parse_Inv_pdf parse_maeobs = new Parse_Inv_pdf();
            string ResultsMAEOBS = parse_maeobs.zipFilesinDir_MAEOB(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");

            //string time1S = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //var tS = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 2);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //tS.Wait();


            //BackCASS processRedturnsS = new BackCASS();
            //string ResultsBack_CASS_S = processRedturnsS.ProcessFiles("SBC");
            label4.Text = label4.Text + Environment.NewLine + "SBC";

            NParse_pdfs parse_pdfsM = new NParse_pdfs();
            string ResultsPdfM = parse_pdfsM.zipFilesinDir_MBA(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");
            results = (ResultsPdfM.Length == 0) ? results : results + ResultsPdfM + Environment.NewLine;
            string time1M = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //var tM = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 2);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //tM.Wait();


            //BackCASS processRedturnsM = new BackCASS();
            //string ResultsBack_CASSM = processRedturnsM.ProcessFiles("MBA_SMN");


            NParse_pdfs parse_pdfsNoDate = new NParse_pdfs();
            string ResultsPdND = parse_pdfsNoDate.zipFilesinDir_NoDate(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");
            results = (ResultsPdND.Length == 0) ? results : results + ResultsPdND + Environment.NewLine;
            //string time1ND = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //var tND = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 2);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //tND.Wait();


            //BackCASS processRedturnsND = new BackCASS();
            //string ResultsBack_CASSND = processRedturnsND.ProcessFiles("MBA_SMN");
            //results = (ResultsBack_CASSND.Length == 0) ? "" : results + ResultsBack_CASSND + "\\n\\n";
            label4.Text = label4.Text + Environment.NewLine + "MBA, SMN PNO SVN " + Environment.NewLine + "Done at " + DateTime.Now.ToString("yyyy_MM_dd   HH:mm");


            Cursor.Current = Cursors.Default;
            if (results.Length == 0)
                results = Environment.NewLine + " no Errors parsing PDFs";
            else
                results = Environment.NewLine + results;
            return results;
        }

        private void button24_Click(object sender, EventArgs e)
        {
            
        }


        private void button26_Click(object sender, EventArgs e)
        {
            String PrintNumber = "01";
            appSets appsets = new appSets();
            appsets.setVars();
            dataNOPdfs.Clear();
            string reportdate = DateTime.Now.ToString("yyyy-MM-dd");  //"2017-03-24";

            BackCASS processRedturns = new BackCASS();
            HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //string strsql = "select filenamecass from HOR_parse_files_to_CASS where TableName = 'HOR_parse_HNJH_Panel_Roster_Provider' ";
            string strsql = "select filenamecass, Processed,XmpieReady, filename,convert(date,ImportDate) as ImportDate  from HOR_parse_files_to_CASS where TableName =  " +
                            "'HOR_parse_HNJH_Panel_Roster_Provider' and (Processed is null or XmpieReady is null)";
            DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            foreach (DataRow row in table_BCCToProcess.Rows)
            {
                string Ftxtname = row[0].ToString().Replace("_toBCC.csv", "");
                string OriginalFilename = row[3].ToString();
                DateTime dateProcess = Convert.ToDateTime(row[4].ToString());
                if (DBNull.Value.Equals(row[1]))  // processed is null the update back from BCC
                {
                    result = processRedturns.FilestoProcessHNJH(row[0].ToString());
                    processXMPie.SplitXMPiePdf(OriginalFilename, PrintNumber);
                }

                //was  PagNoXmpie   
                strsql = "select distinct PagNoXmpie from HOR_parse_HNJH_Panel_Roster_Provider where filename = '" +
                            OriginalFilename + "' and flag_Xmpie = '1' and Xmpie_File <>''";   //PagNoXmpie";  // the original file name does not have  HNJH-PR_
                //Process each pdf name
                DataTable Xmpie_Files = dbU.ExecuteDataTable(strsql);
                foreach (DataRow rowX in Xmpie_Files.Rows)
                {
                    string resultXMPie = processXMPie.SentTo_XMpieTables(rowX[0].ToString(), OriginalFilename);

                    if (resultXMPie == "")
                    {
                        //XMpie_Roster printXMpie = new XMpie_Roster();
                        //string JobIDs = printXMpie.P_Roster(ProcessVars.DocumentID_ToProcess);
                        //printXMpie.CheckJobStatus(JobIDs);

                        //var t = Task.Run(async delegate
                        //{
                        //    await Task.Delay(1000 * 60 * 1);
                        //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        //});
                        //t.Wait();



                        processXMPie.SCI_file_after_XMpiePdf(rowX[0].ToString(), OriginalFilename);
                        dbU.ExecuteNonQuery("update HOR_parse_HNJH_Panel_Roster_Provider set flag_Xmpie = '2' " +
                                    "where PagNoXmpie = '" + rowX[0].ToString() + "' and filename = '" + row[3].ToString() + "'");
                    }
                }

                //summary
                reportdate = dateProcess.ToString("yyyy-MM-dd");
                string xName = ProcessVars.SourceDataRosterDir + reportdate + "\\Panel_Roster\\summary.xls";
                processXMPie.pritnSummary(reportdate, "HOR_rpt_HNJH_Roster__Summary_Date", xName);

                dbU.ExecuteNonQuery("update HOR_parse_files_to_CASS set XmpieReady = 'Y' " +
                                    "where TableName =  'HOR_parse_HNJH_Panel_Roster_Provider' and filename = '" + row[3].ToString() + "'");
                // Non Delivery to SCI
                //processXMPie.printNDtoSCI(row[0].ToString());


            }
            string dirPath = ProcessVars.XMPIDataRoster;
            string[] subdirectoryEntries = Directory.GetDirectories(dirPath);
            foreach (string subdirectory in subdirectoryEntries)
            {
                string NewsubdirName = subdirectory.Remove(0, subdirectory.LastIndexOf('\\') + 1);
                if (NewsubdirName.ToUpper().IndexOf("BATCH") == 0)
                {

                    LoadSubDirs(subdirectory, DateTime.Now.ToString("MMyyyy").ToString());
                }
            }
            if(dataNOPdfs.Rows.Count > 0)
            {

                createCSV createfile = new createCSV();

                HNJH_To_XMPie processXMPie_Extras = new HNJH_To_XMPie();

                foreach (DataRow dr in dataNOPdfs.Rows)
                {
                    
                    try
                    {
                        //string ffname = ProcessVars.SourceDataRosterDir + reportdate + "\\Panel_Roster\\Missing_Provider_" + dr["Recnum"].ToString() + ".csv";
                        string pFname = ProcessVars.SourceDataRosterDir + reportdate + "\\Panel_Roster\\";
                        //DataTable summary = dataNOPdfs.Clone();
                        //foreach (DataRow dr1 in dataNOPdfs.Rows)
                        //{
                        //    if (dr1["Recnum"].ToString() == dr["Recnum"].ToString())
                        //    {
                        //            summary.ImportRow(dr1);
                        //    }
                        //}
                        //createfile.printCSV_fullProcess(ffname, summary, "", "");
                        int recnumReprint = Int32.Parse(dr["Recnum"].ToString());

                        string resultXMPie = processXMPie_Extras.SentTo_XMpieTablesRecnum(recnumReprint, pFname);
                    }
                    catch (Exception ex)
                    {
                        var msg = ex.Message;
                    }
                }
            }

        }
        private void LoadSubDirs(string dir, string Myear_Report)
        {

            string[] allFiles = Directory.GetFileSystemEntries(dir);
            int fcount = Directory.GetFiles(dir).Length;
            if (fcount < 500)
            {
                string batchNum = dir.Substring(dir.Length - 1);
                string strsql = "select recnum, Xmpie_File + '.pdf' from HOR_parse_HNJH_Panel_Roster_Provider where RIGHT('0' + RTRIM(MONTH(importdate)), 2) + RIGHT('0' + RTRIM(YEAR(importdate)), 4) = '" + Myear_Report + "' and PagNoXmpie = '" + batchNum + "'";
                DataTable filestoInspect = dbU.ExecuteDataTable(strsql);
                if (filestoInspect.Rows.Count > 0)
                {
                    //dataNOPdfs.Clear();
                    foreach (DataRow dr in filestoInspect.Rows)
                    {
                        if (!File.Exists(dir + "\\" + dr[1].ToString()))
                        {
                            var row = dataNOPdfs.NewRow();
                            row["Batch"] = batchNum;
                            row["Recnum"] = dr[0].ToString();
                            row["XMPieName"] = dr[1].ToString();
                            dataNOPdfs.Rows.Add(row);
                        }
                    }
                }
            }
        }

        private void button27_Click(object sender, EventArgs e)
        {
            string location = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2016-04-01\from_FTP\Test";

            HNJH_pdf_Counts CountsPdf = new HNJH_pdf_Counts();

            DirectoryInfo xmpiePDFs = new DirectoryInfo(location);

            FileInfo[] files = xmpiePDFs.GetFiles("*.pdf");

            string errors = "";
            foreach (FileInfo file in files)
            {
                string result = CountsPdf.Read_RosterPDF(file.FullName);

            }
        }

        private void button28_Click(object sender, EventArgs e)
        {
            //HOR_Parse_HNJH_WK_Master_Translation
            //HOR_parse_HNJH_WK
            
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Checking files Welcome Kits  ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            //string location = @"\\CIERANT-TAPER\Clients\Horizon BCBS\16-0502_Horizon NJH Production Mngmt\Welcome Kits\SECURE DATA";
            appSets appsets = new appSets();
            appsets.setVars();

            //string location = ProcessVars.InputDirectory + @"HNJH\WK";
            string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\WK";
            string locationLocal = ProcessVars.InputDirectory + @"HNJH\WK\";
            HNJH_WK_Medicaid procesWKits = new HNJH_WK_Medicaid();

            DirectoryInfo txts = new DirectoryInfo(location);

            FileInfo[] files = txts.GetFiles("NJOUTBOUND*.txt");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    DataTable filesProcessed = dbU.ExecuteDataTable("select filename from HOR_parse_files_to_CASS where filename = '" + file.Name + "'");
                    if (filesProcessed.Rows.Count == 0)
                        errors = procesWKits.Process_WK(file.FullName, locationLocal);
                }
            }
            Results.Text = "Welcome Kits ready ...";
            objPleaseWait.Close();
        }

        private void button29_Click(object sender, EventArgs e)
        {
            //string location = @"\\CIERANT-TAPER\Clients\Horizon BCBS\16-0502_Horizon NJH Production Mngmt\CHAMPS";
            //string location = ProcessVars.InputDirectory + @"HNJH\Chams";
            string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\CHAMPS";

            HNJH_Champs procesChamps = new HNJH_Champs();

            DirectoryInfo xlss = new DirectoryInfo(location);

            FileInfo[] files = xlss.GetFiles("*.xlsx");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1 && file.Name.IndexOf("._") == -1)
                {
                    errors = procesChamps.Process_Champs(file.FullName);
                }
            }
        }

        private void button30_Click(object sender, EventArgs e)
        {
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable resultsTicket01 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_11am");


            Ticket createTicket = new Ticket();
            createTicket.createTicket(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");
            Results.Text = "Ticket 02 Ready";
        }

        private void button31_Click(object sender, EventArgs e)
        {
            string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(-0).ToString("yyyy-MM-dd") + @"\WK";
            string locationLocal = ProcessVars.InputDirectory + @"HNJH\WK\";

            HNJH_WK_Medicaid HnjH_wk = new HNJH_WK_Medicaid();
            //string result = HnjH_wk.Print_HNJH_WK(DateTime.Now.ToString("yyyy_MM_dd"));
            string result = HnjH_wk.Print_HNJH_WK(location, locationLocal, GlobalVar.DateofProcess.AddDays(-0).ToString("yyyy-MM-dd").ToString());

            //XMpie_Roster printXMpie = new XMpie_Roster();
            //printXMpie.welcomeKits(ProcessVars.wk_DocumentID);
            //ProcessVars.wk_RecipientTablet_4200 = "HNJH_WKits_NJH_XMpie_2";
            //printXMpie.welcomeKits(ProcessVars.wk_DocumentID);
            //ProcessVars.wk_RecipientTablet_4200 = "HNJH_WKits_NJH_XMpie_3";
            //printXMpie.welcomeKits(ProcessVars.wk_DocumentID);

            Results.Text = "Output  WK_Medicaid Ready";

            SendMails sendmail2 = new SendMails();
            sendmail2.SendMail("HNJH Welcome Kits " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,cgaytan@apps.cierant.com" +
                ",kcarpenter@apps.cierant.com,stilford@apps.cierant.com,kmcnamara@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                         "Please check, files Ready");
        }

        private void button32_Click(object sender, EventArgs e)
        {

        }

        private void button33_Click(object sender, EventArgs e)
        {
            closeFixingIDCards();
            //HOR_upd_DailyUpload_toInterim_ID_Cards_Fix_Missing
        }
        private void closeFixingIDCards()
        {

            string erros = "";
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

           


            string strsql = "SELECT  dateprocess FROM [BCBS_Horizon].[dbo].[CIE_Interim_Dates] order by dateprocess desc";
            //string strsql = "SELECT  distinct cycledate  FROM [BCBS_Horizon].[dbo].[CIE_Interim_JobReceipt_Manual] where job_class='misc'  and ( ReceiveXMLFile is null or PrintXMLFile is null or InsertXMLFile is null or MailXMLFile is null) " +
            //                "and cycledate < '2016-05-09'  order by 1 desc";
            DateTime dt2 = Convert.ToDateTime("2016-03-03");
           

            DataTable datestoRun = dbU.ExecuteDataTable(strsql);
            int x = 0;
            foreach (DataRow rowD in datestoRun.Rows)
            {

                try
                {
                    SqlParameter[] sqlParams;
                    sqlParams = null;
                    sqlParams = new SqlParameter[] { new SqlParameter("@Pdate", rowD["dateprocess"].ToString()) };
                  
                    DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim__date", sqlParams);
                    //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_toFix");
                    //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_only_HIX_Inv");
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
                        string insertLog = "Insert into CIE_Interim_JobReceipt_log (" + colnames.Substring(1, colnames.Length - 1) + ") VALUES ('";
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
                                    var resultSql2 = dbU.ExecuteScalar(insertLog + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
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
                        //Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=receive&Date=" + GlobalVar.DateofProcess.ToString("yyyyMMdd"));
                        Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=receive&Date=" + rowD["dateprocess"].ToString().Replace("-", ""));
                    }

                }
                catch (Exception ex)
                {

                    var here = "";
                }
            }
        }
        private void button34_Click(object sender, EventArgs e)
        {
            string strsql = "SELECT  dateprocess FROM [BCBS_Horizon].[dbo].[CIE_Interim_Dates] order by dateprocess desc";
            //string strsql = "SELECT  distinct cycledate  FROM [BCBS_Horizon].[dbo].[CIE_Interim_JobReceipt_Manual] where job_class='misc'  and ( ReceiveXMLFile is null or PrintXMLFile is null or InsertXMLFile is null or MailXMLFile is null) " +
            //                "and cycledate < '2016-05-09'  order by 1 desc";
            DateTime dt2 = Convert.ToDateTime("2016-03-03");
            string erros = "";
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable datestoRun = dbU.ExecuteDataTable(strsql);
            int x = 0;
            foreach (DataRow row in datestoRun.Rows)
            {

                try
                {
                    DateTime cycleDate = DateTime.Parse(row[0].ToString());
                    string cDate = cycleDate.Year + cycleDate.Month.ToString("00") + cycleDate.Day.ToString("00");

                    //if (cycleDate.Date > dt2)
                    //{
                    Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=Receive&Date=" + cDate);
                    string time1R = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    var tR = Task.Run(async delegate
                    {
                        await Task.Delay(1000 * 60 * 1);
                    });
                    tR.Wait();
                    Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=Print&Date=" + cDate);
                    string time2R = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    var tR2 = Task.Run(async delegate
                    {
                        await Task.Delay(1000 * 60 * 1);
                        return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    });
                    tR2.Wait();
                    Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=Insert&Date=" + cDate);
                    string time3R = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    var tR3 = Task.Run(async delegate
                    {
                        await Task.Delay(1000 * 60 * 1);
                        return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    });
                    tR3.Wait();
                    Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=Mail&Date=" + cDate);
                    string time4R = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    var tR4 = Task.Run(async delegate
                    {
                        await Task.Delay(1000 * 60 * 1);
                        return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                    });
                    tR4.Wait();
                }
                //string time2R = tR.Result;
                //Process myProcess = Process.GetProcessByName("updates");

                    //myProcess.Kill();
                //}

                catch (Exception ex)
                {
                    var errFix = ex.Message;
                }
            }
            //    if (x == 10)
            //    {
            //        x = 0;
            //        string time1R = DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //        var tR = Task.Run(async delegate
            //        {
            //            await Task.Delay(1000 * 60 * 5);
            //            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //        });
            //        tR.Wait();

            //        string time2R = tR.Result;
            //        ClientScript.RegisterStartupScript(typeof(Page), "closePage", "window.open('close.html', '_self', null);", true);

            //    }
            //}
        }

        private void button35_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing Involuntary Disenrollment...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            NParse_Involuntary involuntary = new NParse_Involuntary();
            string DirLocal = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + DateTime.Now.ToString("yyyy-MM-dd") + @"\Involuntary_Disenrollment\";
            //ProcessVars.InputDirectory + @"AbilTo";
            string result = involuntary.importInvoluntary(DirLocal);
            Results.Text = "Involuntary ready ...";
            objPleaseWait.Close();

        }

        private void button36_Click(object sender, EventArgs e)
        {
            NparseHNJH_Panel processFiles = new NparseHNJH_Panel();

            string DirLocal = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\Panel_Roster";
            appSets appsets = new appSets();
            appsets.setVars();

            //string location = ProcessVars.InputDirectory + @"HNJH\Panel_Roster";
            //Directory.CreateDirectory(location);
            string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), ProcessVars.SourceDataRoster, "2");
            //string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), ProcessVars.SourceDataRoster);
            Results.Text = result + " Roster Process ready ...";
           
        }

        private void button37_Click(object sender, EventArgs e)
        {


            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Checking files Dental  ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            //string location = @"\\CIERANT-TAPER\Clients\Horizon BCBS\16-0502_Horizon NJH Production Mngmt\Welcome Kits\SECURE DATA";
            appSets appsets = new appSets();
            appsets.setVars();

            //string location = ProcessVars.InputDirectory + @"HNJH\WK";
            string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\Dental_ER";
            string locationLocal = ProcessVars.InputDirectory + @"HNJH\Dental_ER\";
            string tablename = "HOR_parse_HNJH_Dental";
            Directory.CreateDirectory(locationLocal);

            Import_Generic processFiles = new Import_Generic();

            DirectoryInfo Selectedfiles = new DirectoryInfo(location);
            string zipName = "";
            FileInfo[] filesZ = Selectedfiles.GetFiles("*.zip");


            foreach (FileInfo filez in filesZ)
            {
                zipName = filez.Name.Replace(".zip", "");
            }
            FileInfo[] files = Selectedfiles.GetFiles("*.xls*");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1 && file.Name.IndexOf("._") == -1)
                {
                    DataTable filesProcessed = dbU.ExecuteDataTable("select filename from HOR_parse_files_to_CASS where filename = '" + file.Name + "'");
                    if (filesProcessed.Rows.Count == 0)
                        errors = processFiles.ProcessThisFileXLS(file.FullName, locationLocal, GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd"), tablename, zipName);
                }
            }

            dbU.ExecuteNonQuery("delete from HOR_XMPIE_HNJH_Dental");
            SqlParameter[] sqlParams;
            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@Pdate", GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd")) };

            dbU.ExecuteNonQuery("HOR_upd_HNJH_Dental_Xmpie", sqlParams);
            Results.Text = "Dental ready ...";
            objPleaseWait.Close();

        }
        private void button25_Click(object sender, EventArgs e)
        {
            NparseHNJH_Panel processFiles = new NparseHNJH_Panel();

            string DirLocal = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\Panel_Roster";



            appSets appsets = new appSets();
            appsets.setVars();

            //string location = ProcessVars.InputDirectory + @"HNJH\Panel_Roster";
            //Directory.CreateDirectory(location);
            string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), ProcessVars.SourceDataRoster, "1");
            //string result = processFiles.ProcessFilesinDir(GlobalVar.DateofProcess.ToShortDateString(), ProcessVars.SourceDataRoster);
            Results.Text = result;
        }

        private void button25_MouseEnter(object sender, EventArgs e)
        {
            string DirLocal = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\Panel_Roster";


            label8.Text = DirLocal + Environment.NewLine;
            DirectoryInfo originalDATs = new DirectoryInfo(DirLocal);
            try
            {
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.txt");

                if (FilesDAT.Count() > 0)
                {
                    foreach (FileInfo file in FilesDAT)
                    {

                        if (file.Name.IndexOf("_") != 0)
                        {
                            label8.Text = label8.Text + file.Name + Environment.NewLine;
                        }

                    }
                    label8.BackColor = Color.Green;
                }
                else
                {
                    label8.Text = label8.Text + " NO FILES >>>>>>";
                    label8.BackColor = Color.Red;
                }
            }
            catch (Exception ex)
            {
                label8.Text = label8.Text + " NO FILES >>>>>>";
                label8.BackColor = Color.Red;
            }
        }

        private void button32_Click_1(object sender, EventArgs e)
        {
            NParse_Pdfs_DueDilligence parsePDFs_D = new NParse_Pdfs_DueDilligence();
            parsePDFs_D.ProcessFiles(DateTime.Now.ToShortDateString());
        }

        private void button38_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();


            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.InputDirectory + "\\from_FTP\\");
            // DirectoryInfo processedFiles = new DirectoryInfo(@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2016-08-16\from_FTP\");
            FileInfo[] FilesReady = processedFiles.GetFiles("CRNJLTR*.csv");
            foreach (FileInfo file in FilesReady)
            {
                if (File.Exists(file.FullName.Replace(".csv", ".zip")))
                {
                    xml_pdfs compareFiles = new xml_pdfs();
                    compareFiles.compare_csv_pdfs_inZip(file.FullName, file.FullName.Replace(".csv", ".zip"), 2);
                }
            }
        }

        private void button39_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();


            //DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.InputDirectory + "\\from_FTP\\");
            DirectoryInfo processedFiles = new DirectoryInfo(@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Mapart\");
            FileInfo[] FilesReady = processedFiles.GetFiles("*MAPA*.ZIP");
            foreach (FileInfo file in FilesReady)
            {

                if (file.Name.IndexOf("__") == -1)
                {
                    NParse_MAPAB process_MAPAB = new NParse_MAPAB();
                    process_MAPAB.process_filesin_Zip(file.FullName, GlobalVar.DateofFilesToProcess);
                }
            }
        }

        private void button40_Click(object sender, EventArgs e)
        {
            string dateProcess = GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd");   // "2016-08-18";//Microsoft.VisualBasic.Interaction.InputBox("Date reprint Champs  yyy-mm-dd", "hello", "nothing", 10, 10);
            string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\CHAMPS";

            appSets appsets = new appSets();
            appsets.setVars();

            BackCASS processRedturns = new BackCASS();
            HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //string strsql = "select filenamecass from HOR_parse_files_to_CASS where TableName = 'HOR_parse_HNJH_Panel_Roster_Provider' ";
            string strsql = "select filenamecass, Processed from HOR_parse_files_to_CASS where TableName =  " +
                            "'HOR_parse_HNJH_Champion' and convert(date,importdate) = '" + dateProcess + "'";
            DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            foreach (DataRow row in table_BCCToProcess.Rows)
            {

                string Ftxtname = row[0].ToString().Replace("_toBCC.csv", "");

                HNJH_Champs get_Results = new HNJH_Champs();
                result = get_Results.HNJH_ChampionPrint(row[0].ToString(), location);

            }
        }

        private void button41_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Downloading Files MRDF ...";
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
                        var errorsHre = "";
                    }
                }
            }


            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
            Results.Text = "Files MRDF DONE ...";
        }

        private void button42_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "JUST ASSIGN RECNUMs and give to STEVE Direct Mail...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            //NParse_DirectMail importdata = new NParse_DirectMail();
            //string DirNetwork = @"\\CIERANT-TAPER\Clients\Horizon BCBS\16-0545_Direct_Mailing_Program\SECURE\PROD_INBOUND\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + @"\";

            //string DirLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\DirectMail\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + @"\";
            
            ////ProcessVars.InputDirectory + @"AbilTo";
            //string result = importdata.importDirectMail(DirLocal);
            //Results.Text = "Direct Mail ready ...";
            objPleaseWait.Close();
        }

        private void button43_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Downloading Files IMB ...";
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
                        string results1 = updateFile.update_IMB_back(DateTime.Now.ToString("yyyy-MM-dd"));
                        string results = updateFile.Update_IdCards(DateTime.Now.ToString("yyyy-MM-dd"));
                        string results2 = updateFile.Update_DirectMail(DateTime.Now.ToString("yyyy-MM-dd"));
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

            Results.Text = Results.Text.ToString()  + Environment.NewLine + "Files IMB Done...";
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
        }

        private void button44_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing HOR_FEP-UPS ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            string DirLocal = @"\\CIERANT-TAPER\Clients\Horizon BCBS\16-0551_HOR_FEP-UPS_Setup\Source Documents\blueCrossFilesReceived20160304.d\";
            DirectoryInfo Selectedfiles = new DirectoryInfo(DirLocal);
            string zipName = "";
            FileInfo[] filesZ = Selectedfiles.GetFiles("*.sys00028");


            foreach (FileInfo filez in filesZ)
            {
                
                NParse_HOR_FEP_UPS importdata = new NParse_HOR_FEP_UPS();
                string result = importdata.processData(filez.FullName);
            }


            Results.Text = "HOR_FEP-UPS ready ...";
            objPleaseWait.Close();
        }

        private void button45_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing MAPARTB ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            // check unzip HLGS get extra character....
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            

            //string resultD = downloadDta.downloadData(GlobalVar.DateofFilesToProcess);
            downloadDta.expand_MAPARTB_ZIP(GlobalVar.DateofFilesToProcess);
            Results.Text = "MAPARTB ready ...";
            objPleaseWait.Close();

        }

        private void button46_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "PRINT CSV Direct Mail...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            NParse_DirectMail importdata = new NParse_DirectMail();
            //string DirLocal = @"\\CIERANT-TAPER\Clients\Horizon BCBS\16-0545_Direct_Mailing_Program\SECURE\PROD_INBOUND\" + DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd") + @"\";

            string DirLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\DirectMail\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + @"\";
            
            //ProcessVars.InputDirectory + @"AbilTo";
            importdata.PrintCSV_only(DirLocal, DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd"));
            Results.Text = "CSV   Direct Mail ready ...";
            objPleaseWait.Close();
        }

        private void button48_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing OutReach...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            Nparse_OutReach importdata = new Nparse_OutReach();

            appSets appsets = new appSets();
            appsets.setVars();

            string DirLocal = ProcessVars.InputDirectory + @"OUTREACH\";
            string dirNetwork = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd") + @"\OUTREACH\";
            string result = importdata.importOutReach(DirLocal);


            Results.Text = "OutReach Mail ready ...";
            objPleaseWait.Close();
        }

        private void button49_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing HCVR ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            
            //string resultD = downloadDta.downloadData(GlobalVar.DateofFilesToProcess);
            string DirLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + @"\HCVR\";
            downloadDta.expand_HCVR_ZIP(GlobalVar.DateofFilesToProcess, DirLocal);
            Results.Text = "HCVR ready ...";
            objPleaseWait.Close();
        }

        private void button50_Click(object sender, EventArgs e)
        {
            appSets appsets = new appSets();
            appsets.setVars();

            string DirLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + @"\HCVR\";
            string strsql = "select distinct filename_xml from HOR_parse_HCVR_Receipt where convert(date,uploaddate) = '" + DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd") + "'";
            //string strsql = "select distinct filename_xml from HOR_parse_HCVR_Receipt where FileName_xml like 'HCVRLTR_16299%' order by FileName_xml";
            DataTable dataToPrint = dbU.ExecuteDataTable(strsql);
            if (dataToPrint.Rows.Count > 0)
            {
                foreach (DataRow row in dataToPrint.Rows)
                {
                    string fname =  row[0].ToString().Replace(".xml", "_toSCI.csv");
                    string pName = DirLocal +  fname;
                    DataTable dataTocsv = dbU.ExecuteDataTable("Select ClientTransactionID as TransactionID, batchid as dlgUId, filename as FName, UpdAddr1 as coverPageName, UpdAddr5 as coverPageAddress1, UpdAddr2 as coverPageAddress2,  UpdAddr3 as coverPageAddress3, UpdAddr4 as coverPageAddress4,UpdCity as City, UpdState as State, UpdZip as ZIP, Recnum, imbChar" +
                                            " from HOR_parse_HCVR_Receipt where filename_xml = '" + row[0].ToString() + "' order by recnum");
                    createCSV createFilecsv = new createCSV();
                    createFilecsv.printCSV_fullProcess(pName, dataTocsv, "", "N");
                }
            }
        }

        private void button51_Click(object sender, EventArgs e)
        {
            //todo:  update Category  in HOR_parse_files_to_CASS
            Results.Text = "Downloading Files for C Plans ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            // check unzip HLGS get extra character....
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            string resultD = downloadDta.downloadDataP(GlobalVar.DateofFilesToProcess);
            
            //string results = "";
            //NParse_pdf_CPlans parse_pdfs_Information = new NParse_pdf_CPlans();
            //string ResultsPdfS = parse_pdfs_Information.zipFilesinDir_I_Information(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");
            //results = (ResultsPdfS.Length == 0) ? "" : results + ResultsPdfS + Environment.NewLine;

            //objPleaseWait.Close();
            Results.Text = resultD + Environment.NewLine + "CPlans import done!";
            Results.BringToFront();
            objPleaseWait.Close();
        }

        private void button52_Click(object sender, EventArgs e)
        {
            NParse_pdf_CPlans returns = new NParse_pdf_CPlans();
            string results = returns.retun_Cplans();

            string printed = returns.print_Cplans();
            //Special Ticket
            DBUtility dbU;

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable resultsTicket03 = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_6am_Renewals");
            Export_XLSX export = new Export_XLSX();
            if (resultsTicket03.Rows.Count > 0)
                export.CreateExcelFile(resultsTicket03, ProcessVars.InputDirectory + @"From_FTP\", "03");



            Results.Text = "C Plans output ready ...";
        }

        private void button47_Click(object sender, EventArgs e)
        {

        }

        private void button53_Click(object sender, EventArgs e)
        {
            string dateReport = DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd");
            string selection = "select Recnum, grp_nm as Addr1, Street as addr2, street2 as addr3, '' as addr4, '' as addr5, rtrim(ltrim(city + ', ' + state + ' ' + zip)) as addr6   from HOR_parse_ICH_SG_SBC_Annual where filename = '" ;
            NParse_ICH_SG_SBC_Annual annual = new NParse_ICH_SG_SBC_Annual();
            string result = annual.sendtoBCC(dateReport, "HOR_parse_ICH_SG_SBC_Annual", selection);
            
            string localPath = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + dateReport + @"\fromcass\";

            //Directory.CreateDirectory(location);
            DBUtility dbU;

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            //DataTable dataToBCC = dbU.ExecuteDataTable("select distinct filename  from HOR_parse_ICH_SG_SBC_Annual where importdate = '" + dateReport + "' and  bccProcessed is null");
            DataTable dataToBCC = dbU.ExecuteDataTable("select distinct filename  from HOR_parse_ICH_SG_SBC_Annual where mark_re_Process = '2017-11-18' and bccProcessed is null");
            string pNameT = ""; string BCCname = "";
            foreach (DataRow rowf in dataToBCC.Rows)
            {
                annual.print_csvsFilename(rowf["filename"].ToString(), dateReport);
                dbU.ExecuteNonQuery("update HOR_parse_ICH_SG_SBC_Annual set bccProcessed = 'Y' where filename = '" + rowf["filename"].ToString() + "'");
            }
            //annual.print_csvs(localPath, dateReport, "HOR_rpt_ICH_SG_SBC_Annual_SCI","HOR_parse_ICH_SG_SBC_Annual");
            //annual.print_csvsFilename("SG Annual SBC Mail File 10_23_17 NonMappedGrps.xlsx");
            //annual.print_csvsFilename("SG SBC On_Exchange Jan_Dec Renewals Mail File _10_23_17.xlsx");


        }

        private void button54_Click(object sender, EventArgs e)
        {
            Nparse_pdf_addrs_to_csv import = new Nparse_pdf_addrs_to_csv();
            import.extract_info_from_pdf();
            
        }

        private void button55_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Reprint Involuntary Disenrollment...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            NParse_Involuntary involuntary = new NParse_Involuntary();
            string DirLocal = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd") + @"\Involuntary_Disenrollment\";
            //ProcessVars.InputDirectory + @"AbilTo";
            string result = involuntary.reprint(DirLocal);
            Results.Text = "Involuntary Reprint ready ...";
            objPleaseWait.Close();
        }

        private void button56_Click(object sender, EventArgs e)
        {
            string dateReport = DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
            string selection = "select Recnum, FullNAme as Addr1, Street as addr2, '' as addr3, '' as addr4, '' as addr5, rtrim(ltrim(city + ', ' + state + ' ' + zip)) as addr6   from HOR_parse_MISC_RENEW_LETTERS where filename = '";

            NParse_ICH_SG_SBC_Annual annual = new NParse_ICH_SG_SBC_Annual();
            string result = annual.sendtoBCC(dateReport, "HOR_parse_MISC_RENEW_LETTERS", selection);

            string localPath = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\MISC_Renew_letters\" + dateReport + @"\";
            annual.print_csvs(localPath, dateReport, "HOR_rpt_MISC_RENEW_LETTERS_SCI", "HOR_parse_MISC_RENEW_LETTERS");
            //annual.print_csvsFilename("SG Annual SBC Mail File Groups Need SBC to RC 11_2_16.xlsx");
            //annual.print_csvsFilename("SG Annual SBC Mail File UM Groups 2016 SBC to RC_11_4_16.xlsx");

        }

        private void button57_Click(object sender, EventArgs e)
        {
             String PrintNumber = "01";
            appSets appsets = new appSets();
            appsets.setVars();


            HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();

            string resultXMPie = processXMPie.SentTo_XMpieTablesRecnum(46306250, "");
        }

        private void button58_Click(object sender, EventArgs e)
        {
            //HOR_Parse_HNJH_WK_Master_Translation
            //HOR_parse_HNJH_WK


            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Checking files Welcome Kits  MEDICARE...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            //string location = @"\\CIERANT-TAPER\Clients\Horizon BCBS\16-0502_Horizon NJH Production Mngmt\Welcome Kits\SECURE DATA";
            appSets appsets = new appSets();
            appsets.setVars();

            //string location = ProcessVars.InputDirectory + @"HNJH\WK";

            string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\DSNP_WK";
            string locationLocal = ProcessVars.InputDirectory + @"HNJH\DSNP_WK\";
            HNJH_WK_Medicaid procesWKits = new HNJH_WK_Medicaid();

            DirectoryInfo txts = new DirectoryInfo(location);

            FileInfo[] files = txts.GetFiles("NJOUTBOUND*.txt");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    DataTable filesProcessed = dbU.ExecuteDataTable("select filename from HOR_parse_files_to_CASS where filename = '" + file.Name + "'");
                    if (filesProcessed.Rows.Count == 0)
                        errors = procesWKits.Process_WK_Medicare(file.FullName, locationLocal);
                }
            }
            Results.Text = "Welcome Kits MEDICARE ready ...";
            objPleaseWait.Close();
        }

        private void button59_Click(object sender, EventArgs e)
        {
            string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\DSNP_WK";
            string locationLocal = ProcessVars.InputDirectory + @"HNJH\WK\";

            HNJH_WK_Medicaid HnjH_wk = new HNJH_WK_Medicaid();
            //string result = HnjH_wk.Print_HNJH_WK(DateTime.Now.ToString("yyyy_MM_dd"));
            string result = HnjH_wk.Print_HNJH_WK_Medicare(location, locationLocal, GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd").ToString());
            SendMails sendmail2 = new SendMails();
            sendmail2.SendMail("DSNP Welcome kits " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,cgaytan@apps.cierant.com" +
                ",kcarpenter@apps.cierant.com,stilford@apps.cierant.com,kmcnamara@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                         "Please check, files Ready");

        }

        private void button58_MouseEnter(object sender, EventArgs e)
        {
           
        }

        private void button58_MouseEnter_1(object sender, EventArgs e)
        {
            string DirLocal = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\DSNP_WK";


            label8.Text = DirLocal + Environment.NewLine;
            DirectoryInfo originalDATs = new DirectoryInfo(DirLocal);
            try
            {
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.txt");

                if (FilesDAT.Count() > 0)
                {
                    foreach (FileInfo file in FilesDAT)
                    {

                        if (file.Name.IndexOf("_") != 0)
                        {
                            label8.Text = label8.Text + file.Name + Environment.NewLine;
                        }

                    }
                    label8.BackColor = Color.Green;
                }
                else
                {
                    label8.Text = label8.Text + " NO FILES   DSNP_WK>>>>>>";
                    label8.BackColor = Color.Red;
                }
            }
            catch (Exception ex)
            {
                label8.Text = label8.Text + " NO FILES  DSNP_WK >>>>>>";
                label8.BackColor = Color.Red;
            }
        }

        private void button60_Click(object sender, EventArgs e)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable groups = dbU.ExecuteDataTable("select distinct filename from HOR_parse_Maintenance_ID_Cards where  convert(date,importdate) = '2017-03-24' and FileName like 'GRP%'");
            foreach (DataRow row in groups.Rows)
            {
                try
                {
                    string fname = row[0].ToString();
                    SqlParameter[] sqlParams;
                    sqlParams = null;
                    sqlParams = new SqlParameter[] { new SqlParameter("@filename", fname) };
                    DataTable grpData = dbU.ExecuteDataTable("HOR_rpt_idCards_BundleGroups", sqlParams);
                    if (grpData.Rows.Count > 0)
                    {
                        FileInfo fileInfo = new System.IO.FileInfo(fname);
                        string pNameTXT2 = ProcessVars.InputDirectory + @"ID_Cards\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv";
                        if (File.Exists(pNameTXT2))
                            File.Delete(pNameTXT2);
                        createCSV createcsvS = new createCSV();
                        createcsvS.printCSV_fullProcess(pNameTXT2, grpData, "", "");
                        string bundleDir = @"\\CIERANT-TAPER\Clients\Horizon BCBS\ID Cards\Bundle\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
                        File.Copy(pNameTXT2, bundleDir + "\\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv");
                    }
                }
                catch (Exception ex)
                {
                    var errFix = ex.Message;
                }
            }


        }

        private static DataTable data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("Batch");
            newt.Columns.Add("XMPieName");

            return newt;
        }

        private void button61_Click(object sender, EventArgs e)
        {
            Cycle01 cycle01 = new Cycle01();
            cycle01.create_Tickets01();
            Results.Text = "Ticket 01 done at " + DateTime.Now.ToString("yyyy_MM_dd   HH_mm");
        }

        private void button62_Click(object sender, EventArgs e)
        {
            Form3 form3 = new Form3();
            form3.Show();
        }

        private void button63_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Downloading PRIORITY Files for CR ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
           //// string extractPath = ProcessVars.InputDirectory + "From_FTP";
            string ResultsPdf = "";
           //// N_loadFromFTP downloadDta = new N_loadFromFTP();
           //// NParse_pdfs parse_pdfs = new NParse_pdfs();


           ////// string resultD = downloadDta.downloadData(GlobalVar.DateofFilesToProcess);
            
           ////// string resultD = downloadDta.downloadDataPriority(GlobalVar.DateofFilesToProcess);

           //// string[] files = GetFiles(ProcessVars.InputDirectory + @"From_FTP", "CRC*.zip|CRN*.zip", SearchOption.TopDirectoryOnly);

           //// if (files.Length > 0)
           //// {
           ////     downloadDta.expand_CRNJLTR_ZIPPriority(GlobalVar.DateofFilesToProcess);
           //// }



           // downloadDta.expand_CRNJLTR_ZIPPriority(GlobalVar.DateofFilesToProcess);

            createEmail createemail = new createEmail();
            createemail.produceSummary_Uploaded_PR();

            //if (resultD == "")
                Results.Text =  "Process for Ticket 02 Priority ready " + "\\n" + ResultsPdf ;
            //else
            //    Results.Text = resultD + "\\n" + ResultsPdf + Environment.NewLine;

            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
        }

        private void button64_Click(object sender, EventArgs e)
        {
             Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Closing Ticket 02 ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            string erros = "";
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_toFix");
            //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_toFix");
            //DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_only_HIX_Inv");
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
            }
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
        }

        private void button65_Click(object sender, EventArgs e)
        {
            createEmail createemail = new createEmail();
            createemail.produceSummary_Uploaded_RegCRR();

        }

        private void pictureBox1_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            SqlParameter[] sqlParams;

            sqlParams = new SqlParameter[] { new SqlParameter("@Date", GlobalVar.DateofProcess.ToString("yyyy-MM-dd")),
                                             new SqlParameter("@type", "ALL")};

            DataTable processedData = dbU.ExecuteDataTable("HOR_rpt_CR_Radius_to_email", sqlParams);
            //Ticket02 zip 1st



            //DataTable resultsTicket02 = dbU.ExecuteDataTable("HOR_scr_DailyUpload_Cycle_02");
            dataGridView1.DataSource = processedData;
            DataGridViewColumn column = dataGridView1.Columns[0];
            column.Width = 160;
            DataGridViewColumn column2 = dataGridView1.Columns[3];
            column2.Width = 160;

            foreach (DataGridViewRow row in dataGridView1.Rows)
            {
                try
                {
                    if ((int)row.Cells["Records in CSV"].Value != (int)row.Cells["PDFs"].Value)
                        row.DefaultCellStyle.BackColor = Color.Red;
                }
                catch (Exception ex)
                {

                }
            }
            Cursor.Current = Cursors.Default;
            label4.Text = "Proces........";
        }

        private void button24_Click_1(object sender, EventArgs e)
        {
            string fileName = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2017-08-29\from_FTP\SMNJAL_17240_26373.pdf";

            string TTName = "HOR_parse_MBA_SMn";
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            string FFName = fileInfo.Name;

            bool GBill = (fileInfo.Name.Substring(0, 3) == "EPB") ? true : false;
            bool CBill = (fileInfo.Name.Substring(0, 5) == "EP0GH") ? true : false;
            bool CR2 = (fileInfo.Name.Substring(0, 3) == "CR_") ? true : false;
            bool MBA = (fileInfo.Name.Substring(0, 3) == "MBA") ? true : false;
            bool SBC = (fileInfo.Name.Substring(0, 3) == "SBC") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "SMN") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "PNO") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "SVN") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "PRT") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "ABL") ? true : false;
            if (!MBA)
                MBA = (fileInfo.Name.Substring(0, 3) == "OEL") ? true : false;


            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            
            //pritn file to SCI
            SqlParameter[] sqlParams;
            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };
            string spName = "";
            if (GBill || CBill)
                spName = "HOR_rpt_PARSE_cbILLSto_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";
            else if (CR2)
                spName = "HOR_rpt_PARSE_CR2to_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";
            else if (MBA)
                spName = "HOR_rpt_PARSE_CR2to_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";
            else if (SBC)
                spName = "HOR_rpt_PARSE_SBCto_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";

            else
                spName = "HOR_rpt_PARSE_to_SCI";
            //if (TTName == "HOR_Fraud")
            //    spName = "HOR_rpt_PARSE_Fraud_to_SCI";

            DataTable datato_SCI = dbU.ExecuteDataTable(spName, sqlParams);
            if (datato_SCI.Rows.Count > 0)
            {
                createCSV createcsv = new createCSV();
                //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                string pName = ProcessVars.OtherProcessed + FFName.Substring(0, FFName.Length - 4) + ".csv";
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < datato_SCI.Columns.Count; index++)
                {
                    fieldnames.Add(datato_SCI.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in datato_SCI.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < datato_SCI.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }

            }
        }

        private void button66_Click(object sender, EventArgs e)
        {
            N_loadFromFTP movefiles = new N_loadFromFTP();


             Cursor.Current = Cursors.WaitCursor;
            GlobalVar.dbaseName = "BCBS_Horizon";
            string strsql = "select zipname as Fname from HOR_parse_files_to_CASS where DateProcess = '2017-08-30' and zipname is not null " +
                            "union all select filename as Fname from HOR_parse_files_to_CASS where DateProcess = '2017-08-30' and filename like '%.pdf'";

            DataTable processedData = dbU.ExecuteDataTable(strsql);
            //Ticket02 zip 1st
            if (processedData.Rows.Count > 0)
            {
                foreach (DataRow dr in processedData.Rows)
                {
                    string _ftpURL = "ftp://sftp.cierant.com";         //Host URL or address of the FTP server
                    string _UserName = "Horizon";             //User Name of the FTP server
                    string _Password = "CyRyk1al";          //Password of the FTP server
                    string _ftpDirectory = "/HorizonBCBS/IN/";      //The directory in FTP server where the file will be uploaded
                    string _FileName = dr["Fname"].ToString();         //File name, which one will be uploaded
                    string _ftpDirectoryProcessed = "sftp.cierant.com/HorizonBCBS/IN/processed_automation_Cierant/"; //The directory in FTP server where the file will be moved

                    movefiles.MoveFile(_ftpURL, _UserName, _Password, _ftpDirectory, _ftpDirectoryProcessed, _FileName);
                }
            }



        }

        private void button67_Click(object sender, EventArgs e)
        {
            zip_Pdfs_Prod zipfiles = new zip_Pdfs_Prod();
            string result = zipfiles.select_to_zip();
        }

        private void button68_Click(object sender, EventArgs e)
        {
            NParse_to_Merge_XMPie parse = new NParse_to_Merge_XMPie();
            parse.Output_Full_SMB();
        }

        private void button69_Click(object sender, EventArgs e)
        {
            NParse_ICH_SG_SBC_Annual printF = new NParse_ICH_SG_SBC_Annual();
            string strsql = "select D.[recnum],[filename],[importdate],[maingrp],[subgrp],[FIRST_NAME],[LAST_NAME],[Product],[grp_nm],[street],[street2],[city],[state],[zip],[product1],[EXCHANGE],[EFFECTIVE_DATE],[PRD_ID],[RX_prdID],[Dental_prdID],[mg_efdate],[mainsubprd_efdate],[Health PRD ID1],[Health PRD ID2],[rx_prd_id] from HOR_parse_ICH_SG_SBC_Annual D inner join HOR_parse_ICH_SG_SBC_data_NOT_IN_Retired R on D.recnum = R.recnum";
            printF.print_A_csvsFilename(strsql, "Records not in Retired Data.csv");
        }

        private void button70_Click(object sender, EventArgs e)
            {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "Processing Commercial Correspondence ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            Cursor.Current = Cursors.WaitCursor;
            N_Parse_CommercialCorrespondence processCommercial = new N_Parse_CommercialCorrespondence();
            int  results = processCommercial.expand_zips();
            if (results > 0)
                {
                SendMails sendmail2 = new SendMails();
                sendmail2.SendMail("Commercial Correspondence " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,cgaytan@apps.cierant.com" +
                    ",kcarpenter@apps.cierant.com,stilford@apps.cierant.com,kmcnamara@apps.cierant.com,dgannuscio@cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             "Please check, files Ready");
                Results.Text = "Processing Commercial Correspondence ... DONE ";
                }
            else
                Results.Text = "Processing Commercial Correspondence ... DONE NO Files";
            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.BringToFront();
            }

        private void button71_Click(object sender, EventArgs e)
        {
            Cursor.Current = Cursors.WaitCursor;
            Results.Text = "re-parse FSA ...";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();

            Cursor.Current = Cursors.WaitCursor;
            N_Parse_CommercialCorrespondence processCommercial = new N_Parse_CommercialCorrespondence();
            processCommercial.reParse_FSA();

            Cursor.Current = Cursors.Default;
            objPleaseWait.Close();
            Results.Text = "re-parse FSA done!!!";
            Results.BringToFront();
        }

        private void button72_Click(object sender, EventArgs e)
        {
            //todo:  update Category  in HOR_parse_files_to_CASS
            Results.Text = "Downloading from Cierant MFT";
            PleaseWait objPleaseWait = new PleaseWait();
            objPleaseWait.Show();
            Application.DoEvents();
            // check unzip HLGS get extra character....
            N_loadFromFTP downloadDta = new N_loadFromFTP();
            string resultD = downloadDta.downloadData_MFT(GlobalVar.DateofFilesToProcess);

            //string results = "";
            //NParse_pdf_CPlans parse_pdfs_Information = new NParse_pdf_CPlans();
            //string ResultsPdfS = parse_pdfs_Information.zipFilesinDir_I_Information(DateTime.Now.ToString("yyyy-MM-dd"), ProcessVars.InputDirectory + @"from_FTP");
            //results = (ResultsPdfS.Length == 0) ? "" : results + ResultsPdfS + Environment.NewLine;

            //objPleaseWait.Close();
            Results.Text = resultD + Environment.NewLine + "download from Cierant MFT done!";
            Results.BringToFront();
            objPleaseWait.Close();
        }

        private void button73_Click(object sender, EventArgs e)
        {
            Nparse_xls_SHBP parse_SHBP_eoc = new Nparse_xls_SHBP();
            parse_SHBP_eoc.parse_all_SHBP_EOC();
        }

        private void button74_Click(object sender, EventArgs e)
        {
            Nparse_MAEOB_cards_pdfs parse = new Nparse_MAEOB_cards_pdfs();
            parse.parse_all_MAEOB_cards();
        }

        private void button75_Click(object sender, EventArgs e)
            {
            Test_Development formDev = new Test_Development();
            this.Hide();
            formDev.ShowDialog();
            }

        

    }
}
