using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Configuration;
using System.Xml;
using System.Xml.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using iTextSharp.text.pdf;
using iTextSharp.text;
using System.Collections.Specialized;

namespace CodeCallService
{
    public class fKitsAcctAcct
    {
        public string prodfamily { get; set; }

        public string DocumentDesc { get; set; }

        public string Source { get; set; }

        public string fileName { get; set; }

        public string Expression { get; set; }

        public string nameonly { get; set; }
        
    }
    //public class CreateKitsAcct
    public class CreateKitsAcct
    {
        CodeCallService.DBUtility dbU;

        string pfsDir = @"\\freenas\BCBSMA\PDF_Files\Non-Contract_SourceDocs";
        string RidersFolder = @"\\freenas\BCBSMA\PDF_Files\e__company_tradingpartners_Marketing_Comm_PDF";
        string unzipLocal = @"C:\CierantProjects_dataLocal\wBCBS_MA_Acct";
        
        string FileSummary = "";
        int SeqNum = 0;
        List<string> arrayDocs = new List<string>();
        List<fKitsAcctAcct> listKits = new List<fKitsAcctAcct>();

        string summary = "";
         public string generalProess_Kits()
        {
            string result = "";
            string subdir = "ACCT";
            result = load_XmpieFiles(subdir);

            subdir = "ACCT-NEW";
            result = load_XmpieFiles(subdir);


            return result;
        }
        private string[] GetWeekRange(DateTime dateToCheck)
        {
            string[] result = new string[2];
            TimeSpan duration = new TimeSpan(0, 0, 0, 0); //One day 
            DateTime dateRangeBegin = dateToCheck;
            DateTime dateRangeEnd = DateTime.Today.Add(duration);

            dateRangeBegin = dateToCheck.AddDays(-(int)dateToCheck.DayOfWeek);
            dateRangeEnd = dateToCheck.AddDays(6 - (int)dateToCheck.DayOfWeek);

            result[0] = dateRangeBegin.Date.ToString("yyyy-MM-dd");
            result[1] = dateRangeEnd.Date.ToString("yyyy-MM-dd");
            return result;

        }
        private void ClearFolderAll(string FolderName)
        {
            DirectoryInfo dir = new DirectoryInfo(FolderName);
            try
            {
                foreach (FileInfo fi in dir.GetFiles())
                {
                    fi.IsReadOnly = false;
                    fi.Delete();
                }

                foreach (DirectoryInfo di in dir.GetDirectories())
                {
                    ClearFolderAll(di.FullName);
                    di.Delete();
                }
            }
            catch (Exception ex)
            {
                var err = ex.Message;
            }

        }
        private void ClearFolder(string FolderName, string sdir)
        {

            try
            {
                DirectoryInfo dir = new DirectoryInfo(FolderName + "\\" + sdir);
                foreach (FileInfo fi in dir.GetFiles())
                {
                    fi.IsReadOnly = false;
                    fi.Delete();
                }
            }
            catch (Exception ex)
            {
                var err = ex.Message;
            }
        }


        public string expandXMPie(string sourceXMPie, string netOutput, string sdir)
        {
            string response = "";

            try
            {
                //System.IO.DirectoryInfo unzip = new DirectoryInfo(unzipLocal);
                //foreach (FileInfo file in unzip.GetFiles())
                //{
                //    file.Delete();
                //}
                System.IO.DirectoryInfo diLocal = new DirectoryInfo(unzipLocal + "\\" + sdir);
                //foreach (DirectoryInfo dirL in diLocal.GetDirectories())
                //{
                foreach (FileInfo file in diLocal.GetFiles())
                {
                    file.Delete();
                }
                //}



            }
            catch (Exception ex)
            {
                response = response + ex.Message;
            }

            string[] filesindirectory = Directory.GetDirectories(sourceXMPie + "\\" + sdir);
            //foreach (var subdir in filesindirectory)
            //{
            string NewsubdirName = netOutput + @"\" + sdir;  // subdir.Remove(0, subdir.LastIndexOf('\\') + 1);
            string unzipVer = unzipLocal + @"\" + sdir;  // subdir.Remove(0, subdir.LastIndexOf('\\') + 1);
            System.IO.Directory.CreateDirectory(unzipVer);


            System.IO.Directory.CreateDirectory(NewsubdirName);
            //foreach (FileInfo f in subdir)
            DirectoryInfo XmpieZIPs = new DirectoryInfo(sourceXMPie + "\\" + sdir);

            System.IO.DirectoryInfo di = new DirectoryInfo(NewsubdirName);
            foreach (FileInfo file in di.GetFiles())
            {
                file.Delete();
            }
            //foreach (DirectoryInfo dir in di.GetDirectories())
            //{
            //    dir.Delete(true);
            //}

            foreach (FileInfo f in XmpieZIPs.GetFiles("*.zip"))
            {


                try
                {
                    if (f.Name.IndexOf("__") == 0)
                    { }
                    else
                    {
                        System.IO.Directory.CreateDirectory(NewsubdirName);
                        using (ZipArchive archive = ZipFile.OpenRead(sourceXMPie + "\\" + sdir + @"\" + f.Name))  //http://stackoverflow.com/questions/19740099/system-io-compression-filesystem-assembly-in-vs-2010
                        {
                            foreach (ZipArchiveEntry entry in archive.Entries)
                            {
                                entry.ExtractToFile(Path.Combine(unzipVer, entry.FullName));
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    response = response + ex.Message;
                }

            }
            //}
            return response;
        }
        public void CleanDirsFor_Kits()
        {
            string[] daysWeek = GetWeekRange(DateTime.Now);

            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_Acct";
            //System.IO.Directory.Delete(netOutput,true);
            System.IO.Directory.CreateDirectory(netOutput);
            ClearFolderAll(netOutput);
            System.IO.Directory.CreateDirectory(unzipLocal);
        }
       
        public string load_XmpieFiles(string sdir)
        {

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteNonQuery("Update BCBS_MA_parse_eoc_Acct set summaryPrint = ''"); // where filename = '" + file.Name + "'");
            string drivesOk = "";
            string[] daysWeek = GetWeekRange(DateTime.Now);

            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_Acct";
            string errFileName = netOutput + "\\errors in assembly.txt";
            //System.IO.Directory.Delete(netOutput,true);
            //ClearFolder(netOutput, sdir);


            System.IO.Directory.CreateDirectory(netOutput);
            string sourceXMPie = @"\\freenas\BCBSMA\PRODUCTION\XMPIE";
            string resultUnzip = expandXMPie(sourceXMPie, netOutput, sdir);
            if (resultUnzip == "")
            {
                appSets checkD = new appSets();
                checkD.setVars();
                drivesOk = checkD.checkDrives();
                if (drivesOk == "")
                {
                    //create subdirs
                    DirectoryInfo dir = new DirectoryInfo(unzipLocal);

                    System.IO.Directory.CreateDirectory(netOutput + @"\" + sdir);


                    //foreach (DirectoryInfo di in dir.GetDirectories())
                    //{
                    //    System.IO.Directory.CreateDirectory(netOutput + @"\" + di.Name);
                    //}
                    string resultKit = "";
                    var files = from fileName in
                                    Directory.EnumerateFiles(unzipLocal + @"\" + sdir, "*.pdf", SearchOption.AllDirectories)
                                where fileName.ToLower().Contains(".pdf")
                                select fileName;
                    //// (ProcessVars.dataEOC + @"\from_XMpie")

                    foreach (var fileName in files)
                    {
                        FileInfo file = new FileInfo(fileName);
                        string subdir = file.Directory.ToString().Remove(0, file.Directory.ToString().LastIndexOf('\\') + 1);
                        resultKit = assembly_KITS(fileName, netOutput, subdir);

                        if (resultKit != "")
                        {
                            if (File.Exists(errFileName))
                            {
                                
                                using (StreamReader sr = File.OpenText(errFileName))
                                {
                                    string s = "";
                                    while ((s = sr.ReadLine()) != null)
                                    {
                                        Console.WriteLine(s);
                                    }
                                }
                            }
                            else
                            {
                                using (StreamWriter sw = File.CreateText(errFileName))
                                {
                                    sw.WriteLine(@resultKit);
                                    sw.Close();

                                }
                            }

                        }
                        resultKit = "";
                    }
                    string repFilename = netOutput + @"\AssemblySummary_" + sdir + "_" + DateTime.Now.ToString("yyyy_MM_dd") + ".csv";
                    DataTable printed = dbU.ExecuteDataTable("select filename, recnum, cov_pack, ua_cov_cd, summaryPrint from BCBS_MA_parse_eoc_Acct where convert(date,dateAssemby) ='" + DateTime.Now.ToString("yyyy-MM-dd") + "' and XMPiePrinted = '" + sdir + "' order by filename, recnum");
                    CreateCSV createfile = new CreateCSV();
                    createfile.printCSV_fullProcess(repFilename, printed, "", "");
                }

                //string nfilename = file.Directory + "\\__" + file.Name;
                //if (File.Exists(nfilename))
                //    File.Delete(nfilename);
                //File.Move(file.FullName, nfilename);
                //drivesOk = drivesOk + resultKit + " " + file.Name + Environment.NewLine;



            }
            else
                drivesOk = "No files";

            return drivesOk;
        }
        public string assembly_KITS(string filename, string netOutput, string subdir)
        {

            var recnum = dbU.ExecuteScalar("select max(Seq) from BCBS_MA_log_eoc_Acct_Kit_assembly");
            int GRecnum = 0;
            int recordnumber = 0;
            int Tcoll = 0;
            int TcollMDX = 0;
            string errors_assemble = "";
            if (recnum.ToString() == "")
                SeqNum = 1;
            else
                SeqNum = Convert.ToInt32(recnum.ToString()) + 1;


            string resut = ""; string Xrecnum = ""; string Xkit_Type = "";
            FileInfo fileInfo = new System.IO.FileInfo(filename);
            string acct_num = "";
            string[] fInfo = fileInfo.Name.Split('_');
            Xrecnum = fInfo[0].ToString();
            var acct = dbU.ExecuteScalar("select acct_num  + '|' + filename from BCBS_MA_parse_eoc_Acct where recnum = " + Xrecnum);
            if (acct == null)
            {
                resut = "";
            }
            else
            {
                string[] vars = acct.ToString().Split('|');
                acct_num = vars[0].ToString();

                if (acct_num == "4957793")
                    resut = "";


                string sqlstr = "select acct_num , cov_pack, ProdFamilies, med_mcc, recnum, Count_OLBC_Collaterals, Count_OLBC_Collat_MDX, ua_cov_cd, grp_num from BCBS_MA_parse_eoc_Acct where acct_num = '" + vars[0].ToString() +
                    "' and dup = 'N' and FileName = '" + vars[1].ToString() + "' " +
                    "order by (  case when ProdFamilies = 'HMO' then '0' when ProdFamilies = 'PPO' then '1' when ProdFamilies = 'IND' then '2' when ProdFamilies = 'SNR' then '3' when ProdFamilies = 'MDX' then '4' when ProdFamilies = 'DNTL' then '5' end)";
                DataTable prodfamilies = dbU.ExecuteDataTable(sqlstr);
                if (prodfamilies.Rows.Count == 0)
                    resut = "";

                else if (prodfamilies.Rows.Count > 0)
                {
                    int totitems = 0;
                    
                    string recRecnum = "";
                   listKits.Clear();
                    foreach (DataRow row in prodfamilies.Rows)
                    {
                        if (row[5].ToString() != "0")
                            Tcoll = 1;
                        if (row[6].ToString() != "0")
                            TcollMDX = 1;
                        recRecnum = row[4].ToString();
                        dbU.ExecuteScalar("Insert into BCBS_MA_log_eoc_Acct_Kit_assembly(seq, recnum, filename, AssemblyDate,acct_num, prodfamily, expression, filenameassembly ) values(" +
                          SeqNum + "," + recRecnum + ",'" + fileInfo.Name + "',GETDATE(),'" + acct_num + "','" + row[2].ToString() +
                          "','Start Assembly','" + " MCC = "  + row[3].ToString() + " cov_pack = " + row[1].ToString() + "  ua_cov_cd = " + row[7].ToString() + "  grp_num = " + row[8].ToString() + "')");

                        SeqNum++;

                        string families = row[2].ToString();
                        string medMCC = row[3].ToString();


                        totitems++;
                        if (totitems > 1)
                        {
                            resut = "";
                        }
                        errors_assemble = errors_assemble + assembly_IncludeInAll(fileInfo, fInfo[1].ToString().ToUpper(), recRecnum, netOutput, subdir, families, medMCC, totitems);

                    }
                    if (listKits.Count > 0 && errors_assemble == "")
                    {
                        if (Tcoll == 1)
                        {
                            totitems++;
                            errors_assemble = errors_assemble + assembly_IncludeInAll(fileInfo, fInfo[1].ToString().ToUpper(), recRecnum, netOutput, subdir, "FORMRX", "0", totitems);
                        }
                        if (TcollMDX == 1)
                        {
                            totitems++;
                            errors_assemble = errors_assemble + assembly_IncludeInAll(fileInfo, fInfo[1].ToString().ToUpper(), recRecnum, netOutput, subdir, "FORMRXMDX", "0", totitems);
                        }
                        assembly_General(fileInfo, listKits, Xrecnum, netOutput, subdir, acct_num);

                    }
                   
                }
            }
            if(errors_assemble != "")
            {
                errors_assemble = "Error in Acct_Num " + acct_num  +Environment.NewLine + errors_assemble;

            }

            return errors_assemble;
        }
        public string assembly_IncludeInAll(FileInfo filename, string type, string Xrecnum, string netOutput, string subdir, string families, string medMCC, int times)
        {
           
            int numElements = 0;
            bool result = false;
            int error = 0;
            string errorDesc = "";
            arrayDocs.Clear();
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            string strsql = "";
            if (times == 1)
            {
                strsql = "select document, Expression, Source, prodfamily, document_desc from Master_KitType where [Kit_Type Value] = '" + type +
                "' and (prodfamily = '" + families + "' or prodfamily = 'ALL') "
                + " and (source = 'INCLUDE IN ALL' or source = 'IF' or Source = 'IF_SOME_R'  or Source = 'IF_SOME_F') order by [Order]";
            }
            else
            {
                strsql = "select document, Expression, Source, prodfamily, document_desc from Master_KitType where [Kit_Type Value] = '" + type +
                "' and prodfamily = '" + families + "' "
                + " and (source = 'INCLUDE IN ALL' or source = 'IF' or Source = 'IF_SOME_R'  or Source = 'IF_SOME_F') order by [Order]";
            }
            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            DataTable filesforDN3 = dbU.ExecuteDataTable(strsql);
            arrayDocs.Add(filename.FullName.ToString());


            // List<fKitsAcctAcct> listKits = new List<fKitsAcctAcct>();

            if (times == 1)
            {
                fKitsAcctAcct kitcoll1 = new fKitsAcctAcct();
                kitcoll1.prodfamily = "ALL";
                kitcoll1.DocumentDesc = "EOC Letter";
                kitcoll1.Source = "XMPie";
                kitcoll1.fileName = filename.FullName.ToString();
                kitcoll1.Expression = "";
                kitcoll1.nameonly = filename.Name.ToString();
                listKits.Add(kitcoll1);
            }
            //NameValueCollection kitCollection = new NameValueCollection();
            //kitCollection.Add(filename.FullName.ToString(), "");

            foreach (DataRow row in filesforDN3.Rows)
            {
                string docname = row[0].ToString();
                string expression = row[1].ToString();
                string source = row[2].ToString();
                //string medMCC = row[3].ToString();
                if (File.Exists(pfsDir + @"\" + docname))
                {
                    if (source == "INCLUDE IN ALL")
                    {
                        fKitsAcctAcct kitcoll2 = new fKitsAcctAcct();
                        kitcoll2.prodfamily = row["Prodfamily"].ToString();
                        kitcoll2.DocumentDesc = row["document_desc"].ToString();
                        kitcoll2.Source = row["Source"].ToString();
                        kitcoll2.fileName = pfsDir + @"\" + docname;
                        kitcoll2.Expression = expression;
                        kitcoll2.nameonly = docname;
                        listKits.Add(kitcoll2);
                        // kitCollection.Add(pfsDir + @"\" + docname, expression);
                    }


                    else if (expression != "" && source == "IF")
                    {
                        DataTable conditiontrue = dbU.ExecuteDataTable("select files_Riders from BCBS_MA_parse_eoc_Acct where recnum = " + Xrecnum + " and " + expression);
                        if (conditiontrue.Rows.Count > 0)
                        {
                            fKitsAcctAcct kitcoll2a = new fKitsAcctAcct();
                            kitcoll2a.prodfamily = row["Prodfamily"].ToString();
                            kitcoll2a.DocumentDesc = row["document_desc"].ToString();
                            kitcoll2a.Source = row["Source"].ToString();
                            kitcoll2a.fileName = pfsDir + @"\" + docname;
                            kitcoll2a.Expression = "";
                            kitcoll2a.nameonly = docname;
                            listKits.Add(kitcoll2a);
                        }
                    }
                    else
                    {
                        error++;
                    }
                }
                else
                {
                    if (source == "IF_SOME_R")
                    {
                        //string whereExp = (expression == "") ? "" : " and " + expression;
                        var RiderFiles = dbU.ExecuteScalar("select files_Riders from BCBS_MA_parse_eoc_Acct where recnum = " + Xrecnum);
                        if (RiderFiles.ToString().Length > 1)
                        {
                            if (!RiderFiles.ToString().Contains("~"))
                            {
                                if (expression == "")
                                {
                                    if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                    {
                                        fKitsAcctAcct kitcoll2b = new fKitsAcctAcct();
                                        kitcoll2b.prodfamily = row["Prodfamily"].ToString();
                                        kitcoll2b.DocumentDesc = row["document_desc"].ToString();
                                        kitcoll2b.Source = row["Source"].ToString();
                                        kitcoll2b.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                        kitcoll2b.Expression = "";//expression;
                                        kitcoll2b.nameonly = RiderFiles.ToString() + ".pdf";
                                        listKits.Add(kitcoll2b);
                                    }
                                    else
                                    {
                                        error++;
                                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                    }
                                }
                                else
                                {
                                    if (expression.IndexOf("not ") == -1)
                                    {
                                        if (RiderFiles.ToString().ToUpper().IndexOf(expression) != -1)
                                        {
                                            if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                            {
                                                fKitsAcctAcct kitcoll2c = new fKitsAcctAcct();
                                                kitcoll2c.prodfamily = row["Prodfamily"].ToString();
                                                kitcoll2c.DocumentDesc = row["document_desc"].ToString();
                                                kitcoll2c.Source = row["Source"].ToString();
                                                kitcoll2c.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                                kitcoll2c.Expression = "";//expression;
                                                kitcoll2c.nameonly = RiderFiles.ToString() + ".pdf";
                                                listKits.Add(kitcoll2c);
                                            }
                                            else
                                            {
                                                error++;
                                                errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (RiderFiles.ToString().ToUpper().IndexOf(expression.Substring(4, expression.Length - 4)) == -1)
                                        {
                                            if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                            {
                                                fKitsAcctAcct kitcoll2d = new fKitsAcctAcct();
                                                kitcoll2d.prodfamily = row["Prodfamily"].ToString();
                                                kitcoll2d.DocumentDesc = row["document_desc"].ToString();
                                                kitcoll2d.Source = row["Source"].ToString();
                                                kitcoll2d.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                                kitcoll2d.Expression = "";//expression;
                                                kitcoll2d.nameonly = RiderFiles.ToString() + ".pdf";
                                                listKits.Add(kitcoll2d);
                                            }
                                            else
                                            {
                                                error++;
                                                errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                            }
                                        }
                                    }
                                }
                            }
                            else
                            {
                                string[] fileR = RiderFiles.ToString().Split('~');
                                foreach (string item in fileR)
                                {
                                    if (item.Length > 1)
                                    {
                                        if (expression == "")
                                        {
                                            if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                            {
                                                fKitsAcctAcct kitcoll2e = new fKitsAcctAcct();
                                                kitcoll2e.prodfamily = row["Prodfamily"].ToString();
                                                kitcoll2e.DocumentDesc = row["document_desc"].ToString();
                                                kitcoll2e.Source = row["Source"].ToString();
                                                kitcoll2e.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                                kitcoll2e.Expression = "";//expression;
                                                kitcoll2e.nameonly = item.ToString() + ".pdf";
                                                listKits.Add(kitcoll2e);
                                            }
                                            else
                                            {
                                                error++;
                                                errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                            }
                                        }
                                        else
                                        {
                                            if (expression.IndexOf("not ") == -1)
                                            {
                                                if (item.ToUpper().IndexOf(expression) != -1)
                                                {
                                                    if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                                    {
                                                        fKitsAcctAcct kitcoll2f = new fKitsAcctAcct();
                                                        kitcoll2f.prodfamily = row["Prodfamily"].ToString();
                                                        kitcoll2f.DocumentDesc = row["document_desc"].ToString();
                                                        kitcoll2f.Source = row["Source"].ToString();
                                                        kitcoll2f.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                                        kitcoll2f.Expression = "";//expression;
                                                        kitcoll2f.nameonly = item.ToString() + ".pdf";
                                                        listKits.Add(kitcoll2f);
                                                    }
                                                    else
                                                    {
                                                        error++;
                                                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                if (item.ToUpper().IndexOf(expression.Substring(4, expression.Length - 4)) == -1)
                                                {
                                                    if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                                    {
                                                        fKitsAcctAcct kitcoll2g = new fKitsAcctAcct();
                                                        kitcoll2g.prodfamily = row["Prodfamily"].ToString();
                                                        kitcoll2g.DocumentDesc = row["document_desc"].ToString();
                                                        kitcoll2g.Source = row["Source"].ToString();
                                                        kitcoll2g.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                                        kitcoll2g.Expression = "";//expression;
                                                        kitcoll2g.nameonly = item.ToString() + ".pdf";
                                                        listKits.Add(kitcoll2g);
                                                    }
                                                    else
                                                    {
                                                        error++;
                                                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (source == "IF_SOME_F")
                    {
                        //string whereExp = (expression == "") ? "" : " and " + expression;
                        dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
                        var RiderFiles = dbU.ExecuteScalar("select files_pcp_Flier from BCBS_MA_parse_eoc_Acct where recnum = " + Xrecnum);
                        if (RiderFiles.ToString().Length > 1)
                        {
                            if (!RiderFiles.ToString().Contains("~"))
                            {
                                if (expression == "")
                                {
                                    if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                    {
                                        fKitsAcctAcct kitcoll2h = new fKitsAcctAcct();
                                        kitcoll2h.prodfamily = row["Prodfamily"].ToString();
                                        kitcoll2h.DocumentDesc = row["document_desc"].ToString();
                                        kitcoll2h.Source = row["Source"].ToString();
                                        kitcoll2h.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                        kitcoll2h.Expression = "";//expression;
                                        kitcoll2h.nameonly = RiderFiles.ToString() + ".pdf";
                                        listKits.Add(kitcoll2h);
                                    }
                                    else
                                    {
                                        error++;
                                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                    }
                                }
                                else
                                {
                                    if (expression.IndexOf("not ") == -1)
                                    {
                                        if (RiderFiles.ToString().ToUpper().IndexOf(expression) != -1)
                                        {
                                            if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                            {
                                                fKitsAcctAcct kitcoll2j = new fKitsAcctAcct();
                                                kitcoll2j.prodfamily = row["Prodfamily"].ToString();
                                                kitcoll2j.DocumentDesc = row["document_desc"].ToString();
                                                kitcoll2j.Source = row["Source"].ToString();
                                                kitcoll2j.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                                kitcoll2j.Expression = "";//expression;
                                                kitcoll2j.nameonly = RiderFiles.ToString() + ".pdf";
                                                listKits.Add(kitcoll2j);
                                            }
                                            else
                                            {
                                                error++;
                                                errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        if (RiderFiles.ToString().ToUpper().IndexOf(expression.Substring(4, expression.Length - 4)) == -1)
                                        {
                                            if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                            {
                                                fKitsAcctAcct kitcoll2k = new fKitsAcctAcct();
                                                kitcoll2k.prodfamily = row["Prodfamily"].ToString();
                                                kitcoll2k.DocumentDesc = row["document_desc"].ToString();
                                                kitcoll2k.Source = row["Source"].ToString();
                                                kitcoll2k.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                                kitcoll2k.Expression = "";//expression;
                                                kitcoll2k.nameonly = RiderFiles.ToString() + ".pdf";
                                                listKits.Add(kitcoll2k);
                                            }
                                            else
                                            {
                                                error++;
                                                errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                            }
                                        }
                                    
                                    }

                                }
                            }
                            else
                            {
                                string[] fileR = RiderFiles.ToString().Split('~');
                                foreach (string item in fileR)
                                {
                                    if (item.Length > 1)
                                    {
                                        if (expression == "")
                                        {
                                            if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                            {
                                                fKitsAcctAcct kitcoll2m = new fKitsAcctAcct();
                                                kitcoll2m.prodfamily = row["Prodfamily"].ToString();
                                                kitcoll2m.DocumentDesc = row["document_desc"].ToString();
                                                kitcoll2m.Source = row["Source"].ToString();
                                                kitcoll2m.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                                kitcoll2m.Expression = "";//expression;
                                                kitcoll2m.nameonly = item.ToString() + ".pdf";
                                                listKits.Add(kitcoll2m);
                                            }
                                            else
                                            {
                                                error++;
                                                errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                            }
                                        }
                                        else
                                        {
                                            if (expression.IndexOf("not ") == -1)
                                            {
                                                if (item.ToUpper().IndexOf(expression) != -1)
                                                {
                                                    if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                                    {
                                                        fKitsAcctAcct kitcoll2n = new fKitsAcctAcct();
                                                        kitcoll2n.prodfamily = row["Prodfamily"].ToString();
                                                        kitcoll2n.DocumentDesc = row["document_desc"].ToString();
                                                        kitcoll2n.Source = row["Source"].ToString();
                                                        kitcoll2n.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                                        kitcoll2n.Expression = "";//expression;
                                                        kitcoll2n.nameonly = item.ToString() + ".pdf";
                                                        listKits.Add(kitcoll2n);
                                                    }
                                                    else
                                                    {
                                                        error++;
                                                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                string Expression2 = expression.Substring(4, expression.Length - 4);
                                                if (item.ToUpper().IndexOf(Expression2) == -1)
                                                {
                                                    if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                                    {
                                                        fKitsAcctAcct kitcoll2p = new fKitsAcctAcct();
                                                        kitcoll2p.prodfamily = row["Prodfamily"].ToString();
                                                        kitcoll2p.DocumentDesc = row["document_desc"].ToString();
                                                        kitcoll2p.Source = row["Source"].ToString();
                                                        kitcoll2p.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                                        kitcoll2p.Expression = "";// expression;
                                                        kitcoll2p.nameonly = item.ToString() + ".pdf";
                                                        listKits.Add(kitcoll2p);
                                                    }
                                                    else
                                                    {
                                                        error++;
                                                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (expression != "" && source == "IF")
                    {
                        if (docname == "file_IN" && medMCC.Length > 0)
                        {
                            if (medMCC != "MCC_16.pdf" && medMCC != "MCC_17.pdf" && medMCC != "MCC_18.pdf" && medMCC != "MCC_19.pdf")
                            {
                                if (File.Exists(pfsDir + @"\" + medMCC))
                                {
                                    fKitsAcctAcct kitcoll2q = new fKitsAcctAcct();
                                    kitcoll2q.prodfamily = row["Prodfamily"].ToString();
                                    kitcoll2q.DocumentDesc = row["document_desc"].ToString();
                                    kitcoll2q.Source = row["Source"].ToString();
                                    kitcoll2q.fileName = pfsDir + @"\" + medMCC;
                                    kitcoll2q.Expression = "";
                                    kitcoll2q.nameonly = medMCC;
                                    listKits.Add(kitcoll2q);
                                }
                                else
                                {
                                    error++;
                                    errorDesc = errorDesc + "File not found " + pfsDir + @"\" + medMCC + ".pdf  recnum " + Xrecnum + Environment.NewLine;
                                }
                            }
                        }
                        else
                        {
                            //errorDesc = errorDesc;
                        }
                    }
                    //var families = dbU.ExecuteScalar("select prodfamilies + '|' + Med_MCC from BCBS_MA_parse_eoc_Acct where recnum = " + Xrecnum);
                    else if (expression == "" && source == "INCLUDE IN ALL" && docname == "Premium_Agreement.pdf")
                    {
                        var partFname = dbU.ExecuteScalar("select qtr from BCBS_MA_parse_eoc_Acct where recnum = " + Xrecnum);
                        if (partFname.ToString().Length > 0 )
                        {
                            if (partFname.ToString() != "NA")
                            {
                                if (File.Exists(pfsDir + @"\" + partFname.ToString().Replace(" ", "_") + "_" + docname))
                                {
                                    fKitsAcctAcct kitcoll2r = new fKitsAcctAcct();
                                    kitcoll2r.prodfamily = row["Prodfamily"].ToString();
                                    kitcoll2r.DocumentDesc = row["document_desc"].ToString();
                                    kitcoll2r.Source = row["Source"].ToString();
                                    kitcoll2r.fileName = pfsDir + @"\" + partFname.ToString().Replace(" ", "_") + "_" + docname;
                                    kitcoll2r.Expression = "";
                                    kitcoll2r.nameonly = medMCC;
                                    listKits.Add(kitcoll2r);
                                }
                                else
                                {
                                    error++;
                                    errorDesc = errorDesc + "File not found " + pfsDir + @"\" + partFname.ToString().Replace(" ", "_") + docname + "_  recnum " + Xrecnum + Environment.NewLine;
                                }
                            }
                        }
                        else
                        {
                            error++;
                            errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + medMCC + "  recnum " + Xrecnum + Environment.NewLine;
                        }

                    }
                    else
                    {
                        error++;
                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + docname + "  expression " + expression + "   recnum " + Xrecnum + Environment.NewLine;
                    }
                }
            }
            if (errorDesc != "")
                errorDesc = errorDesc;
            return errorDesc;
            //if (error != 0)
            //   // assembly_General(filename, listKits, Xrecnum, netOutput, subdir);
            //else
            //    result = true;
            //return result;
        }

        public bool assembly_General(FileInfo filename, List<fKitsAcctAcct> kitcoll, string Xrecnum, string netOutput, string subdir, string acct_num)
        {
            bool result = false;
            PdfReader reader = null;
            Document sourceDocument = null;
            PdfCopy pdfCopyProvider = null;
            PdfImportedPage importedPage;
            string lookupPDF = "";
            if (filename.FullName.ToString() == @"C:\CierantProjects_dataLocal\wBCBS_MA\DEN-P\00006813_MEDX-F1_Let_162617__04-01-2017.pdf")
                result = false;
            ///summary = summary + Xrecnum + "|";

            string newfileName = netOutput + @"\" + subdir + @"\" + filename.Name.ToString();
            //filename.Directory.ToString().Replace("from_XMpie", @"toSCI") + @"\" + filename.Name.ToString();
            if (File.Exists(newfileName))
            {
                File.Delete(newfileName);
            }
            sourceDocument = new Document();
            pdfCopyProvider = new PdfCopy(sourceDocument, new System.IO.FileStream(newfileName, System.IO.FileMode.Create));

            //Open the output file
            sourceDocument.Open();
            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);

            try
            {
                int Count = arrayDocs.Count;
                //Loop through the files list
                foreach (var file in kitcoll)
                {
                   
                    string fname = file.fileName.ToString();
                    string expression = file.Expression.ToString();
                    dbU.ExecuteScalar("Insert into BCBS_MA_log_eoc_Acct_Kit_assembly(seq, recnum, filename, AssemblyDate,acct_num, prodfamily, FileNameAssembly, DocumentDesc, source, expression ) values(" +
                       SeqNum + "," + Xrecnum + ",'" + filename.Name + "',GETDATE(),'" + acct_num + "','" + file.prodfamily.ToString()  +
                       "','" + file.fileName.ToString() + "','" + file.DocumentDesc.ToString() + "','" + file.Source.ToString() + "','" + file.Expression.ToString() + "')");

                    SeqNum++;


                    if (file.Expression.ToString() == "")
                    {
                        summary = summary + file.nameonly.ToString() + "|";
                        int pages = get_pageCcount(file.fileName.ToString());
                        pages++;
                        reader = new PdfReader(file.fileName.ToString());
                        //Add pages of current file
                        try
                        {
                            for (int i = 1; i <= pages; i++)
                            {
                                importedPage = pdfCopyProvider.GetImportedPage(reader, i);
                                pdfCopyProvider.AddPage(importedPage);
                            }

                        }
                        catch (Exception ex)
                        {
                            var exception = "";
                            if (ex.Message.IndexOf("Invalid page") == 0)
                                exception = "";
                            else
                                exception = "";
                        }
                        reader.Close();

                        //Saving the PDF file name in a variable for later to retrieve the necessary data for reporting.
                        //lookupPDF = arrayDocs[f];

                    }
                    else
                    {
                        var resultQ = dbU.ExecuteScalar("select count(*) from BCBS_MA_parse_eoc_acct where recnum = " + Xrecnum + " and " + expression).ToString();
                        if (resultQ.ToString() != "0")
                        {
                            summary = summary + file.nameonly.ToString() + "|";
                            int pages = get_pageCcount(file.fileName.ToString());

                            reader = new PdfReader(file.fileName.ToString());
                            //Add pages of current file
                            try
                            {
                                for (int i = 1; i <= pages; i++)
                                {
                                    importedPage = pdfCopyProvider.GetImportedPage(reader, i);
                                    pdfCopyProvider.AddPage(importedPage);
                                }
                            }
                            catch (Exception ex)
                            {
                                var exception = "";
                                if (ex.Message.IndexOf("Invalid page") == 0)
                                    exception = "";
                                else
                                    exception = "";
                            }
                            reader.Close();
                        }
                    }
                }


            }
            catch (Exception ex)
            {
                var msg = ex.Message;
            }
            finally
            {
                //At the end save the output file  

                try
                {
                    sourceDocument.Close();
                }

                catch (Exception ex2)
                {
                    var msg2 = ex2.Message;
                }
            }
            dbU.ExecuteNonQuery("Update BCBS_MA_parse_eoc_acct set summaryPrint = '" + summary + "', dateAssemby = GETDATE(), XMPiePrinted = '" + subdir + "'  where recnum = " + Xrecnum);

            summary = "";
            return result;
        }
        private static int get_pageCcount(string file)
        {
            PdfReader reader = new PdfReader(file);
            int totP = reader.NumberOfPages;
            return totP;

            //using (StreamReader sr = new StreamReader(File.OpenRead(file)))
            //{
            //    Regex regex = new Regex(@"/Type\s*/Page[^s]");
            //    MatchCollection matches = regex.Matches(sr.ReadToEnd());

            //    return matches.Count;
            //}
        }
        public bool assembly_DN2(FileInfo filename)
        {
            bool result = false;

            return result;
        }
        public bool assembly_DN1(FileInfo filename)
        {
            bool result = false;

            return result;
        }


    }
}
