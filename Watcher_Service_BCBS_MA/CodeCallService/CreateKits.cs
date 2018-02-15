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
    

    public class fKits
    {
        public string fileName { get; set; }
        
        public string Expression { get; set; }

        public string nameonly { get; set; }
    }
    
    public class CreateKits
    {
        CodeCallService.DBUtility dbU;

        string pfsDir = @"\\freenas\BCBSMA\PDF_Files\Non-Contract_SourceDocs";
        string RidersFolder = @"\\freenas\BCBSMA\PDF_Files\e__company_tradingpartners_Marketing_Comm_PDF";
        string unzipLocal = @"C:\CierantProjects_dataLocal\wBCBS_MA";

        string FileSummary = "";

        List<string> arrayDocs = new List<string>();

        string summary = "";

        public string generalProess_Kits()
        {
            string result = "";
            string batch = "";
            string subdir = "DEN-P";
            result = load_XmpieFiles(subdir);
            //subdir = "DEN-F";

           // result = load_XmpieFiles(subdir);
            subdir = "MEDEX-P";
            
            result = load_XmpieFiles(subdir);


            subdir = "MED-WEB-P";
            result = load_XmpieFiles(subdir);

            subdir = "MEDEX-F";
            result = load_XmpieFiles(subdir);


            subdir = "MED-WEB-F";
            result = load_XmpieFiles(subdir);


            subdir = "SNR";
            result = load_XmpieFiles(subdir);

            return result;
        }

        public string MultiDocProcess_Kits()
        {
            string result = "";
            string batch = "";
            string subdir = "DEN-P";
            result = assemblyMulty(subdir, batch);
            
            subdir = "MEDEX-P";
            //batch = "_B7_";
            result = assemblyMulty(subdir, batch);

          

            subdir = "MED-WEB-P";
            result = assemblyMulty(subdir);

          

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
            string  response = "";

            try
            {
               
                System.IO.Directory.CreateDirectory(unzipLocal + @"\" + sdir);

                System.IO.DirectoryInfo diLocal = new DirectoryInfo(unzipLocal + "\\" + sdir);
               
                    foreach (FileInfo file in diLocal.GetFiles())
                    {
                        file.Delete();
                    }
             
            }
            catch (Exception ex)
            {
                response = response + ex.Message;
            }

            string[] filesindirectory = Directory.GetDirectories(sourceXMPie + "\\" + sdir);
          
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
         
           
                foreach (FileInfo f in XmpieZIPs.GetFiles("*.zip")) 
                {

                if (f.Name.IndexOf("__") != 0)
                {
                    int errors = 0;
                    try
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
                    catch (Exception ex)
                    {
                        response = response + ex.Message;
                        errors++;
                    }
                    if (errors == 0)
                    {
                        string nfilename = f.Directory + "\\__" + f.Name;
                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(f.FullName, nfilename);

                    }
                }
            }
       
            return response;
        }
        public void CleanDirsFor_Kits()
        {
            string[] daysWeek = GetWeekRange(DateTime.Now);

            //string netOutput = @"\\freenas\BCBSMA\PRODUCTION\OUTBOUND\Week_" + daysWeek[0].ToString() + @"\ToSCI";
            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_EOC";
            System.IO.Directory.CreateDirectory(netOutput);
            ClearFolderAll(netOutput);

        }


        public string assemblyMulty(string dir, string oBatch = "")
        {
            string prefix = "File_" + dir + "_" + DateTime.Now.ToString("yyyyMMdd") +  oBatch;
            //string prefix = "File_" + dir + "_20171231_reprocess_";
            string[] daysWeek = GetWeekRange(DateTime.Now);
            string result = "";
            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_EOC\";
            string netSource = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_EOC\" + dir;
            int Fseq = 1;
            int tofDoc = 0;

            PdfReader reader = null;
            Document sourceDocument = null;
            PdfCopy pdfCopyProvider = null;
            PdfImportedPage importedPage;
            string lookupPDF = "";

            DataTable ControlData = new DataTable();
           
            ControlData.Columns.Add("Recnum");
            ControlData.Columns.Add("digUId");
            ControlData.Columns.Add("FName");
            ControlData.Columns.Add("artifactId");
            ControlData.Columns.Add("LetterName");
            ControlData.Columns.Add("CoverPageName");
            ControlData.Columns.Add("CoverpageAddress1");
            ControlData.Columns.Add("CoverpageAddress2");
            ControlData.Columns.Add("CoverpageAddress3");
            ControlData.Columns.Add("CoverpageAddress4");
            ControlData.Columns.Add("City");
            ControlData.Columns.Add("State");
            ControlData.Columns.Add("Zip");
            ControlData.Columns.Add("BRE");
            ControlData.Columns.Add("TOD");
            ControlData.Columns.Add("DL");

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            

            string newfileName = netOutput + prefix + Fseq.ToString("D5") + ".pdf";
            string rFilename = netOutput + prefix + Fseq.ToString("D5") + ".csv";
            
            if (File.Exists(newfileName))
            {
                File.Delete(newfileName);
            }
            sourceDocument = new Document();
            pdfCopyProvider = new PdfCopy(sourceDocument, new System.IO.FileStream(newfileName, System.IO.FileMode.Create));

            //Open the output file
            sourceDocument.Open();
            int totpagesdOC = 1;
            int tDoc = 1;
            try
            {

                var files = from fileName in
                           Directory.EnumerateFiles(netSource)
                            where fileName.ToLower().Contains(".pdf")
                            select fileName;
                foreach (var fileName in files)
                {
                    FileInfo ff = new FileInfo(fileName);

                    int Docpages = get_pageCcount(fileName.ToString());
                    DataTable tabData = dbU.ExecuteDataTable("select (FNAME + ' ' + LNAME) as name , ADDR1, ADDR2, CITY, STATE, ZIP10, dl from BCBS_MA_parse_eoc where recnum = '" + ff.Name.ToString().Substring(0, 8) + "'");

                    //FNAME, LNAME, ADDR1, ADDR2, CITY, STATE, ZIP10
                    DataRow _doc = ControlData.NewRow();
                    _doc["FName"] = prefix + Fseq.ToString("D5") + ".pdf";   //ff.Name.ToString();
                    _doc["recnum"] = ff.Name.ToString().Substring(0,8);
                    _doc["artifactId"] = Docpages.ToString();
                    _doc["TOD"] = tDoc.ToString();
                    if (tabData.Rows.Count == 1)
                    {
                        DataRow row = tabData.Rows[0];
                        _doc["CoverPageName"] = row["name"].ToString();
                        _doc["CoverpageAddress1"] = row["ADDR1"].ToString();
                        _doc["CoverpageAddress2"] = row["ADDR2"].ToString();
                        _doc["CoverpageAddress3"] = "";
                        _doc["CoverpageAddress4"] = "";
                        _doc["City"] = row["City"].ToString();
                        _doc["State"] = row["State"].ToString();
                        _doc["Zip"] = row["ZIP10"].ToString();
                        _doc["DL"] = row["DL"].ToString();

                    }
                    ControlData.Rows.Add(_doc);
                    tDoc = tDoc + Docpages;

                    totpagesdOC = totpagesdOC + Docpages;
                    reader = new PdfReader(fileName.ToString());
                    //Add pages of current file
                    try
                    {
                        PdfSmartCopy copy;
                        for (int i = 1; i <= Docpages; i++)
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

                    if (totpagesdOC > 7900)
                    {

                        


                        tDoc = 1;
                        totpagesdOC = Docpages;
                        sourceDocument.Close();

                       // smartCopy_pdf(newfileName);

                        CreateCSV createfile = new CreateCSV();
                        createfile.printCSV_fullProcess(rFilename, ControlData, "", "");

                        ControlData.Rows.Clear();
                        Fseq++;
                        newfileName = netOutput + prefix + Fseq.ToString("D5") + ".pdf";
                        rFilename = netOutput + prefix + Fseq.ToString("D5") + ".csv";

                        if (File.Exists(newfileName))
                        {
                            File.Delete(newfileName);
                        }
                        sourceDocument = new Document();
                        pdfCopyProvider = new PdfCopy(sourceDocument, new System.IO.FileStream(newfileName, System.IO.FileMode.Create));

                        sourceDocument.Open();

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
            if(ControlData.Rows.Count > 0)
            {
                //smartCopy_pdf(newfileName);
                CreateCSV createfile = new CreateCSV();
                createfile.printCSV_fullProcess(rFilename, ControlData, "", "");

            }
            summary = "";
            return result;

        }
        public void smartCopy_pdf(string fileName)
        {
            FileInfo fileI = new FileInfo(fileName);
            File.Move(fileName,fileI.DirectoryName + "\\w_" + fileI.Name);
            try
            {

                PdfReader reader = null;
                

                string pdfTemplatePath = fileI.DirectoryName;
                reader = new PdfReader(fileI.DirectoryName + "\\w_" + fileI.Name);
                //PdfCopy copy;
                PdfSmartCopy copy;
                Document d1 = new Document();
                copy = new PdfSmartCopy(d1, new FileStream(pdfTemplatePath + "\\" + fileI.Name, FileMode.Create));
                d1.Open();
                copy.AddPage(copy.GetImportedPage(reader, 1));
                d1.Close();


                for (int i = 2; i < reader.NumberOfPages; i++)
                {
                    Document d2 = new Document();
                    
                    copy = new PdfSmartCopy(d2, new FileStream(pdfTemplatePath + "\\" + fileI.Name, FileMode.Append));
                    d2.Open();
                    copy.AddPage(copy.GetImportedPage(reader, i));
                    d2.Close();
                }
            }

            catch (Exception ex2)
            {
                var msg2 = ex2.Message;
            }
            
        }
            public string load_XmpieFiles(string sdir)
        {

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteNonQuery("Update BCBS_MA_parse_eoc set summaryPrint = ''"); // where filename = '" + file.Name + "'");
             string drivesOk = "";
            string[] daysWeek = GetWeekRange(DateTime.Now);

            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_EOC";
            //System.IO.Directory.Delete(netOutput,true);
            //ClearFolder(netOutput, sdir);


            System.IO.Directory.CreateDirectory(netOutput);
             string sourceXMPie = @"\\freenas\BCBSMA\PRODUCTION\XMPIE";
             string resultUnzip = expandXMPie(sourceXMPie, netOutput, sdir);
             if (resultUnzip == "")
             {
                 appSets checkD = new appSets();
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
                                    Directory.EnumerateFiles(unzipLocal + @"\" + sdir, "*.pdf",SearchOption.AllDirectories)   
                                 where fileName.ToLower().Contains(".pdf")
                                 select fileName;
                    //// (ProcessVars.dataEOC + @"\from_XMpie")
                    int totfiles = 0;
                    foreach (var fileName in files)
                     {
                         FileInfo file = new FileInfo(fileName);
                        string subdir = file.Directory.ToString().Remove(0, file.Directory.ToString().LastIndexOf('\\') + 1);
                         resultKit = assembly_KITS(fileName, netOutput, subdir);
                        totfiles++;
                         if (resultKit == "")
                         {
                             //string nfilename = file.Directory + "\\__" + file.Name;
                             //if (File.Exists(nfilename))
                             //    File.Delete(nfilename);
                             //File.Move(file.FullName, nfilename);
                             //drivesOk = drivesOk + resultKit + " " + file.Name + Environment.NewLine;
                         }

                     }
                    if (totfiles > 0)
                    {
                        string repFilename = netOutput + @"\AssemblySummary_" + sdir + "_" + DateTime.Now.ToString("yyyy_MM_dd") + ".csv";
                        DataTable printed = dbU.ExecuteDataTable("select filename, recnum, cov_code, UACOVCODE1, summaryPrint from BCBS_MA_parse_eoc where convert(date,dateAssemby) ='" + DateTime.Now.ToString("yyyy-MM-dd") + "' and XMPiePrinted = '" + sdir + "' order by filename, recnum");
                        CreateCSV createfile = new CreateCSV();
                        createfile.printCSV_fullProcess(repFilename, printed, "", "");
                    }
                 }

             }
             else
                 drivesOk = "No files";

            return drivesOk;
        }
        public string assembly_KITS(string filename, string netOutput, string subdir)
        {
            string resut = ""; string Xrecnum = ""; string Xkit_Type = "";
            FileInfo fileInfo = new System.IO.FileInfo(filename);
            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);

            string[] fInfo = fileInfo.Name.Split('_');
            Xrecnum = fInfo[0].ToString();
            Xkit_Type = fInfo[1].ToString().ToUpper().Replace("-", "");
            string strsql = "select recnum from BCBS_MA_parse_eoc where recnum = " + Xrecnum + " and SpecialSupress is null";
            DataTable oktoProcess = dbU.ExecuteDataTable(strsql);
            if (oktoProcess.Rows.Count == 1)
            {
                switch (Xkit_Type)
                {
                    case "DENP1":
                    case "DENP2":
                    case "DENP3":
                    case "MEDP1":
                    case "MEDXP1":
                    case "MEDXCP1":
                    case "MEDXF1":
                    case "MEDXCF1":
                    case "SNRF1":
                        //assembly_DN1(fileInfo);
                        assembly_IncludeInAll(fileInfo, fInfo[1].ToString().ToUpper(), Xrecnum, netOutput, subdir);
                        break;

                    default:
                        //assembly_DN3(fileInfo, fInfo[2].ToString().ToUpper());
                        break;
                }
            }

            else
                resut = "";
            return resut;
        }
        public bool assembly_IncludeInAll(FileInfo filename, string type, string Xrecnum, string netOutput, string subdir)
        {
            if (subdir == "MEDX-P1")
            {

                var here = "";
            }
            int numElements = 0;
            bool result = false;
            int error = 0;
            string errorDesc = "";
            arrayDocs.Clear();
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);


            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            DataTable filesforDN3 = dbU.ExecuteDataTable("select document, Expression, Source from Master_KitType where [Kit_Type Value] = '" + type
                + "' and (source = 'INCLUDE IN ALL' or source = 'IF' or Source = 'IF_SOME_R'  or Source = 'IF_SOME_F') order by [Order]");
            arrayDocs.Add(filename.FullName.ToString());


            List<fKits> listKits = new List<fKits>();


            fKits kitcoll1 = new fKits();
            kitcoll1.fileName = filename.FullName.ToString();
            kitcoll1.Expression = "";
            kitcoll1.nameonly = filename.Name.ToString();
            listKits.Add(kitcoll1);

            //NameValueCollection kitCollection = new NameValueCollection();
            //kitCollection.Add(filename.FullName.ToString(), "");

            foreach (DataRow row in filesforDN3.Rows)
            {
                string docname = row[0].ToString();
                string expression = row[1].ToString();
                string source = row[2].ToString();

                if (File.Exists(pfsDir + @"\" + docname))
                {
                    if (source == "INCLUDE IN ALL")
                    {
                        fKits kitcoll = new fKits();
                        kitcoll.fileName = pfsDir + @"\" + docname;
                        kitcoll.Expression = expression;
                        kitcoll.nameonly = docname;
                        listKits.Add(kitcoll);
                        // kitCollection.Add(pfsDir + @"\" + docname, expression);
                    }


                    else if (expression != "" && source == "IF")
                    {
                        DataTable conditiontrue = dbU.ExecuteDataTable("select files_Riders from BCBS_MA_parse_eoc where recnum = " + Xrecnum + " and " + expression);
                        if (conditiontrue.Rows.Count > 0)
                        {
                            fKits kitcoll = new fKits();
                            kitcoll.fileName = pfsDir + @"\" + docname;
                            kitcoll.Expression = "";
                            kitcoll.nameonly = docname;
                            listKits.Add(kitcoll);
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
                        
                        var RiderFiles = dbU.ExecuteScalar("select files_Riders from BCBS_MA_parse_eoc where recnum = " + Xrecnum );
                        if (RiderFiles.ToString().Length > 1)
                        {
                            if (!RiderFiles.ToString().Contains("~"))
                            {
                                if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                {
                                    fKits kitcoll = new fKits();
                                    kitcoll.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                    kitcoll.Expression = "";
                                    kitcoll.nameonly = RiderFiles.ToString() + ".pdf";
                                    listKits.Add(kitcoll);
                                }
                                else
                                {
                                    error++;
                                    errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf" + Environment.NewLine;
                                }
                            }
                            else
                            {
                                string[] fileR = RiderFiles.ToString().Split('~');
                                foreach (string item in fileR)
                                {
                                    if (item.Length > 1)
                                    {
                                        if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                        {
                                            fKits kitcoll = new fKits();
                                            kitcoll.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                            kitcoll.Expression = "";
                                            kitcoll.nameonly = item.ToString() + ".pdf";
                                            listKits.Add(kitcoll);
                                        }
                                        else
                                        {
                                            error++;
                                            errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf" + Environment.NewLine;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (source == "IF_SOME_F")
                    {
                        dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
                        var RiderFiles = dbU.ExecuteScalar("select files_pcp_Flier from BCBS_MA_parse_eoc where recnum = " + Xrecnum );
                        if (RiderFiles.ToString().Length > 1)
                        {
                            if (!RiderFiles.ToString().Contains("~"))
                            {
                                if (File.Exists(RidersFolder + @"\" + RiderFiles.ToString() + ".pdf"))
                                {
                                    fKits kitcoll = new fKits();
                                    kitcoll.fileName = RidersFolder + @"\" + RiderFiles.ToString() + ".pdf";
                                    kitcoll.Expression = "";
                                    kitcoll.nameonly = RiderFiles.ToString() + ".pdf";
                                    listKits.Add(kitcoll);
                                }
                                else
                                {
                                    error++;
                                    errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + RiderFiles.ToString() + ".pdf" + Environment.NewLine;
                                }
                            }
                            else
                            {
                                string[] fileR = RiderFiles.ToString().Split('~');
                                foreach (string item in fileR)
                                {
                                    if (item.Length > 1)
                                    {
                                        if (File.Exists(RidersFolder + @"\" + item.ToString() + ".pdf"))
                                        {
                                            fKits kitcoll = new fKits();
                                            kitcoll.fileName = RidersFolder + @"\" + item.ToString() + ".pdf";
                                            kitcoll.Expression = "";
                                            kitcoll.nameonly = item.ToString() + ".pdf";
                                            listKits.Add(kitcoll);
                                        }
                                        else
                                        {
                                            error++;
                                            errorDesc = errorDesc + "File not found " + RidersFolder + @"\" + item.ToString() + ".pdf" + Environment.NewLine;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    else
                    {
                        error++;
                        errorDesc = errorDesc + "File not found " + RidersFolder + @"\"+ docname + "  expression "  + expression + " recnum " + Xrecnum;
                    }
                }
            }
            if (error == 0)
                assembly_General(filename, listKits, Xrecnum, netOutput, subdir);
            else
                result = true;
            return result;
        }

        public bool assembly_General(FileInfo filename, List<fKits> kitcoll, string Xrecnum, string netOutput, string subdir)
        {
            bool result = false;
            PdfReader reader = null;
            Document sourceDocument = null;
            PdfCopy pdfCopyProvider = null;
            PdfImportedPage importedPage;
            string lookupPDF = "";
            if(filename.FullName.ToString() == @"C:\CierantProjects_dataLocal\wBCBS_MA\DEN-P\00006813_MEDX-F1_Let_162617__04-01-2017.pdf")
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

            try
            {
                int Count = arrayDocs.Count;
                //Loop through the files list
                foreach (var file in kitcoll)
                {
                    string fname = file.fileName.ToString();
                    string expression = file.Expression.ToString();
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
                        var resultQ = dbU.ExecuteScalar("select count(*) from BCBS_MA_parse_eoc where recnum = " + Xrecnum + " and " + expression).ToString();
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
            dbU.ExecuteNonQuery("Update BCBS_MA_parse_eoc set summaryPrint = '" + summary + "', dateAssemby = GETDATE(), XMPiePrinted = '" + subdir + "'  where recnum = " + Xrecnum);

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
