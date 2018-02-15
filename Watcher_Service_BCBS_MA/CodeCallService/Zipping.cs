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
    public class Zipping
    {
        CodeCallService.DBUtility dbU;

        string result = "";
        string pdfName = "";
        string txtFilename = "";
        string subdir = "";
        string zipname = "";
        int totrecs = 0;
        int zipnum = 1;
        int totpagesdOC = 1;
        int tDoc = 1;
        string rFilename = "";


        DataTable ControlData = new DataTable();
        

        public string zipECO()
        {
            string result = "";
            string subdir = "DEN-F";
            //result = zipDir(subdir);

            subdir = "MEDEX-F";
            result = zipDir(subdir);



            subdir = "MED-WEB-F";
            result = zipDir(subdir);


            subdir = "SNR";
            result = zipDir(subdir);

           
            return result;
        }
        public string zipECOAcct()
        {
            string result = "";
            string subdir = "ACCT";
            result = zipDirAcct(subdir);
             subdir = "ACCT-New";
            result = zipDirAcct(subdir);
            return result;
        }
            public string zipDir(string dir)
        {
            string result = "";
            totrecs = 0;
            zipnum = 1;
            tDoc = 1;
            DataTable ControlData = new DataTable();
            ControlData.Columns.Add("Recnum");
            ControlData.Columns.Add("FileName");
            ControlData.Columns.Add("Pages");
            ControlData.Columns.Add("TDoc");

            ControlData.Columns.Add("FName");
            ControlData.Columns.Add("LName");
            ControlData.Columns.Add("Addr1");
            ControlData.Columns.Add("Addr2");
            ControlData.Columns.Add("City");
            ControlData.Columns.Add("State");
            ControlData.Columns.Add("Zip");
            ControlData.Columns.Add("DL");

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);

            string prefix = "File_Acct_" + dir + "_" + DateTime.Now.ToString("yyyyMMdd") + "_";
            //string prefix = "File_" + dir + "_20171231_reprocess__";
            
            string[] daysWeek = GetWeekRange(DateTime.Now);
            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_EOC\";
            string netSource = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_EOC\" + dir;
           
            //zipname = netOutput + prefix + "_" + zipnum +  ".zip";
            var files = from fileName in
                          Directory.EnumerateFiles(netSource)
                        where fileName.ToLower().Contains(".pdf")
                        select fileName;
            foreach (var fileName in files)
            {
                FileInfo ff = new FileInfo(fileName);
                DataTable tabData = dbU.ExecuteDataTable("select FNAME, LNAME, ADDR1, ADDR2, CITY, STATE, ZIP10, dl from BCBS_MA_parse_eoc where recnum = '" + ff.Name.ToString().Substring(0, 8) + "'");

                int Docpages = get_pageCcount(fileName.ToString());

                rFilename = netOutput + prefix + zipnum.ToString("D5") + ".csv";

                DataRow _doc = ControlData.NewRow();
                _doc["FileName"] = ff.Name.ToString();
                _doc["recnum"] = ff.Name.ToString().Substring(0, 8);
                _doc["Pages"] = Docpages.ToString();
                _doc["TDoc"] = tDoc.ToString();
                if (tabData.Rows.Count == 1)
                {
                    DataRow row = tabData.Rows[0];
                    _doc["FName"] = row["Fname"].ToString();
                    _doc["LName"] = row["LName"].ToString();
                    _doc["Addr1"] = row["ADDR1"].ToString();
                    _doc["Addr2"] = row["ADDR2"].ToString();
                    _doc["City"] = row["City"].ToString();
                    _doc["State"] = row["State"].ToString();
                    _doc["Zip"] = row["ZIP10"].ToString();
                    _doc["Dl"] = row["DL"].ToString();

                }
                ControlData.Rows.Add(_doc);
                tDoc = tDoc + Docpages;
                totpagesdOC = totpagesdOC + Docpages;
                includeinZip(fileName,  ff.Name, prefix, netOutput, ControlData);
            }
            if (ControlData.Rows.Count > 0)
            {
                CreateCSV createfile = new CreateCSV();
                createfile.printCSV_fullProcess(rFilename, ControlData, "", "");
                

            }
            return result;
        }

        public string zipDirAcct(string dir)
        {
            string result = "";
            totrecs = 0;
            zipnum = 1;
            tDoc = 1;
            DataTable ControlData = new DataTable();
            //ControlData.Columns.Add("Recnum");
            //ControlData.Columns.Add("digUId");
            //ControlData.Columns.Add("FName");
            //ControlData.Columns.Add("artifactId");
            //ControlData.Columns.Add("LetterName");
            //ControlData.Columns.Add("CoverPageName");
            //ControlData.Columns.Add("CoverpageAddress1");
            //ControlData.Columns.Add("CoverpageAddress2");
            //ControlData.Columns.Add("CoverpageAddress3");
            //ControlData.Columns.Add("CoverpageAddress4");
            //ControlData.Columns.Add("City");
            //ControlData.Columns.Add("State");
            //ControlData.Columns.Add("Zip");
            //ControlData.Columns.Add("BRE");
            //ControlData.Columns.Add("TOD");
            //ControlData.Columns.Add("DL");
            ControlData.Columns.Add("Recnum");
            ControlData.Columns.Add("FileName");
            ControlData.Columns.Add("Pages");
            ControlData.Columns.Add("TDoc");

            ControlData.Columns.Add("FName");
            ControlData.Columns.Add("LName");
            ControlData.Columns.Add("Addr1");
            ControlData.Columns.Add("Addr2");
            ControlData.Columns.Add("City");
            ControlData.Columns.Add("State");
            ControlData.Columns.Add("Zip");
            ControlData.Columns.Add("DL");
            ControlData.Columns.Add("Directory");

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);

            string prefix = "File_" + dir + "_" + DateTime.Now.ToString("yyyyMMdd") + "_";
            string[] daysWeek = GetWeekRange(DateTime.Now);
            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_Acct\";
            string netSource = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_Acct\" + dir;

            //zipname = netOutput + prefix + "_" + zipnum +  ".zip";
            var files = from fileName in
                          Directory.EnumerateFiles(netSource)
                        where fileName.ToLower().Contains(".pdf")
                        select fileName;
            foreach (var fileName in files)
            {
                FileInfo ff = new FileInfo(fileName);
                DataTable tabData = dbU.ExecuteDataTable("select contact_1_name,contact_1_addr_1, contact_1_addr_2, contact_1_city, contact_1_state, contact_1_zip, DL, oDirectory from BCBS_MA_parse_eoc_Acct where recnum = '" + ff.Name.ToString().Substring(0, 8) + "'");

                int Docpages = get_pageCcount(fileName.ToString());

                rFilename = netOutput + prefix + zipnum.ToString("D5") + ".csv";

                DataRow _doc = ControlData.NewRow();
                _doc["FileName"] = ff.Name.ToString();
                _doc["recnum"] = ff.Name.ToString().Substring(0, 8);
                _doc["Pages"] = Docpages.ToString();
                _doc["TDoc"] = tDoc.ToString();
                if (tabData.Rows.Count == 1)
                {
                    DataRow row = tabData.Rows[0];
                    _doc["FName"] = row["contact_1_name"].ToString();
                    _doc["LName"] = "";
                    _doc["Addr1"] = row["contact_1_addr_1"].ToString();
                    _doc["Addr2"] = row["contact_1_addr_2"].ToString();
                    _doc["City"] = row["contact_1_city"].ToString();
                    _doc["State"] = row["contact_1_state"].ToString();
                    _doc["Zip"] = row["contact_1_zip"].ToString();
                    _doc["DL"] = row["DL"].ToString();
                    _doc["Directory"] = row["oDirectory"].ToString();
                }
                ControlData.Rows.Add(_doc);
                tDoc = tDoc + Docpages;
                totpagesdOC = totpagesdOC + Docpages;
                includeinZip(fileName, ff.Name, prefix, netOutput, ControlData);
            }
            if (ControlData.Rows.Count > 0)
            {
                CreateCSV createfile = new CreateCSV();
                createfile.printCSV_fullProcess(rFilename, ControlData, "", "");


            }
            return result;
        }
        public string zipECOAcct_old()
        {


            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);

            string drivesOk = "";
            string[] daysWeek = GetWeekRange(DateTime.Now);

            string netOutput = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_Acct\ACCT\Acctzips";
            


            string localOut = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Week_" + daysWeek[0].ToString() + @"\ToSCI_Acct\ACCT";
            System.IO.Directory.CreateDirectory(netOutput);
            System.IO.Directory.CreateDirectory(localOut);
                zipnum = 1;
                totrecs = 0;
                DataTable filestoZip = dbU.ExecuteDataTable("select XmpiFileName, filename, XMPiePrinted from BCBS_MA_parse_eoc_Acct where convert(date,dateAssemby) ='" + DateTime.Now.ToString("yyyy-MM-dd") + "' and len(XmpiFileName) > 0 order by XmpiFileName");
                foreach (DataRow rowz in filestoZip.Rows)
                {
                    pdfName = rowz[0].ToString() + ".pdf";
                    txtFilename = rowz[1].ToString();
                    subdir ="Acctzips";
                    zipname = "ACCT_" + zipnum + "_" + txtFilename.Substring(0, txtFilename.Length - 4) + ".zip";
                    includeinZipAcct(pdfName, subdir, zipname, netOutput, localOut, "ACCT");

                }
            

            return result;
        }

        public void includeinZipAcct(string pdfName, string subdir, string zipname, string netOutput, string localOut, string fam)
        {
            System.IO.Directory.CreateDirectory(localOut + @"\" + subdir);
            if (totrecs == 0)
            {
                try
                {
                    using (ZipArchive newFile = ZipFile.Open(netOutput + @"\" + zipname, ZipArchiveMode.Create))
                    {
                        newFile.CreateEntryFromFile(localOut + @"\" +  pdfName, pdfName);
                        totrecs++;
                    }
                }
                catch (Exception ex)
                {
                    result = result + netOutput + @"\" + subdir + @"\" + pdfName + "~" + fam + "~" + pdfName + Environment.NewLine;
                }

            }
            else
            {
                if (totrecs > 100)
                {
                    try
                    {
                        zipnum++;
                        zipname = fam + "_" + zipnum + "_" + txtFilename.Substring(0, txtFilename.Length - 4) + ".zip";
                        using (ZipArchive newFile = ZipFile.Open(netOutput + @"\" + zipname, ZipArchiveMode.Create))
                        {
                            newFile.CreateEntryFromFile(localOut + @"\" + pdfName, pdfName);
                        }
                        totrecs = 1;
                    }
                    catch (Exception ex)
                    {
                        result = result + netOutput + @"\" + subdir + @"\" + pdfName + "~" + fam + "~" + pdfName + Environment.NewLine;
                    }
                }
                else
                {
                    try
                    {
                        using (ZipArchive newFile = ZipFile.Open(netOutput + @"\" + zipname, ZipArchiveMode.Update))
                        {
                            newFile.CreateEntryFromFile(localOut + @"\" + pdfName, pdfName);

                        }
                        totrecs++;
                    }
                    catch (Exception ex)
                    {
                        result = result + netOutput + @"\" + subdir + @"\" + pdfName + "~" + fam + "~" + pdfName + Environment.NewLine;
                    }
                }
            }
        }


        public void includeinZip(string pdfName, string pdfNameOnly, string prefix, string netOutput, DataTable ControlData)
        {
            //System.IO.Directory.CreateDirectory(localOut + @"\" + subdir);
            if (totrecs == 0)
            {
                try
                {
                    zipname = netOutput + prefix +  zipnum.ToString("D5") + ".zip";
                    using (ZipArchive newFile = ZipFile.Open(zipname, ZipArchiveMode.Create))
                    {
                        newFile.CreateEntryFromFile(pdfName, pdfNameOnly);
                        totrecs++;
                    }
                }
                catch (Exception ex)
                {
                    result = result + zipname + "~" +"~" + pdfName + Environment.NewLine;
                }

            }
            else
            {
                if (totrecs > 90)  //200
                {
                    try
                    {
                       
                        using (ZipArchive newFile = ZipFile.Open(zipname, ZipArchiveMode.Update))
                        {
                            newFile.CreateEntryFromFile(pdfName, pdfNameOnly);
                        }

                        CreateCSV createfile = new CreateCSV();
                        createfile.printCSV_fullProcess(rFilename, ControlData, "", "");

                        ControlData.Clear();
                        tDoc = 1;
                        zipnum++;
                        zipname = netOutput + prefix + zipnum.ToString("D5") + ".zip";
                        totrecs = 1;
                        rFilename = netOutput + prefix + zipnum.ToString("D5") + ".csv";

                        using (ZipArchive newFile = ZipFile.Open(zipname, ZipArchiveMode.Create))
                        {
                           /// newFile.CreateEntryFromFile(pdfName, pdfNameOnly);
                        }

                    }
                    catch (Exception ex)
                    {
                        result = result + zipname + "~" + "~" + pdfName + Environment.NewLine;
                    }
                }
                else
                {
                    try
                    {
                        FileInfo fInfo = new FileInfo(zipname);
                        while (IsFileLocked(fInfo))
                        {
                            Thread.Sleep(500);
                        }


                        using (ZipArchive newFile = ZipFile.Open(zipname, ZipArchiveMode.Update))
                        {
                            newFile.CreateEntryFromFile(pdfName, pdfNameOnly);

                        }
                        totrecs++;
                    }
                    catch (Exception ex)
                    {
                        result = result + zipname + "~" + "~" + pdfName + Environment.NewLine;

                    }
                }
            }
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

    }
}
