using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using System.Xml;
using System.Xml.Linq;
using System.Threading.Tasks;

namespace Horizon_EOBS_Parse
{
    public class Parse_IDCards
    {
        DBUtility dbU;
        string errors = "";
        int errorcount = 0;
        int Recnum = 1;
        int GRecnum = 1;
        int currLine = 0;
        int seqBundle = 0;
          DataTable DataTable = Data_Table();
          DataTable SummaryDataTable = Data_TableS();
          List<string> Codes = new List<string>();
          List<string> CovCodes = new List<string>();

          string strGroup,strPrevGroup, GroupBundle, Medicare, Source_M;


        public string ProcessFiles(string dateProcess)
        {
            ProcessVars.serviceIsrunning = true;
            //autoEvent.WaitOne(1000 * 60 * 3, false);
            string result = FilestoProcess_CON2(dateProcess, "");
             result = result + " " + FilestoProcess_GRP2(dateProcess, "");
            ProcessVars.serviceIsrunning = false;

            return result;
        }
        public string ProcessFilesinDir(string dateProcess, string LocalDirectory)
        {
            string result = "";
            if (Directory.Exists(LocalDirectory))
            {
                string[] subdirectoryEntries = Directory.GetDirectories(LocalDirectory);

                foreach (string subdirectory in subdirectoryEntries)
                {
                    if (subdirectory.IndexOf("\\_") == -1)
                    {
                        //check file name not saubdirectory
                        if (subdirectory.IndexOf("CON2") != -1 && subdirectory.IndexOf("_CON2") == -1)
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_CON2(dateProcess, subdirectory);

                        if (subdirectory.IndexOf("GRP2") != -1 && subdirectory.IndexOf("_GRP2") == -1)
                            if (Directory.Exists(subdirectory))
                                result = result + " " + FilestoProcess_GRP2(dateProcess, subdirectory);
                        if (subdirectory.IndexOf("Bed Bath and Beyond") != -1 && subdirectory.IndexOf("_Bed Bath and Beyond_") == -1)
                            if (Directory.Exists(subdirectory))
                                result = result + " " + FilestoProcess_BBB(dateProcess, subdirectory, true);
                        if (subdirectory.IndexOf("Heavy and General Laborers") != -1 && subdirectory.IndexOf("_Heavy and General Laborers_") == -1)
                            if (Directory.Exists(subdirectory))
                                result = result + " " + FilestoProcess_BBB(dateProcess, subdirectory, false);
                        //TEST AREA

                        if ((subdirectory.IndexOf("OMNIA_") != -1 && subdirectory.IndexOf("_") != 0 && subdirectory.IndexOf("GRP2") == -1) ||
                            (subdirectory.IndexOf("OMNIA_") != -1 && subdirectory.IndexOf("_") != 0 && subdirectory.IndexOf("CON2") == -1))
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_OMNIA(dateProcess, subdirectory);
                        //HOSHBP_
                        if (subdirectory.IndexOf("SHBP_") != -1 && subdirectory.IndexOf("_") != 0)
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_OMNIA(dateProcess, subdirectory);
                        if (subdirectory.IndexOf("Elaine_") != -1 && subdirectory.IndexOf("_") != 0)
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_OMNIA(dateProcess, subdirectory);
                    }
                }

                //ProcessVars.serviceIsrunning = true;
                ////autoEvent.WaitOne(1000 * 60 * 3, false);
                // result = FilestoProcess_CON2(dateProcess);
                //result = result + " " + FilestoProcess_GRP2(dateProcess);
                //ProcessVars.serviceIsrunning = false;
            }
            return result;
        }
        public string FilestoProcess(string dateProcess)        //not maintenance
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.NLDirectory))
            {
                string[] subdirectoryEntries = Directory.GetDirectories(ProcessVars.IDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\");
                string[] subdirectoryEntries2 = Directory.GetDirectories(subdirectoryEntries[0].ToString());
                foreach (string subdirectory in subdirectoryEntries2)
                {
                    DirectoryInfo originalDATs = new DirectoryInfo(subdirectory);
                    FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                    foreach (FileInfo file in FilesDAT)
                    {
                        InsertName = "";
                        DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        if (FilesPDF[0].ToString().ToUpper().IndexOf("PDF") != -1)
                        {

                            InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));

                        }
                        //FileInfo[] getPDF = 
                        if (file.Name.IndexOf("IM") == -1)
                        {
                            try
                            {
                                string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString(), false, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }

                    }
                }

            }
            return errors;
        }
        public string FilestoProcess_OMNIA(string dateProcess, string subdirectory)        //not maintenance
        {
            string InsertName = "";

            //if (Directory.Exists(ProcessVars.InputDirectory))
            if (Directory.Exists(subdirectory))
            {
                //string dirIDCards = ProcessVars.InputDirectory  + @"from_FTP\Con2\";
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");

                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {
                        //DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        DirectoryInfo originalPDFs = new DirectoryInfo(subdirectory + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        InsertName = "";
                        foreach (FileInfo file2 in FilesPDF)
                        {
                            if (file2.ToString().ToUpper().Replace(".PDF", "").IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (file2.ToString().IndexOf("_") > 0 && file2.ToString().IndexOf("_") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf("_"));

                                else if (file2.ToString().IndexOf(" ") > 0 && file2.ToString().IndexOf(" ") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf(" "));

                                //if (FilesPDF[0].ToString().IndexOf(" ") > 0 && FilesPDF[0].ToString().IndexOf(" ") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));
                                //if (FilesPDF[0].ToString().IndexOf("_") > 0 && FilesPDF[0].ToString().IndexOf("_") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf("_"));
                            }
                        }
                        //FileInfo[] getPDF = 

                        try
                        {
                            //string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString());
                            string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, "", false);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                }

                else
                {
                    errors = errors + " More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();

                }

            }
            return errors;
        }
        public string FilestoProcess_CON2(string dateProcess, string subdirectory)        //not maintenance
        {
            string InsertName = "";

            //if (Directory.Exists(ProcessVars.InputDirectory))
            if (Directory.Exists(subdirectory))
            {
                //string dirIDCards = ProcessVars.InputDirectory  + @"from_FTP\Con2\";
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {
                        //DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        //=====================================================================================
                        InsertName = "";
                        DirectoryInfo originalPDFs = new DirectoryInfo(subdirectory + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        foreach (FileInfo file2 in FilesPDF)
                        {
                            if (file2.ToString().ToUpper().Replace(".PDF", "").IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (file2.ToString().IndexOf("_") > 0 && file2.ToString().IndexOf("_") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf("_"));

                                else if (file2.ToString().IndexOf(" ") > 0 && file2.ToString().IndexOf(" ") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf(" "));
                            }

                        }
                        //FileInfo[] getPDF = 
                        if (file.Name.IndexOf("CON2") == 0)
                        {
                            try
                            {
                                //string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString());
                                string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                    }

                }
                else
                {
                    var errorDAT = "More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();

                }

            }
            return errors;
        }
        public string FilestoProcess_GRP2(string dateProcess, string subdirectory)        //not maintenance
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.InputDirectory))
            {
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {
                        InsertName = "";

                        DirectoryInfo originalPDFs = new DirectoryInfo(subdirectory + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        foreach (FileInfo file2 in FilesPDF)
                        {

                            if (file2.ToString().ToUpper().Replace(".PDF", "").IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (file2.ToString().IndexOf("_") > 0 && file2.ToString().IndexOf("_") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf("_"));
                                else if (file2.ToString().IndexOf(" ") > 0 && file2.ToString().IndexOf(" ") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf(" "));
                                //if (FilesPDF[0].ToString().IndexOf("_") > 0 && FilesPDF[0].ToString().IndexOf("_") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf("_"));
                                //else if (FilesPDF[0].ToString().IndexOf(" ") > 0 && FilesPDF[0].ToString().IndexOf(" ") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));
                            }
                        }
                        if (file.Name.IndexOf("GRP2") == 0)
                        {
                            try
                            {
                                seqBundle = 0;
                                string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                }
                else
                {
                    var errorDAT = "More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();
                }
            }
            return errors;
        }

        public string FilestoProcess_BBB(string dateProcess, string subdirectory, bool isBBB)        //Bed Bath and Beyond
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.InputDirectory))
            {
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");

                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {
                        string insertBBBLocation = "";
                        InsertName = "";
                        if (isBBB)
                        {
                            DirectoryInfo originalPDFs = new DirectoryInfo(ProcessVars.oInsertBBB);
                            FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                            foreach (FileInfo file2 in FilesPDF)
                            {
                                InsertName = file2.Name.ToString().Substring(0, file2.Name.ToString().IndexOf("_"));
                                insertBBBLocation = file2.FullName;
                            }
                        }
                        try
                        {
                            seqBundle = 0;
                            string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, insertBBBLocation, false);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                }

                else
                {
                    var errorDAT = "More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();

                }

            }
            return errors;
        }

        public string MaintenanceFilestoProcess(string dateProcess)   //maintenance
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.NLDirectory))
            {
                string dirIDCards = ProcessVars.IDCardsMDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                string[] subdirectoryEntries = Directory.GetDirectories(dirIDCards);
                //string[] subdirectoryEntries2 = Directory.GetDirectories(subdirectoryEntries[0].ToString());
                foreach (string subdirectory in subdirectoryEntries)
                {
                    DirectoryInfo originalDATs = new DirectoryInfo(subdirectory);
                    FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                    foreach (FileInfo file in FilesDAT)
                    {
                        DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        foreach (FileInfo file2 in FilesPDF)
                        {
                            if (file2.ToString().ToUpper().IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (FilesPDF[0].ToString().ToUpper().IndexOf("PDF") != -1)
                                {
                                    if (FilesPDF[0].ToString().IndexOf(" ") > 0 && FilesPDF[0].ToString().IndexOf(" ") < (FilesPDF[0].ToString().Length - 3))
                                        InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));
                                    if (FilesPDF[0].ToString().IndexOf("_") > 0 && FilesPDF[0].ToString().IndexOf("_") < (FilesPDF[0].ToString().Length - 3))
                                        InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf("_"));
                                }
                            }
                        }
                        //FileInfo[] getPDF = 
                        if (file.Name.IndexOf("IM") == -1)
                        {
                            try
                            {
                                string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString(), true, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";

                                InsertName = "";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }

                    }
                }

            }
            return errors;
        }
        public string evaluate_IDCards(string fileName, string insertName, string directoryTXT, bool nozip, string insertBBB, bool testProcess)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string fileZipInfoff = "";
            Recnum = 1;
            //DataTable pullOuts = dbU.ExecuteDataTable("select code from HOR_parse_ID_Cards_Pull where CONVERT(DATE,InputDate)='" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'");
            DataTable pullOuts = dbU.ExecuteDataTable("select code from HOR_parse_ID_Cards_Pull where CONVERT(DATE,InputDate) > '2016-02-28' and processDate is null");

            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            string[] files = System.IO.Directory.GetFiles(fileInfo.DirectoryName, "*.zip");
            DateTime lastUpdate = DateTime.Now;
            foreach (string item in files)
            {
                FileInfo fileZipInfo = new System.IO.FileInfo(item);
                try
                {
                    fileZipInfoff = fileZipInfo.Name;
                    lastUpdate = (DateTime)dbU.ExecuteScalar("select importdate_start from HOR_parse_files_downloaded where filename = '" + fileZipInfo.Name + "'");
                }
                catch (Exception ex)
                {
                    var wrecnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
                    int wSeqnum = 0;
                    if (wrecnum.ToString() == "")
                        wSeqnum = 1;
                    else
                        wSeqnum = Convert.ToInt32(wrecnum.ToString()) + 1;


                    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                         wSeqnum + ",'" + fileZipInfo.Name + "','zip',1,' ','" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" + DateTime.Now.ToString("yyyy-MM-dd") + "',1)");


                    var res = ex.Message;
                }
            }


            if (!File.Exists(ProcessVars.gmappingFile))
                throw new Exception("Mapping file not found.");

            List<List<Field>> records =
                ParseFile(fileName);

            DataTable table = new DataTable();

            List<Field> fields = GetFields();
            foreach (Field field in fields)
            {

                table.Columns.Add(field.Name);
            }

            foreach (List<Field> record in records)
            {
                var row = table.NewRow();

                foreach (Field field in record)
                {
                    row[field.Name] = field.Value;

                }
                table.Rows.Add(row);
            }
            foreach (DataRow row in table.Rows) // Loop over the rows.
            {
                if ((row[12].ToString().Trim() == "" && row[13].ToString().Trim() == ""))
                {
                    if (row[11].ToString().Trim() != "")
                    {
                        row[12] = row[11].ToString().Trim();
                        row[11] = "";
                    }
                    else if (row[10].ToString().Trim() != "")
                    {
                        row[12] = row[10].ToString().Trim();
                        row[10] = "";
                    }
                }
                if (row[4].ToString().Trim() == "")
                {
                    row[4] = row[5].ToString().Trim() + " " + row[6].ToString().Trim() + " " + row[7].ToString().Trim();
                }
            }
            table.Columns.Add("Recnum");
            table.Columns.Add("Timestamp");
            table.Columns.Add("Insert_Prev");
            table.Columns.Add("Insert");
            table.Columns.Add("Grp_Bundle");
            table.Columns.Add("DL");
            table.Columns.Add("Med_Flag");
            table.Columns.Add("GroupChain");
            table.Columns.Add("Type");
            table.Columns.Add("GroupChainCount");

            DateTime dt = DateTime.Now;
            string s = dt.ToString("yyyyMMddHHmmss");


            bool isOmnia = fileInfo.Name.ToUpper().Contains("OMNIA") ? true : false;
            bool isTMED = fileInfo.Name.ToUpper().Contains("TMED") ? true : false;
            foreach (DataRow row in table.Rows) // Loop over the rows.
            {

                row["Recnum"] = GRecnum;
                GRecnum++;
                row["Timestamp"] = s;
                if (row["Admin Name"].ToString().IndexOf("SEND WITH PREVIOUS CARD") == 0)
                    row["Insert_Prev"] = "Y";
                else
                    row["Insert_Prev"] = "N";
                row["Insert"] = insertName;   // "CMC0007154(0415)";   external parameter
                //row["Grp_Bundle"] = "N";
                row["DL"] = "";
                //row["Med_Flag"] = "N";
                row["GroupChain"] = row["CodeGrp1"].ToString().Trim() + "-" + row["CodeGrp2"].ToString().Trim();
                string type = row["GroupID"].ToString().Substring(2, 4);
                string ResultType = "";
                if (type == "2633")
                    ResultType = "S";
                if (isTMED)
                {
                    ResultType = "T";
                }
                if (isOmnia)
                {
                    if (type != "2633")
                    {
                        ResultType = "O";
                        row["Insert"] = "CMC0007287d (1215)";
                    }
                }
                else
                {
                    if (type == "2169" || type == "2235")

                        ResultType = "B";
                }
                row["Type"] = ResultType;

            }
            //==    print temp data===================================================================
            createCSV createcsvT = new createCSV();
            string pNameT = fileName.Substring(0, fileName.Length - 4) + "_data.csv";
            if (File.Exists(pNameT))
                File.Delete(pNameT);
            var fieldnamesT = new List<string>();
            for (int index = 0; index < table.Columns.Count; index++)
            {
                fieldnamesT.Add(table.Columns[index].ColumnName);
            }
            bool respT = createcsvT.addRecordsCSV(pNameT, fieldnamesT);

            table.Columns.Add("File_Source");
            string File_Source = "";
            string justFileName = fileInfo.Name.Substring(0, (fileInfo.Name.ToString().Length - fileInfo.Extension.ToString().Length));
            //if (justFileName.Length > 20)
            //    File_Source = justFileName.Substring(0, 20);
            //else
            //    File_Source = justFileName;

            foreach (DataRow row in table.Rows)
            {
                row["File_Source"] = justFileName;
                var rowData = new List<string>();
                for (int index = 0; index < table.Columns.Count; index++)
                {
                    rowData.Add(row[index].ToString());
                }
                respT = false;
                respT = createcsvT.addRecordsCSV(pNameT, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            //========================================================================



            getfilesTXT(directoryTXT, fileZipInfoff, fileInfo.Name);


            string test = "";
            if (DataTable.Rows.Count > 0)
            {

                foreach (DataRow row in table.Rows)
                {

                    if (row["GroupChain"].ToString().Trim() != "")
                    {
                        if (row["Member ID"].ToString().Trim() == "3HZN37974380")
                        {

                            test = "";
                        }
                        DataRow[] result = DataTable.Select("Code= '" + row["Member ID"].ToString().Trim() + "' AND Group = '" + row["GroupChain"].ToString().Trim() + "'");

                        if (result.Count() > 0)
                        {
                            if (result.Count() == 1)
                            {

                                foreach (DataRow rowR in result)
                                {
                                    DataRow[] resultSummary = SummaryDataTable.Select("SeqBundle= '" + rowR["SeqBundle"].ToString().Trim() + "'");
                                    if (rowR["MarkUnique"].ToString() == "")
                                    {
                                        row["Grp_Bundle"] = rowR[3];
                                        row["Med_Flag"] = rowR[4];

                                    }
                                    else
                                    {
                                        test = "";
                                    }
                                    if (resultSummary.Count() == 1)
                                        foreach (DataRow rowRs in resultSummary)
                                        {
                                            row["GroupChainCount"] = rowRs[7].ToString();
                                        }
                                }

                            }
                            else
                            {
                                test = "";
                                foreach (DataRow rowR in result)
                                {
                                    test = test + "  " + rowR[0];

                                }
                                test = test + "____" + row["Member ID"].ToString().Trim() + " " + row["GroupChain"].ToString().Trim() + "\n\n";
                            }

                        }

                    }
                }
                DataTable summary = table.Clone();
                foreach (DataRow drtableOld in table.Rows)
                {
                    if (drtableOld["GroupChainCount"].ToString() != "")
                    {
                        if (Int32.Parse(drtableOld["GroupChainCount"].ToString()) > 1)
                        {
                            summary.ImportRow(drtableOld);

                        }
                    }
                }

                if (summary.Rows.Count > 0)
                {
                    for (int x = 33; x > 29; x--)
                        summary.Columns.RemoveAt(x);
                    for (int x = 28; x > 14; x--)
                        summary.Columns.RemoveAt(x);
                    //for (int x = 9; x > 5; x--)
                    //    summary.Columns.RemoveAt(x);
                    for (int x = 3; x >= 0; x--)
                        summary.Columns.RemoveAt(x);
                    //DataTable uniqueTable = summary.DefaultView.ToTable(true, "Grp_Bundle");
                    string prevGbundle = "";
                    DataTable uniqueTable = summary.Clone();
                    foreach (DataRow drtableOld in summary.Rows)
                    {
                        if (drtableOld["grp_bundle"].ToString() != prevGbundle)
                        {

                            uniqueTable.ImportRow(drtableOld);
                            prevGbundle = drtableOld["grp_bundle"].ToString();

                        }
                    }



                   
                }

                //===================================

                DataTable tableNew = table.Clone();

                //foreach (DataRow r in table.Rows)  //loop through the columns. 
                //{
                //    //string val = row.ColumnName;
                //    DataRow[] result = DataTable.Select("Code= '" + r["Member ID"].ToString().Trim() + "' AND CovCode = '" + r["Code2"].ToString().Trim() + "'");
                //    foreach (DataRow row in result)
                //    {
                //        Console.WriteLine("{0}, {1}", row[0], row[1]);
                //    }


                //}
                foreach (DataRow drtableOld in table.Rows)
                {
                    if (drtableOld["Member ID"].ToString() == "3HZN59939670")
                    {
                        tableNew.ImportRow(drtableOld);
                    }
                }
                DataTable tableNew2 = DataTable.Clone();
                foreach (DataRow drtableOld in DataTable.Rows)
                {
                    if (drtableOld["Code"].ToString() == "3HZN59939670") //&& drtableOld["CovCode"].ToString() == "G978")
                    {
                        tableNew2.ImportRow(drtableOld);
                    }
                }

                //==================
            }
            table.Columns.Remove("CodeGrp2");
            table.Columns.Remove("CodeGrp1");
            table.Columns.Remove("GroupChain");
            table.Columns.Remove("GroupChainCount");
            table.Columns.Add("IMBDig");

            //FileInfo fileInfo = new System.IO.FileInfo(fileName);


            //mark pull outs


            foreach (DataRow dr in table.Rows)
            {
                string valuetosearch = dr["Member ID"].ToString();
                foreach (DataRow drS in pullOuts.Rows)
                {
                    if (drS["code"].ToString() == valuetosearch)
                    {
                        dr["DL"] = "N";
                        dr["grp_bundle"] = "";
                        //                     dbU.ExecuteScalar("update HOR_parse_ID_Cards_Pull set ProcessDate = GETDATE(), Recordnum = " +
                        //dr["Recnum"] + ", filename = '" + fileInfo.Name + "' where CONVERT(DATE,InputDate)='" + DateTime.Now.ToString("yyyy-MM-dd") + "' and code = '" + drS["code"].ToString() + "'");


                        dbU.ExecuteScalar("update HOR_parse_ID_Cards_Pull set ProcessDate = GETDATE(), Recordnum = " +
                           dr["Recnum"] + ", filename = '" + fileInfo.Name + "' where code = '" + drS["code"].ToString() + "'");
                    }
                    //dr["File_Source"] = 
                }
            }

            //++++++++++++==================   CSV   to   BCC
            string BCCname = "HORIZ_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
            string pBCCname = fileInfo.DirectoryName + "\\" + BCCname;
            if (File.Exists(pBCCname))
                File.Delete(pBCCname);

            var fieldnamesBcc = new List<string>();
            fieldnamesBcc.Add("Recnum");
            fieldnamesBcc.Add("F2"); fieldnamesBcc.Add("F3"); fieldnamesBcc.Add("F4"); fieldnamesBcc.Add("F5"); fieldnamesBcc.Add("F6"); fieldnamesBcc.Add("F7");
            fieldnamesBcc.Add("F8"); fieldnamesBcc.Add("F9"); fieldnamesBcc.Add("F10"); fieldnamesBcc.Add("F11"); fieldnamesBcc.Add("F12"); fieldnamesBcc.Add("F13");
            fieldnamesBcc.Add("F14"); fieldnamesBcc.Add("Addr1"); fieldnamesBcc.Add("Addr2"); fieldnamesBcc.Add("Addr3"); fieldnamesBcc.Add("Addr4"); fieldnamesBcc.Add("Addr5"); fieldnamesBcc.Add("Addr6");

            // createCSV createcsvT = new createCSV();
            bool resp = createcsvT.addRecordsCSV(pBCCname, fieldnamesBcc);
            foreach (DataRow row in table.Rows)
            {

                var rowData = new List<string>();

                rowData.Add(row[23].ToString());
                rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); ; rowData.Add("");
                rowData.Add(row[4].ToString());
                rowData.Add(row[9].ToString());
                rowData.Add(row[10].ToString());
                rowData.Add("");//rowData.Add(row[11].ToString());
                rowData.Add(row[11].ToString());
                rowData.Add(row[12].ToString() + " " + row[13].ToString() + " " + row[14].ToString());

                resp = false;
                resp = createcsvT.addRecordsCSV(pBCCname, rowData);

            }
            //copy to CASS
            string cassFileName = ProcessVars.gDMPs + BCCname;
            File.Copy(pBCCname, cassFileName);

            string bccready = @"\\CIERANT-TAPER\DMPS\BCC_JM_PROCESSED_FOLDER_HORIZON-ID-AND-NOTICE-W-IMB\" + BCCname.Replace(".csv", "-OUTPUT.csv");
            string BCCBack = @"\\CIERANT-TAPER\DMPS\BCC_JM_PROCESSED_FOLDER_HORIZON-ID-AND-NOTICE-W-IMB\";
            int numberTry = 0;

            FileInfo infoBCCreadfy = new FileInfo(bccready);
            string getBCCready = "";
            while (IsFileReady(infoBCCreadfy))
            {
                Thread.Sleep(500);
                numberTry++;
                if (numberTry > 300)
                {
                    getBCCready = "not found file after 200 attempts : " + bccready;
                    //sendMails sendmail = new sendMails();
                    //sendmail.SendMailError("BCBS_MA_Processing EOC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                    //errors = errors + "Error " + getBCCready;
                    break;
                }
            }
            //var t0 = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 1);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //t0.Wait();



            
            int errorsBcc = 0;
            //===================================
            if (File.Exists(BCCBack + BCCname.Replace(".csv", "-OUTPUT.csv")))
            {
                BackCASS readresults = new BackCASS();
                DataTable backfromBCC = readresults.readcsvHNJH(BCCBack + BCCname.Replace(".csv", "-OUTPUT.csv"));
                backfromBCC.Columns.RemoveAt(1);
                backfromBCC.Columns.RemoveAt(2);
                backfromBCC.Columns.RemoveAt(3);
                backfromBCC.Columns.RemoveAt(4);
                backfromBCC.Columns.RemoveAt(5);
                backfromBCC.Columns.RemoveAt(1);
                backfromBCC.Columns.RemoveAt(1);
                backfromBCC.Columns.RemoveAt(1);
                backfromBCC.Columns.RemoveAt(1);
                //backfromBCC.Columns.RemoveAt(1);

                createcsvT.printCSV_fullProcess(pBCCname.Replace(".csv", "_Digits.csv"), backfromBCC, "N", "");
                try
                {
                    //var resultMatch = from dataRows1 in table.AsEnumerable()
                    //                  join dataRows2 in backfromBCC.AsEnumerable()
                    //             on dataRows1.Field<string>("Recnum") equals dataRows2.Field<string>("Recnum")

                    //                  select dtResult.LoadDataRow(new object[]
                    //     {
                    //        dataRows1,
                    //        dataRows2.Field<string>("IMBDig")

                    //      }, false);
                    //resultMatch.CopyToDataTable();

                    //var query = from dataRows1 in table.AsEnumerable()
                    //            join dataRows2 in backfromBCC.AsEnumerable()
                    //                on dataRows1.Field<string>("Recnum") equals dataRows2.Field<string>("Recnum") into GJ
                    //            from sub in GJ.DefaultIfEmpty()
                    //            select new { RowA = dataRows1, RowB = sub };
                    //query.ToList();
                    table.Columns.Add("IMBChar", typeof(String));

                    foreach (DataRow r1 in table.Rows.Cast<DataRow>().ToArray()) // save rows to array
                    {
                        foreach (DataRow r2 in backfromBCC.Rows)
                        {
                            if (r1.Field<string>("Recnum") == r2.Field<string>("Recnum"))
                            {
                                r1["IMBDig"] = r2.Field<string>("IMBDig");
                                r1["IMBChar"] = r2.Field<string>("IMBChar");
                                break; // break inner loop
                            }
                        }
                    }



                }

                catch (Exception ex)
                {
                    errorsBcc++;
                }
            }
            else
            {
                var here = "no bcc file...";
            }





            List<int> primes = new List<int>(new int[] { 23, 24, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 25, 26, 27, 28, 29, 30, 33 });


            createCSV createcsv = new createCSV();
            //string pName = System.IO.Directory.GetParent(directoryTXT).FullName + @"\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            string pName = fileName.Substring(0, fileName.Length - 4) + ".csv";
            if (File.Exists(pName))
                File.Delete(pName);
            var fieldnames = new List<string>();
            foreach (int number in primes)
            {
                fieldnames.Add(table.Columns[number].ColumnName);
            }




            bool respBCC = createcsv.addRecordsPipe_CSV(pName, fieldnames);
            //bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            DataTable dataToSql = table.Copy();
            foreach (DataRow oRow in table.Rows)            // check datatoSQL......................................
            {
                var rowData = new List<string>();
                foreach (int number in primes)
                {
                    rowData.Add(oRow[number].ToString());
                }
                respBCC = false;
                respBCC = createcsv.addRecordsPipe_CSV(pName, rowData);
            }

            string colnames = "";
            for (int index = 0; index < dataToSql.Columns.Count; index++)
            {
                string colname = dataToSql.Columns[index].ColumnName;
                colnames = colnames + ", [" + colname + "]";
            }


            int TotRecnum = dataToSql.Rows.Count;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from HOR_parse_TMP_Maintenance_ID_Cards");

            int errors = 0;
            string recnumError = "";
            string insertCommand1 = "Insert into HOR_parse_TMP_Maintenance_ID_Cards([FileName],[ImportDate]" + colnames + ") VALUES ('";
            foreach (DataRow row in dataToSql.Rows)
            {
                string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                for (int index = 0; index < dataToSql.Columns.Count; index++)
                {
                    insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''") + "','";
                }
                try
                {
                    recnumError = row[0].ToString();
                    var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                }
                catch (Exception ex)
                {
                    errors++;
                }
            }
            //DataTable uniqueTable2 = dbU.ExecuteDataTable("");
            //createCSV createcsvS = new createCSV();
            //string pNameTXT2 = ProcessVars.InputDirectory + @"ID_Cards\\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv";
            //if (File.Exists(pNameTXT2))
            //    File.Delete(pNameTXT2);
            //var fieldnames2 = new List<string>();
            //for (int index = 0; index < uniqueTable2.Columns.Count; index++)
            //{
            //    fieldnames2.Add(uniqueTable2.Columns[index].ColumnName);
            //}
            //bool resp2 = createcsvS.addRecordsCSV(pNameTXT2, fieldnames2);
            //foreach (DataRow row in uniqueTable2.Rows)
            //{

            //    var rowData = new List<string>();
            //    for (int index = 0; index < uniqueTable2.Columns.Count; index++)
            //    {
            //        rowData.Add(row[index].ToString());
            //    }
            //    resp2 = false;
            //    resp2 = createcsvS.addRecordsCSV(pNameTXT2, rowData);
            //    //if (UpdSQL != "")
            //    //    dbU.ExecuteScalar(UpdSQL + row[0]);
            //}
            //string bundleDir = @"\\CIERANT-TAPER\Clients\Horizon BCBS\ID Cards\Bundle\" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
            //Directory.CreateDirectory(bundleDir);
            //File.Copy(pNameTXT2, bundleDir + "\\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_Group_to_labels.csv");



            string resultcsv = "";
            //string resultcsv = createcsv.create_HORIZ_CAS_CSV(
            //                                fileName, table, "Maintenance_ID_Cards", Recnum, table.Rows.Count.ToString(), "No Sysout", "No Jobname", GlobalVar.DateofProcess.ToString());
            //if (resultcsv == "")   

            // if (!testProcess)
            if(errors == 0)
            {
            int tottimes = 1;
                {
                    string zipName = "";
                    DirectoryInfo originalZIP = new DirectoryInfo(directoryTXT);


                    if (!nozip)
                        {

                        FileInfo[] FileZIP = originalZIP.GetFiles("*.zip");
                        zipName = "";
                        if (FileZIP.Count() == 1)
                            zipName = FileZIP.First().ToString();
                        ZipFiles zipfiles = new ZipFiles();
                        string resultZ = zipfiles.AddFilestoZip(directoryTXT + "\\" + zipName, pName);
                        if (insertBBB != "")
                            resultZ = zipfiles.AddFilestoZip(directoryTXT + "\\" + zipName, insertBBB);
                        // copy zip to network
                        string NDirectory = @"\\CIERANT-TAPER\Clients\Horizon BCBS\ID Cards\SECURE DATA\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
                        string Network_pName = NDirectory + "\\" + zipName;
                        if (!Directory.Exists(NDirectory))
                            Directory.CreateDirectory(NDirectory);

                        if (File.Exists(Network_pName))
                            File.Delete(Network_pName);
                        File.Copy(directoryTXT + "\\" + zipName, Network_pName);
                        // upload zip
                        N_loadFromFTP uploadZip = new N_loadFromFTP();
                        string resultUpload = "";
                        try
                            {
                            if (zipName.ToUpper().IndexOf("TEST") == -1)
                                {
                                if (zipName.IndexOf("OMNIA") != -1)
                                    {
                                    resultUpload = uploadZip.Upload_SFTP(zipName, directoryTXT + "\\" + zipName, 2, "/OMNIA_ID_Cards/", 1, 1);
                                    File.Copy(directoryTXT + "\\" + zipName, ProcessVars.InputDirectory + "id_cards\\ID_Cards_Omnia\\" + zipName);
                                    }
                                else
                                    {
                                    resultUpload = uploadZip.Upload_SFTP(zipName, directoryTXT + "\\" + zipName, 2, "/ID_Cards/", 1, 1);
                                    try
                                        {
                                        File.Copy(directoryTXT + "\\" + zipName, ProcessVars.InputDirectory + "id_cards\\ID_Cards_Reg\\" + zipName);
                                        }

                                    catch (Exception ex)
                                        {
                                        var msg = ex.Message;
                                        }

                                    }
                                }
                            else
                                {
                                File.Copy(directoryTXT + "\\" + zipName, ProcessVars.InputDirectory + "id_cards\\ID_Cards_Test\\" + zipName);
                                try
                                    {
                                    Directory.CreateDirectory(@"\\CIERANT-TAPER\Clients\Horizon BCBS\TEST FILES\SECURE DATA\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd"));
                                    File.Copy(directoryTXT, @"\\CIERANT-TAPER\Clients\Horizon BCBS\TEST FILES\SECURE DATA\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "\\" + zipName);
                                    }

                                catch (Exception ex)
                                    {
                                    var msg = ex.Message;
                                    }

                                resultUpload = "";
                                }
                            LogWriter logEndProcess = new LogWriter();
                            logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);
                            tottimes = 1;

                        checkAgain:

                            string ftplocation = "ftp://sftp.cierant.com//IN//";
                            string info_User = "Horizon";
                            string info_Pass = "CyRyk1al";


                            string resultRename = uploadZip.NotDownLoadFile_just_Move(ftplocation, zipName, info_User, info_Pass);
                            if (resultRename != "")
                                {
                                tottimes++;
                                if (tottimes > 5)
                                    {
                                    var msg = "";
                                    }
                                else
                                    goto checkAgain;
                                }
                            }

                        catch (Exception ex)
                            {
                            var msg = "";

                            }

                        }
                  
                    if (errors == 0)
                    {
                        try
                        {
                            dbU.ExecuteNonQuery("Delete from HOR_parse_Maintenance_ID_Cards where filename ='" + fileInfo.Name + "'");
                            dbU.ExecuteScalar("Insert into HOR_parse_Maintenance_ID_Cards select * from HOR_parse_TMP_Maintenance_ID_Cards");
                            dbU.ExecuteScalar("delete from HOR_parse_TMP_Maintenance_ID_Cards");
                            dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_Maintenance_ID_Cards', GETDATE())");
                            // rename directory
                            string dirName = Path.GetFileName(directoryTXT);
                            string pathDir = Directory.GetParent(directoryTXT).ToString();  //"\\\\CIERANT-TAPER\\Clients\\Horizon BCBS\\ID Cards\\SECURE DATA\\2015-10-15"
                            Directory.Move(pathDir + "\\" + dirName, pathDir + "\\_" + dirName);
                            //File.Move(directoryTXT, directoryTXT);
                        }
                        catch (Exception ex)
                        {
                            var msg = ex.Message;
                        }
                    }
                    else
                    {
                        var errorsMsg = errors;
                    }
                    try
                    {
                        //check if intesert
                        if ((fileInfo.Name.ToString()).Length > 66)
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                                                  TotRecnum + ",'" + TotRecnum.ToString() + "','" + fileInfo.Name.ToString().Substring(0, 66) + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "','" + lastUpdate + "','HOR_parse_Maintenance_ID_Cards" + "','No CASS','No Sysout','No Jobname','Receive','Y','" + GlobalVar.DateofProcess + "','" + zipName + "')");

                        }
                        else
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                                                       TotRecnum + ",'" + TotRecnum.ToString() + "','" + fileInfo.Name.ToString() + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "','" + lastUpdate + "','HOR_parse_Maintenance_ID_Cards" + "','No CASS','No Sysout','No Jobname','Receive','Y','" + GlobalVar.DateofProcess + "','" + zipName + "')");
                        }//DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")
                    }
                    catch (Exception ex)
                    {
                        string strsqlDup = "select filename , importdate where filename like '%" + fileInfo.Name + "%'";
                        dbU.ExecuteDataTable(strsqlDup);
                        var errorParse = "Dup File???" + ex.Message;
                        errors++;
                    }

                }
            }
            else
            {
                resultcsv  = resultcsv +  "error in file " + fileInfo.FullName;
            }
            return resultcsv;
        }
        public void getfilesTXT(string directoryTXT, string zipname, string fname)
        {
            DataTable.Clear();
            strGroup = GroupBundle =  string.Empty;
            DirectoryInfo originalTXTs = new DirectoryInfo(directoryTXT);
            FileInfo[] FilesTXT = originalTXTs.GetFiles("*.TXT");
            foreach (FileInfo file in FilesTXT)
            {
                if (file.Name.IndexOf("IM") == -1)
                {
                    try
                    {
                        string error = getFileTXT(file.FullName,zipname,fname);
                        if (error != "")
                            errors = errors + error + "\n\n";
                    }
                    catch (Exception ez)
                    {
                        errors = errors + file + "  " + ez.Message + "\n\n";
                    }
                }

            }

        }
        public string getFileTXT(string fileName, string zipname, string fname)
        {
            string[] namesM = new string[] { "MEDICARE", "HMO", "PPD", "PDP", "MA NJ DIRECT15", "MA NJ DIRECT10" };
            //DataTable DataTable = Data_Table();
            bool valueOk = false;
            string line;
            string prevValLine = "";
            int prevline = 0;
            currLine = 0;
            int lineSys = 0;
            Medicare = "";
            Source_M = "";
            bool isIndv = false;

            string sys = "21      ";
            string sysNG = "21      XX";
            string IdentNumber = "3HZN";

            bool inRecord = false;
            FileInfo fileInfo = new System.IO.FileInfo(fileName);

            string pNameTXT = fileName.Substring(0, fileName.Length - 4) + "_Bundle_TXT.csv";
            string pNameTXT2 = fileName.Substring(0, fileName.Length - 4) + "_Bundle_Summary.csv";
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
               
                try
                {
                    if (line.IndexOf(sys) == 0 && line.IndexOf(sysNG) == -1)
                    {
                        if (Codes.Count > 0)
                        {
                            //while (Codes.Count < 25)
                            //{
                            //    Codes.Add("");
                            //}
                            addToTable();
                        }

                        
                        inRecord = true;
                        prevline = currLine;
                        lineSys = currLine + 2;
                        strGroup = prevValLine.Substring(prevValLine.IndexOf("GROUP NO: ", 1) + 10, prevValLine.Length - (prevValLine.IndexOf("GROUP NO: ", 1) + 10));
                        //if(Array.IndexOf(namesM, line) != -1)
                        if (namesM.Any(line.Contains))
                        {
                            Medicare = "IMPORTANT PLAN INFORMATION ENCLOSED";
                            Source_M = line.ToString();
                        }
                        else
                        {
                            Medicare = "";
                            Source_M = line.ToString();
                        }
                    }
                    else
                    {
                        prevline = currLine;
                    }
                    if (line.IndexOf(" 1       END OF SECTION") == 0)
                    {
                        if (Codes.Count > 0)
                        {
                            //while (Codes.Count < 25)
                            //{
                            //    Codes.Add("");
                            //}
                            addToTable();
                        }
                        inRecord = false;
                    }
                    if(currLine == lineSys && inRecord)
                    {
                        if (line.IndexOf("MAIL TO INDIVIDUAL") != -1)
                            GroupBundle = "Individual";
                        else
                        {
                            if (strPrevGroup != strGroup)
                            {
                                GroupBundle = "Group";
                                seqBundle++;
                            }
                        }
                    }
                    if (inRecord && line.IndexOf(IdentNumber) != -1)
                    {
                        Codes.Add(line.Substring(line.IndexOf(IdentNumber, 1),
                            (line.IndexOf(" ", line.IndexOf(IdentNumber, 1) + 1)) - line.IndexOf(IdentNumber, 1)));
                        if (line.Length == 65 || line.Length > 65)
                            CovCodes.Add(line.Substring(60, 5));
                        else
                            CovCodes.Add(line.Substring(60, 4));
                    }

                }
                catch (Exception ex)
                { 
                    CovCodes.Add("error line " + currLine);
                }
                prevValLine = line;

                currLine++;
                if (currLine ==4991)
                    valueOk = true;

            }
            file.Close();
            string test = "";
            DataView dv = DataTable.DefaultView;
            dv.Sort = "SeqBundle";
            DataTable sortedDT = dv.ToTable();
            SummaryDataTable = dv.ToTable();
            sortedDT.Columns.Add("ZipFile", typeof(string));
            sortedDT.Columns.Add("FileName", typeof(string));
            sortedDT.Columns.Add("Records per Bundle", typeof(string));
            string prevValue = "";
            int count = 0;
            int recordnum = 0;
            foreach (DataRow row in sortedDT.Rows)
            {
                if (row["Line"].ToString() == "3271")
                    test = "";
                row["ZipFile"] = zipname;
                row["FileName"] = fname;
                if (row["SeqBundle"].ToString() == prevValue)
                { count++; }
                else
                {
                    if (count == 1)
                    {
                        //mark
                        sortedDT.Rows[recordnum-1]["MarkUnique"] = "X";
                    }
                    count = 1;
                    prevValue = row["SeqBundle"].ToString();
                }
                recordnum++;
            }
            if(sortedDT.Rows[recordnum-2]["SeqBundle"].ToString() != sortedDT.Rows[recordnum-1]["SeqBundle"].ToString())
            {
                sortedDT.Rows[recordnum-1]["MarkUnique"] = "X";
            }


            DataTable tableSummaryBundle = sortedDT.Clone();
            int firstTime = 0;
            string prevSeq = "";
            int recordBundle = 0;
            int currentRow = 0;
            //DataRow prevRow ;
            if (sortedDT.Rows.Count > 0)
            {
                foreach (DataRow drtableOld in sortedDT.Rows)
                {

                    if (drtableOld["MarkUnique"].ToString() != "X" && drtableOld["SeqBundle"].ToString().Length > 0)
                    {

                        if (firstTime == 0)
                        {
                            prevSeq = drtableOld["SeqBundle"].ToString();
                            firstTime = 1;
                        }
                        if (prevSeq != drtableOld["SeqBundle"].ToString())
                        {
                            sortedDT.Rows[currentRow - 1]["Records per Bundle"] = recordBundle;
                            //tableSummaryBundle.ImportRow(sortedDT.Rows[currentRow - 1]);
                            prevSeq = drtableOld["SeqBundle"].ToString();
                            recordBundle = 1;
                        }
                        else
                            recordBundle++;
                    }
                    else
                    {
                        if (firstTime == 1)
                        {
                            if (recordBundle > 1)
                            {
                                sortedDT.Rows[currentRow - 1]["Records per Bundle"] = recordBundle;
                                //tableSummaryBundle.ImportRow(prevRow);
                                prevSeq = drtableOld["SeqBundle"].ToString();
                                recordBundle = 1;
                            }
                        }
                    }
                    currentRow++;
                }



                createCSV createcsv = new createCSV();
                if (File.Exists(pNameTXT))
                    File.Delete(pNameTXT);
                var fieldnames = new List<string>();
                for (int index = 0; index < sortedDT.Columns.Count; index++)
                {
                    fieldnames.Add(sortedDT.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(pNameTXT, fieldnames);
                foreach (DataRow row in sortedDT.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < sortedDT.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pNameTXT, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }
                foreach (DataRow row in sortedDT.Rows)
                {
                    if (row["MarkUnique"].ToString() == "X")
                    {
                        foreach (DataRow rowDT in DataTable.Rows)
                        {
                            if (row["Line"].ToString() == rowDT["Line"].ToString())
                            {
                                rowDT["MarkUnique"] = "X";
                            }
                        }
                    }
                }

                sortedDT.Columns.Remove("Line");
                sortedDT.Columns.Remove("Bundle");
                sortedDT.Columns.Remove("Code");
                sortedDT.Columns.Remove("MarkUnique");
                SummaryDataTable = sortedDT.Clone();
                foreach (DataRow drtableOld in sortedDT.Rows)
                {
                    if (drtableOld["Records per Bundle"].ToString() != "")
                    {
                        if (Int32.Parse(drtableOld["Records per Bundle"].ToString()) > 1)
                        {
                            SummaryDataTable.ImportRow(drtableOld);

                        }
                    }
                }
                if (SummaryDataTable.Rows.Count > 0)
                {
                    if (File.Exists(pNameTXT2))
                        File.Delete(pNameTXT2);
                    var fieldnames2 = new List<string>();
                    for (int index = 0; index < SummaryDataTable.Columns.Count; index++)
                    {
                        fieldnames2.Add(SummaryDataTable.Columns[index].ColumnName);
                    }
                    resp = createcsv.addRecordsCSV(pNameTXT2, fieldnames2);
                    foreach (DataRow row in SummaryDataTable.Rows)
                    {

                        var rowData = new List<string>();
                        for (int index = 0; index < SummaryDataTable.Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());
                        }
                        resp = false;
                        resp = createcsv.addRecordsCSV(pNameTXT2, rowData);
                        //if (UpdSQL != "")
                        //    dbU.ExecuteScalar(UpdSQL + row[0]);
                    }
                }
            }

            return "";
        }
        public void addToTable()
        {
            for (int i = 1; i <= Codes.Count; i++)
            {
                if (Codes[i - 1].Length > 1)
                {
                    var row = DataTable.NewRow();
                    row["Line"] = currLine;
                    row["Group"] = strGroup;
                    if (GroupBundle == "Group")
                    {
                        row["Bundle"] = GroupBundle;
                        row["SeqBundle"] = seqBundle.ToString("D6") + "_" + DateTime.Now.ToString("yyyyMMdd");
                    }
                    row["Medicare"] = Medicare;
                    row["Code" ] = Codes[i - 1];
                    row["CovCode"] = CovCodes[i - 1];
                    row["Source_M"] = Source_M;
                    DataTable.Rows.Add(row);
                }
            }
             Codes.Clear();
             CovCodes.Clear();
             Source_M = "";
             strPrevGroup = strGroup;
            //var row = DataTable.NewRow();
            //row["Line"] = currLine;
            //row["Group"] = strGroup;
            //row["Boundle"] = GroupBoundle;
            //for (int i = 1; i <= 25; i++)
            //{
            //    row["Code" + i] = Codes[i - 1];
            //}

            //DataTable.Rows.Add(row);
            //Codes.Clear();

        }

        public string searchText2(string valuetosearch, string line, int currline)
        {
            try
            {
                int poscBlank = line.IndexOf(" ", line.IndexOf(valuetosearch) + valuetosearch.Length);
                string seg1 = line.Substring(line.IndexOf(valuetosearch));
                string seg2 = seg1.Substring(0, seg1.IndexOf("DATE")).TrimEnd();
                return seg2;
            }
            catch (Exception ex)
            {
                errorcount++;
                return "error line " + currline;
            }
        }
        private List<List<Field>> ParseFile(string inputFile)
        {
            //Get the field mapping.
            List<Field> fields = GetFields();
            //Create a List<List<Field>> collection of collections.
            // The main collection contains our records, and the
            // sub collection contains the fields each one of our
            // records contains.
            List<List<Field>> records = new List<List<Field>>();

            //Open the flat file using a StreamReader.
            using (StreamReader reader = new StreamReader(inputFile))
            {
                //Load the first line of the file.
                string line = reader.ReadLine();

                //Loop through the file until there are no lines
                // left.
                while (line != null)
                {
                    //Create out record (field collection)
                    List<Field> record = new List<Field>();

                    //Loop through the mapped fields
                    foreach (Field field in fields)
                    {
                        Field fileField = new Field();

                        //Use the mapped field's start and length
                        // properties to determine where in the
                        // line to pull our data from.
                        fileField.Value =
                            line.Substring(field.Start, field.Length);

                        //Set the name of the field.
                        fileField.Name = field.Name;

                        //Add the field to our record.
                        record.Add(fileField);
                    }

                    //Add the record to our record collection
                    records.Add(record);

                    //Read the next line.
                    line = reader.ReadLine();
                }
            }

            //Return all of our records.
            return records;
        }

        private List<Field> GetFields()
        {
            List<Field> fields = new List<Field>();
            XmlDocument map = new XmlDocument();

            //Load the mapping file into the XmlDocument
            map.Load(ProcessVars.gmappingFile);

            //Load the field nodes.
            XmlNodeList fieldNodes = map.SelectNodes("/FileMap/Field");

            //Loop through the nodes and create a field object
            // for each one.
            foreach (XmlNode fieldNode in fieldNodes)
            {
                Field field = new Field();

                //Set the field's name
                field.Name = fieldNode.Attributes["Name"].Value;

                //Set the field's length
                field.Length =
                        Convert.ToInt32(fieldNode.Attributes["Length"].Value) ;

                //Set the field's starting position
                field.Start =
                        Convert.ToInt32(fieldNode.Attributes["Start"].Value) -1;

                //Add the field to the Field list.
                fields.Add(field);
            }

            return fields;
        }

        private static DataTable Data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Line");
            newt.Columns.Add("Group");
            newt.Columns.Add("Bundle");
            newt.Columns.Add("SeqBundle");
            newt.Columns.Add("Medicare");
            newt.Columns.Add("Code");
            newt.Columns.Add("CovCode");
            newt.Columns.Add("MarkUnique");
            newt.Columns.Add("Source_M");
            return newt;
        }
        private static DataTable Data_TableS()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Line");
            newt.Columns.Add("Group");
            newt.Columns.Add("Bundle");
            newt.Columns.Add("SeqBundle");
            newt.Columns.Add("Medicare");
            newt.Columns.Add("Code");
            newt.Columns.Add("CovCode");
            newt.Columns.Add("MarkUnique");
            newt.Columns.Add("Source_M");
            newt.Columns.Add("Records per Bundle");
            return newt;
        }
        static bool IsFileReady(FileInfo file)
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

