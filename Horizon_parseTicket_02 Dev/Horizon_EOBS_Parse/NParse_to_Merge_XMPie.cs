using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using System.Threading;
using System.Data.SqlClient;
using System.Configuration;


namespace Horizon_EOBS_Parse
{
    public class NParse_to_Merge_XMPie
    {
        DBUtility dbU;

        public void Load_SMB_txt()
        {
            int totprocessed = 0;
            appSets appsets = new appSets();
            appsets.setVars();
            appSets checkD = new appSets();
            string drivesOk = checkD.checkDrives();
            if (drivesOk == "")
            {
                var files = from fileName in
                                Directory.EnumerateFiles(ProcessVars.InputDirectory + @"From_FTP")
                            where fileName.ToLower().Contains(".txt")
                            select fileName;
                foreach (var fileName in files)
                {
                    FileInfo fileInfo = new System.IO.FileInfo(fileName);
                    if (fileInfo.Name.IndexOf("__") == -1 && fileInfo.Name.IndexOf("SML") != -1)
                    {
                        string result = Process_SMB_txt(fileName);
                        if (result == "")
                        {

                            //string nfilename = ProcessVars.OtherProcessed +  fileInfo.Name;
                            //if (File.Exists(nfilename))
                            //    File.Delete(nfilename);

                            File.Copy(fileInfo.FullName, ProcessVars.OtherProcessed + fileInfo.Name);
                            File.Move(fileInfo.FullName, fileInfo.Directory + @"\\__" + fileInfo.Name);
                            //File.Move(fileInfo.FullName, nfilename);
                            totprocessed++;
                        }
                    }
                }
                //print
                if (totprocessed > 0)
                    OutputSMB();
            }
        }
        public void OutputSMB()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteNonQuery("delete from HOR_XMPie_SMB_1PG");
            dbU.ExecuteNonQuery("delete from HOR_XMPie_SMB");

            SqlParameter[] sqlParams;
            sqlParams = new SqlParameter[] { new SqlParameter("@dateprocess", GlobalVar.DateofProcess.ToString("yyyy-MM-dd")) };
            dbU.ExecuteScalar("HOR_upd_SMB_Data", sqlParams);


            string strsqlU = "select recnum, TOD from HOR_parse_SMB where CONVERT(date,dateImport) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and pages = 10 order by recnum";
            //string strsqlU = "select recnum, TOD from HOR_parse_SMB where (CONVERT(date,dateImport)= '2017-09-01' or CONVERT(date,dateImport) = '2017-09-26' or CONVERT(date,dateImport) = '2017-09-27') and pages = 10 order by recnum";
            DataTable toUpdTOD = dbU.ExecuteDataTable(strsqlU);
            int xx = 1;
            if (toUpdTOD.Rows.Count > 0)
            {
                foreach (DataRow rowTOD in toUpdTOD.Rows)
                {
                    rowTOD["TOD"] = xx;
                    dbU.ExecuteNonQuery("update HOR_parse_SMB set TOD = " + xx + " where recnum  = " + rowTOD["recnum"]);
                    xx = xx + 10;
                }
            }

            string strsqlU2 = "select recnum, TOD from HOR_parse_SMB where CONVERT(date,dateImport) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and pages = 1 order by recnum";
            //string strsqlU2 = "select recnum, TOD from HOR_parse_SMB where(CONVERT(date,dateImport)= '2017-09-01' or CONVERT(date,dateImport) = '2017-09-26' or CONVERT(date,dateImport) = '2017-09-27')  and pages = 1 order by recnum";
            DataTable toUpdTOD2 = dbU.ExecuteDataTable(strsqlU2);
            int xx2 = 1;
            if (toUpdTOD2.Rows.Count > 0)
            {
                foreach (DataRow rowTOD2 in toUpdTOD2.Rows)
                {
                    rowTOD2["TOD"] = xx2;
                    dbU.ExecuteNonQuery("update HOR_parse_SMB set TOD = " + xx2 + " where recnum  = " + rowTOD2["recnum"]);
                    xx2 = xx2 + 1;
                }
            }

            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@dateprocess", GlobalVar.DateofProcess.ToString("yyyy-MM-dd")) };
            dbU.ExecuteScalar("HOR_upd_SMB_Xmpie",sqlParams);



            string strsql2 = "select distinct XmpieTable, Xmpiefilename from HOR_parse_SMB where CONVERT(date,dateImport) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and len(XmpieTable) > 0";

            //fix TOD numbers
            DataTable toUpd = dbU.ExecuteDataTable(strsql2);   ///BRE get only LETTER CODE:  Recertification Letter   according HOR_parse_SMB_Master
            if (toUpd.Rows.Count > 0)
            {
                foreach (DataRow row in toUpd.Rows)
                {
                   

                   string  strsql = "select Recnum, '' as digUId,OutputFileName as FName,'' as artifactId,'' as LetterName, Company_Name as CoverPageName,Delivery_Address as CoverpageAddress1,ALT_ADDRESS_1 as CoverpageAddress2,'' as CoverpageAddress3,'' as CoverpageAddress4, City,st as  State,[Zip+4] as Zip, BRE, TOD, '' as DL" +
                                           " from " + row["XmpieTable"].ToString() + " order by recnum";

                    DataTable toSCI = dbU.ExecuteDataTable(strsql);

                   string  filename = ProcessVars.OtherProcessed + row["Xmpiefilename"].ToString() + ".csv";
                    //savedfilename = ProcessVars.InputDirectory + @"\\from_ftp\\" + row["Xmpiefilename"].ToString() + ".csv";
                    if (toSCI.Rows.Count > 0)
                    {

                        if (File.Exists(filename))
                            File.Delete(filename);
                        createCSV printcsv = new createCSV();
                        printcsv.printCSV_fullProcess(filename, toSCI, "", "");

                    }
                    

                }
                
                //data for XMPIe
                //DataTable dataXmpie1 = dbU.ExecuteDataTable("select * from HOR_XMPie_SMB order by recnum");
                //string filename1 = ProcessVars.InputDirectory + @"\\from_ftp\\" + "Data_from_HOR_XMPie_SMB.csv";
                //if (dataXmpie1.Rows.Count > 0)
                //{
                //    if (File.Exists(filename1))
                //        File.Delete(filename1);
                //    createCSV printcsv = new createCSV();
                //    printcsv.printCSV_fullProcess(filename1, dataXmpie1, "", "Y");

                //}
                //DataTable dataXmpie2 = dbU.ExecuteDataTable("select * from HOR_XMPie_SMB_1PG order by recnum");
                //string filename2 = ProcessVars.InputDirectory + @"\\from_ftp\\" + "Data_from_HOR_XMPie_SMB_1PG.csv";
                //if (dataXmpie2.Rows.Count > 0)
                //{
                //    if (File.Exists(filename2))
                //        File.Delete(filename2);
                //    createCSV printcsv = new createCSV();
                //    printcsv.printCSV_fullProcess(filename2, dataXmpie2, "", "Y");

                //}
            }
            // per file for fot Horizon
            //================================
            //string strsql = "select Recnum, '' as digUId,OutputFileName as FName,'' as artifactId,'' as LetterName, Company_Name as CoverPageName,Delivery_Address as CoverpageAddress1,ALT_ADDRESS_1 as CoverpageAddress2,'' as CoverpageAddress3,'' as CoverpageAddress4, City,st as  State,[Zip+4] as Zip, BRE, TOD, '' as DL" +
            //    
            string strsqlHOR = "select distinct filename from HOR_parse_SMB where CONVERT(date,dateImport) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and len(XmpieTable) > 0";

            DataTable toHOR = dbU.ExecuteDataTable(strsqlHOR);
            if (toHOR.Rows.Count > 0)
            {
                foreach (DataRow row in toHOR.Rows)
                {
                    string strsql = "select RIGHT('00000000' + CAST(Recnum AS VARCHAR(8)),8) + '.pdf' as DocumentName,RIGHT('00000000' + CAST(Recnum AS VARCHAR(8)),8)  as ImageID,Record_ID as SFLetterID, filename as SCIPrintControlNumber, convert(varchar,dateimport,101) as PrintDate, 'RecertificationLetters' as DocumentType " +
                                          " from HOR_parse_SMB where filename = '" + row["filename"].ToString() +  "'  order by recnum";

                    DataTable dataHor = dbU.ExecuteDataTable(strsql);

                    string filename = ProcessVars.OtherProcessed + row["filename"].ToString().Replace(".txt","") + "_HOR.csv";
                    if (dataHor.Rows.Count > 0)
                    {

                        if (File.Exists(filename))
                            File.Delete(filename);
                        createCSV printcsv = new createCSV();
                        printcsv.printCSV_fullProcess(filename, dataHor, "", "");

                    }

                }
            }


        }
        public void Output_Full_SMB()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteNonQuery("delete from HOR_XMPie_SMB_1PG");
            dbU.ExecuteNonQuery("delete from HOR_XMPie_SMB");

            SqlParameter[] sqlParams;
            sqlParams = new SqlParameter[] { new SqlParameter("@dateprocess", DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd")) };
            dbU.ExecuteScalar("HOR_upd_SMB_Data", sqlParams);  //set for full file


            string strsqlU = "select recnum, TOD from HOR_parse_SMB where pages = 10 and letter_code = 'Recertification Letter' order by recnum";
            DataTable toUpdTOD = dbU.ExecuteDataTable(strsqlU);
            int xx = 1;
            if (toUpdTOD.Rows.Count > 0)
            {
                foreach (DataRow rowTOD in toUpdTOD.Rows)
                {
                    rowTOD["TOD"] = xx;
                    dbU.ExecuteNonQuery("update HOR_parse_SMB set TOD = " + xx + " where recnum  = " + rowTOD["recnum"]);
                    xx = xx + 10;
                }
            }

            string strsqlU2 = "select recnum, TOD from HOR_parse_SMB where pages = 1 and letter_code = 'Recertification Letter' order by recnum";
            DataTable toUpdTOD2 = dbU.ExecuteDataTable(strsqlU2);
            int xx2 = 1;
            if (toUpdTOD2.Rows.Count > 0)
            {
                foreach (DataRow rowTOD2 in toUpdTOD2.Rows)
                {
                    rowTOD2["TOD"] = xx2;
                    dbU.ExecuteNonQuery("update HOR_parse_SMB set TOD = " + xx2 + " where recnum  = " + rowTOD2["recnum"]);
                    xx2 = xx2 + 1;
                }
            }

            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@dateprocess", DateTime.Now.AddDays(-0).ToString("yyyy-MM-dd")) };
            dbU.ExecuteScalar("HOR_upd_SMB_Xmpie", sqlParams);



            string strsql2 = "select distinct XmpieTable, '' as Xmpiefilename from HOR_parse_SMB where len(XmpieTable) > 0";

            //fix TOD numbers
            DataTable toUpd = dbU.ExecuteDataTable(strsql2);   ///BRE get only LETTER CODE:  Recertification Letter   according HOR_parse_SMB_Master
            if (toUpd.Rows.Count > 0)
            {

                foreach (DataRow row in toUpd.Rows)
                {
                    string strsql = "select RIGHT('00000000' + CAST(Recnum AS VARCHAR(8)),8) + '.pdf' as DocumentName, letter_Code as DocumentType,RIGHT('00000000' + CAST(Recnum AS VARCHAR(8)),8)  as ImageID,Record_ID as SFLetterID, filename as SCIPrintControlNumber, importdate as PrintDate " +
                                           " from " + row["XmpieTable"].ToString() + " order by recnum";

                    DataTable tocsv = dbU.ExecuteDataTable(strsql);

                    string filename = ProcessVars.OtherProcessed + row["XmpieTable"].ToString() + "_allData.csv";
                    string savedfilename = ProcessVars.InputDirectory + @"from_ftp\" + row["XmpieTable"].ToString() + "_allData.csv";
                    if (tocsv.Rows.Count > 0)
                    {

                        if (File.Exists(filename))
                            File.Delete(filename);
                        createCSV printcsv = new createCSV();
                        printcsv.printCSV_fullProcess(filename, tocsv, "", "");

                    }



                }

                //data for XMPIe
                DataTable dataXmpie1 = dbU.ExecuteDataTable("select * from HOR_XMPie_SMB order by recnum");
                string filename1 = ProcessVars.InputDirectory + @"\\from_ftp\\" + "Data_from_HOR_XMPie_SMB.csv";
                if (dataXmpie1.Rows.Count > 0)
                {
                    if (File.Exists(filename1))
                        File.Delete(filename1);
                    createCSV printcsv = new createCSV();
                    printcsv.printCSV_fullProcess(filename1, dataXmpie1, "", "Y");

                }
                DataTable dataXmpie2 = dbU.ExecuteDataTable("select * from HOR_XMPie_SMB_1PG order by recnum");
                string filename2 = ProcessVars.InputDirectory + @"\\from_ftp\\" + "Data_from_HOR_XMPie_SMB_1PG.csv";
                if (dataXmpie2.Rows.Count > 0)
                {
                    if (File.Exists(filename2))
                        File.Delete(filename2);
                    createCSV printcsv = new createCSV();
                    printcsv.printCSV_fullProcess(filename2, dataXmpie2, "", "Y");

                }
            }





        }
        
        public string Process_SMB_txt(string filename)
        {
            string result = "";
            appSets appsets = new appSets();
            appsets.setVars();
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            FileInfo fileInfo = new System.IO.FileInfo(filename);

            DataTable fromTAB_tmp = loadPipedata(filename);
            if (fromTAB_tmp.Rows.Count > 0)
            {
            DataTable newTABTable = fromTAB_tmp.Select().Where(x => !x.IsNull(0)).CopyToDataTable();
           
                foreach (var column in newTABTable.Columns.Cast<DataColumn>().ToArray())
                {
                    if (newTABTable.AsEnumerable().All(dr => dr.IsNull(column)))
                        newTABTable.Columns.Remove(column);
                }

                newTABTable.Columns.Add("DateImport").SetOrdinal(0);
                newTABTable.Columns.Add("FileName").SetOrdinal(0);
                newTABTable.Columns.Add("Recnum").SetOrdinal(0);

                // newTABTable.Columns.Add("IMAGE_SELECT");
                newTABTable.Columns.Add("UpdAddr1");
                newTABTable.Columns.Add("UpdAddr2");
                newTABTable.Columns.Add("UpdAddr3");
                newTABTable.Columns.Add("UpdAddr4");
                newTABTable.Columns.Add("UpdAddr5");
                newTABTable.Columns.Add("UpdCity");
                newTABTable.Columns.Add("UpdState");
                newTABTable.Columns.Add("UpdZip");
                newTABTable.Columns.Add("UpdCounty");
                newTABTable.Columns.Add("DL");
                newTABTable.Columns.Add("IMBChar");
                newTABTable.Columns.Add("IMBDig");
                newTABTable.Columns.Add("XMPieFileName");
                newTABTable.Columns.Add("XMPieLetter");
                newTABTable.Columns.Add("XMPieFormating");
                newTABTable.Columns.Add("Campaign_Name");

                newTABTable.Columns.Add("BRE");
                newTABTable.Columns.Add("TOD");
                newTABTable.Columns.Add("XMPieTable");


                dbU.ExecuteNonQuery("delete from HOR_parse_SMB where filename = '" + fileInfo.Name + "'");
                dbU.ExecuteNonQuery("delete from HOR_parse_SEQ where Description = '" + fileInfo.Name + "' and tablename = 'HOR_parse_SMB'");
                DataColumnCollection columns = newTABTable.Columns;
                int GRecnum = 1;

                SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", newTABTable.Rows.Count), 
                new SqlParameter("@FileName", fileInfo.Name), new SqlParameter("@TableName", "HOR_parse_SMB") };
                dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
                DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + fileInfo.Name + "' and tablename = 'HOR_parse_SMB'");
                if (afterUpdateSeq.Rows.Count == 1)
                    GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - newTABTable.Rows.Count + 1;
                else
                {
                    SendMails sendmail = new SendMails();
                    sendmail.SendMailError("error in HOR_upd_Recnum_beforeTMP ", "Error reading recnum after update", "\n\n" + "Error table: HOR_parse_SMB,   file " + fileInfo.Name, "");
                }
                foreach (DataRow row in newTABTable.Rows)
                {
                    row["FileName"] = fileInfo.Name;
                    row["DateImport"] = GlobalVar.DateofProcess;   // DateTime.Now;
                    row["Recnum"] = GRecnum;
                    GRecnum++;
                }
                //createCSV createcsv = new createCSV();
                //createcsv.printCSV_fullProcess(fileInfo + "testdata.csv", newTABTable, "", "N");
                if (newTABTable.Rows.Count > 0)
                {
                    DataTable toBCC = new System.Data.DataTable();
                    toBCC.Columns.Add("Recnum");
                    toBCC.Columns.Add("FullName");
                    toBCC.Columns.Add("Addr1");
                    toBCC.Columns.Add("Addr2");
                    toBCC.Columns.Add("Addr5");
                    foreach (DataRow row in newTABTable.Rows)
                    {
                        var rowBCC = toBCC.NewRow();
                        rowBCC["Recnum"] = row["Recnum"].ToString();
                        rowBCC["FullName"] = row["Group_Name"].ToString();
                        rowBCC["Addr1"] = row["Address_1"].ToString();
                        rowBCC["Addr2"] = row["Address_2"].ToString();
                        rowBCC["Addr5"] = (row["city"].ToString() + ' ' + row["state"].ToString() + ' ' + row["postal_code"].ToString()).Trim();
                        toBCC.Rows.Add(rowBCC);
                    }
                    for (int i = 0; i < 13; i++)
                    {
                        toBCC.Columns.Add("F" + i, typeof(string)).SetOrdinal(1);
                    }

                    toBCC.Columns.Add("Add4", typeof(string)).SetOrdinal(17);
                    toBCC.Columns.Add("Add3", typeof(string)).SetOrdinal(17);
                    // toBCC.Columns.Add("Cycle", typeof(string)).SetOrdinal(2);


                    //string wbccName = ProcessVars.InputDirectory + @"From_FTP" + "MAS021_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                    //string bccName = ProcessVars.dmpsWatched + "MAS021_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                    //string bccready = ProcessVars.oDMPsDirectoryM + "MAS023_MAS021_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC_PROCESSED.csv";
                    string wbccName = ProcessVars.InputDirectory + "HORIZ_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                    string bccName = ProcessVars.dmpsWatched + "HORIZ_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                    string bccready = ProcessVars.gODMPs_IMB + "HORIZ_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC-OUTPUT.csv";



                    if (File.Exists(bccready))
                        File.Delete(bccready);

                    createCSV printcsv = new createCSV();

                    if (File.Exists(wbccName))
                        File.Delete(wbccName);

                    printcsv.printCSV_fullProcess(wbccName, toBCC, "", "");

                    if (File.Exists(bccName))
                        File.Delete(bccName);
                    File.Copy(wbccName, bccName);
                    int numberTry = 0;
                    string errors = "";
                    FileInfo infoBCCreadfy = new FileInfo(bccready);
                    string getBCCready = "";
                    while (IsFileReady(infoBCCreadfy))
                    {
                        Thread.Sleep(500);
                        numberTry++;
                        if (numberTry > 300)
                        {
                            getBCCready = "not found file after 200 attempts : " + bccready;
                            SendMails sendmail = new SendMails();
                            sendmail.SendMailError("NParse_to_Merge_XMPie", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                            errors = errors + "Error " + getBCCready;
                            break;
                        }
                    }
                    string resultBCC = "";
                    if (getBCCready == "")
                    {
                        resultBCC = processReturnBCC_and_upd_Sql(newTABTable, infoBCCreadfy, fileInfo.Name);

                        if (resultBCC == "")
                        {
                            newTABTable.Columns.Add("Pages", typeof(string));
                            
                            dbU.ExecuteScalar("delete from HOR_parse_SMB_tmp");



                            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                            Connection.Open();

                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                            {
                                bulkCopy.DestinationTableName = "[dbo].[HOR_parse_SMB_tmp]";

                                try
                                {
                                    bulkCopy.BatchSize = newTABTable.Rows.Count;
                                    bulkCopy.BulkCopyTimeout = 0;
                                    bulkCopy.WriteToServer(newTABTable);
                                }
                                catch (Exception ex)
                                {
                                    errors = errors + ex.Message;
                                }
                            }
                            Connection.Close();

                            if (errors == "")
                            {

                                //dbU.ExecuteNonQuery("HOR_upd_Recnum_tmp");


                                dbU.ExecuteScalar("Insert into HOR_parse_SMB select * from HOR_parse_SMB_tmp");


                            }
                        }
                    }
                }
            }
            else
                result = "no Records";

            return result;
        }
        public DataTable loadPipedata(string filename)
        {

            var reader = ReadAsLines(filename);

            var data = new DataTable();
            try
            {
                //this assume the first record is filled with the column names
                var headers = reader.First().Split('|');
                foreach (var header in headers)
                    data.Columns.Add(header);

                var records = reader.Skip(1);
                foreach (var record in records)
                    data.Rows.Add(record.Split('|'));
            }
            catch (Exception ex)
            {
               
            }
            
            return data;
        }
        static IEnumerable<string> ReadAsLines(string filename)
        {
            using (var reader = new StreamReader(filename))
                while (!reader.EndOfStream)
                    yield return reader.ReadLine();
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

        public string processReturnBCC_and_upd_Sql(DataTable dt1, FileInfo bccfile, string fName)
        {
            string result = ""; int numberTry = 0;
            string getBCCready = "";
            while (IsFileReady(bccfile))
            {
                Thread.Sleep(500);
                numberTry++;
                if (numberTry > 50)
                {
                    getBCCready = "not found file after 50 attempts : " + bccfile.FullName;
                    SendMails sendmail = new SendMails();
                    sendmail.SendMailError("NParse_to_Merge_XMPie", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                    break;
                }
            }
            if (getBCCready == "")
            {
                if (File.Exists(bccfile.FullName))
                {

                    BackCASS readresults = new BackCASS();
                    DataTable backfromBCC = readresults.readcsvHorizIMB(bccfile.FullName, fName);
                    //DataTable NonD_Records = readresults.readNonDeliverable(bccfile.FullName.Replace(".csv", "-NON-DELIVERABLE.csv"));
                    dt1.PrimaryKey = new DataColumn[] { dt1.Columns["Recnum"] };
                    if (backfromBCC.Rows.Count > 0)
                    {
                        try
                        {
                            backfromBCC.Columns["Sysout"].ColumnName = "Recnum";
                            backfromBCC.PrimaryKey = new DataColumn[] { backfromBCC.Columns["Recnum"] };

                            foreach (DataRow dRNew in backfromBCC.Rows)
                            {
                                DataRow row = null;
                                try
                                {
                                    row = dt1.Rows.Find(dRNew["Recnum"].ToString());
                                }
                                catch (MissingPrimaryKeyException)
                                {
                                    row = dt1.Select("Recnum=" + dRNew["Recnum"] + "'").First();
                                }
                                if (row != null)
                                {
                                    row["UpdAddr1"] = dRNew["NAME_FULL"];
                                    row["UpdAddr2"] = dRNew["DELIVERY_ADDRESS"];
                                    row["UpdAddr3"] = dRNew["ALT_ADDRESS_1"];
                                    row["UpdAddr4"] = dRNew["ALT_ADDRESS_2"];
                                    row["UpdAddr5"] = dRNew["ADDRESS_LINE_3"];
                                    row["UpdCity"] = dRNew["CITY"];
                                    row["UpdState"] = dRNew["ST"];
                                    row["UpdZip"] = dRNew["ZIP+4"];
                                    row["UpdCounty"] = "";
                                    row["IMBChar"] = dRNew["Intelligent Mail barcode"];
                                    row["IMBDig"] = dRNew["Intelligent Mail barcode DIG"];
                                    row["DL"] = "";
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                        }

                       
                    }
       
                }
                else
                {
                    result = result + bccfile.Name + " OUTPUT.csv not found..." + Environment.NewLine;
                }
            }


            return result;
        }
    }

}
