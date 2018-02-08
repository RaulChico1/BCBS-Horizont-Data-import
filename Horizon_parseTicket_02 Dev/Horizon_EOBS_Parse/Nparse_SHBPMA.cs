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
using System.Data.SqlClient;
using System.Configuration;

namespace Horizon_EOBS_Parse
{
    public class Nparse_SHBPMA
    {
        DBUtility dbU;
        public void Load_SHBPMA_txt()
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
                    if (fileInfo.Name.IndexOf("__") == -1 && fileInfo.Name.IndexOf("SHBPMA") != -1)
                    {
                        string result = Process_SHBPMA_txt(fileName);
                        if (result == "")
                        {

                            File.Copy(fileInfo.FullName, ProcessVars.OtherProcessed + fileInfo.Name);
                            File.Move(fileInfo.FullName, fileInfo.Directory + @"\\__" + fileInfo.Name);
                            //File.Move(fileInfo.FullName, nfilename);
                            totprocessed++;
                        }
                    }
                }
                if(totprocessed > 0)
                    OutputSHBPMA();
            }
        }
        public void OutputSHBPMA()
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
            dbU.ExecuteScalar("HOR_upd_SMB_Xmpie", sqlParams);



            string strsql2 = "select distinct XmpieTable, Xmpiefilename from HOR_parse_SMB where CONVERT(date,dateImport) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and len(XmpieTable) > 0";

            //fix TOD numbers
            DataTable toUpd = dbU.ExecuteDataTable(strsql2);   ///BRE get only LETTER CODE:  Recertification Letter   according HOR_parse_SMB_Master
            if (toUpd.Rows.Count > 0)
            {
                foreach (DataRow row in toUpd.Rows)
                {


                    string strsql = "select Recnum, '' as digUId,OutputFileName as FName,'' as artifactId,'' as LetterName, Company_Name as CoverPageName,Delivery_Address as CoverpageAddress1,ALT_ADDRESS_1 as CoverpageAddress2,'' as CoverpageAddress3,'' as CoverpageAddress4, City,st as  State,[Zip+4] as Zip, BRE, TOD, '' as DL" +
                                            " from " + row["XmpieTable"].ToString() + " order by recnum";

                    DataTable toSCI = dbU.ExecuteDataTable(strsql);

                    string filename = ProcessVars.OtherProcessed + row["Xmpiefilename"].ToString() + ".csv";
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
                                          " from HOR_parse_SMB where filename = '" + row["filename"].ToString() + "'  order by recnum";

                    DataTable dataHor = dbU.ExecuteDataTable(strsql);

                    string filename = ProcessVars.OtherProcessed + row["filename"].ToString().Replace(".txt", "") + "_HOR.csv";
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
        public string Process_SHBPMA_txt(string fileName)
        {
            string results = "";
            
            appSets appsets = new appSets();
            appsets.setVars();
            string tabName = "";
            int colnum = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable tblDef = dbU.ExecuteDataTable("select TableName,parameters from HOR_parse_N_Category_Master where Code = 'SHBPMA'");
            if (tblDef.Rows.Count == 1)
            {
                tabName = tblDef.Rows[0][0].ToString();
                colnum = Int32.Parse(tblDef.Rows[0][1].ToString());
                DataTable columnsDef = dbU.ExecuteDataTable("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + tabName + "'");
                DataTable dt = loadCSV(columnsDef, colnum,fileName);
                string strsql = "";
                string bccloc = "";
                if (processBCC(dt, fileName, tabName))
                {


                }

            }

            return results;
        }

        public bool processBCC(DataTable dt, string fileName, string tabName)
        {
            bool result = false;
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            string BCCname = "HORIZ_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";

            string pBCCname = fileInfo.DirectoryName + "\\" + BCCname;
            if (File.Exists(pBCCname))
                File.Delete(pBCCname);

            var fieldnamesBcc = new List<string>();
            fieldnamesBcc.Add("Recnum");
            fieldnamesBcc.Add("F2"); fieldnamesBcc.Add("F3"); fieldnamesBcc.Add("F4"); fieldnamesBcc.Add("F5"); fieldnamesBcc.Add("F6"); fieldnamesBcc.Add("F7");
            fieldnamesBcc.Add("F8"); fieldnamesBcc.Add("F9"); fieldnamesBcc.Add("F10"); fieldnamesBcc.Add("F11"); fieldnamesBcc.Add("F12"); fieldnamesBcc.Add("F13");
            fieldnamesBcc.Add("F14"); fieldnamesBcc.Add("Addr1"); fieldnamesBcc.Add("Addr2"); fieldnamesBcc.Add("Addr3"); fieldnamesBcc.Add("Addr4"); fieldnamesBcc.Add("Addr5"); fieldnamesBcc.Add("Addr6");

             createCSV createcsvT = new createCSV();
            bool resp = createcsvT.addRecordsCSV(pBCCname, fieldnamesBcc);
            foreach (DataRow row in dt.Rows)
            {

                var rowData = new List<string>();

                rowData.Add(row[0].ToString());
                rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); ; rowData.Add("");
                rowData.Add(row[5].ToString().Trim() + " " + row[6].ToString().Trim());
                rowData.Add(row[7].ToString());
                rowData.Add(row[8].ToString());
                rowData.Add(row[9].ToString());
                rowData.Add("");
                rowData.Add(row[10].ToString() + " " + row[11].ToString() + " " + row[12].ToString());

                resp = false;
                resp = createcsvT.addRecordsCSV(pBCCname, rowData);

            }
            //copy to CASS
            string cassFileName = ProcessVars.gDMPs + BCCname;
            File.Copy(pBCCname, cassFileName);

            string bccready = @"\\CIERANT-TAPER\DMPS\BCC_JM_PROCESSED_FOLDER_HORIZON-ID-AND-NOTICE-W-IMB\" + BCCname.Replace(".csv", "-OUTPUT.csv");
            string BCCBack = @"\\CIERANT-TAPER\DMPS\BCC_JM_PROCESSED_FOLDER_HORIZON-ID-AND-NOTICE-W-IMB\";
            string bccerror = @"\\CIERANT-TAPER\DMPS\BCC_JM_PROCESSED_FOLDER_HORIZON-ID-AND-NOTICE-W-IMB\" + BCCname.Replace(".csv", "-NON-DELIVERABLE.csv");
            string fileDir = ProcessVars.InputDirectory + "w_Process\\";
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
                string resultBCC = processReturnBCC_and_upd_Sql(dt, infoBCCreadfy, fileInfo.Name, tabName, bccerror, fileDir);

            

            return result;
        }
         public string processReturnBCC_and_upd_Sql(DataTable dt1, FileInfo bccfile, string fName, string datatable, string bccerror, string dirCopyTo)
        {
            string result = ""; int numberTry = 0;

            if (File.Exists(bccfile.FullName))
            {
                File.Copy(bccfile.FullName, dirCopyTo + bccfile.Name,true);
                BackCASS readresults = new BackCASS();
                DataTable backfromBCC = readresults.readQualifiedMAS023(bccfile.FullName, fName);
                //DataTable NonD_Records = readresults.readNonDeliverable(bccfile.FullName.Replace(".csv", "-NON-DELIVERABLE.csv"));
                dt1.PrimaryKey = new DataColumn[] { dt1.Columns["Recnum"] };
                if (backfromBCC.Rows.Count > 0)
                {
                    try
                    {
                        //backfromBCC.Columns["LINE_01"].ColumnName = "Recnum";
                        //backfromBCC.PrimaryKey = new DataColumn[] { backfromBCC.Columns["Recnum"] };

                        foreach (DataRow dRNew in backfromBCC.Rows)
                        {
                            DataRow row = null;
                            try
                            {
                                row = dt1.Rows.Find(dRNew["Sysout"].ToString());
                            }
                            catch (MissingPrimaryKeyException)
                            {
                                row = dt1.Select("Recnum=" + dRNew["Sysout"] + "'").First();
                            }
                            if (row != null)
                            {
                                row["UpdAddr1"] = dRNew["NAME_FULL"];
                                row["UpdAddr2"] = dRNew["DELIVERY_ADDRESS"];
                                row["UpdAddr3"] = dRNew["ALT_ADDRESS_2"];
                                row["UpdAddr4"] = dRNew["ADDRESS_LINE_3"];
                                row["UpdAddr5"] = dRNew["ALT_ADDRESS_1"];
                                row["UpdCity"] = dRNew["CITY"];
                                row["UpdState"] = dRNew["ST"];
                                row["UpdZip"] = dRNew["ZIP+4"];
                                row["UpdCounty"] = dRNew["County Name"];
                                row["IMBChar"] = dRNew["Intelligent Mail barcode"];
                                row["IMBDig"] = dRNew["Intelligent Mail barcode DIG"];
                                row["UpdCounty"] = dRNew["County Name"];
                                row["DL"] = "Y";
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                    }
                    int totinerror = 0;
                    if (File.Exists(bccerror))
                    {
                        FileInfo infoBCCerror = new FileInfo(bccerror);
                        File.Copy(infoBCCerror.FullName, dirCopyTo + infoBCCerror.Name,true);
                        DataTable backfromBCCerror = readresults.readQualifiedMAS023(infoBCCerror.FullName, infoBCCerror.Name);
                        dt1.PrimaryKey = new DataColumn[] { dt1.Columns["Recnum"] };
                        if (backfromBCCerror.Rows.Count > 0)
                        {
                            try
                            {
                                totinerror = backfromBCCerror.Rows.Count;
                                //backfromBCC.PrimaryKey = new DataColumn[] { backfromBCC.Columns["Recnum"] };
                                foreach (DataRow dRNew in backfromBCCerror.Rows)
                                {
                                    DataRow row = null;
                                    try
                                    {
                                        row = dt1.Rows.Find(dRNew["RECNO"].ToString());
                                    }
                                    catch (MissingPrimaryKeyException)
                                    {
                                        row = dt1.Select("Recnum=" + dRNew["RECNO"] + "'").First();
                                    }
                                    if (row != null)
                                    {
                                        row["UpdZip"] = dRNew["Return Code"];
                                        row["DL"] = "N";
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                            }
                        }
                    }
                    try
                    {
                        GlobalVar.dbaseName = "BCBS_Horizon";
                        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                        if (result == "")
                        {
                            dbU.ExecuteScalar("delete from " + datatable + "_tmp");
                           // GlobalVar.connectionKey = "conStrOld";
                            //GlobalVar.dbaseName = "BCBS_Horizon";
                            //dbU = new DBUtility("conStrOld", DBUtility.ConnectionStringType.Configured);

                            var test = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
                            int GRecnum = 0;
                            SqlParameter[] sqlParams2;
                            sqlParams2 = null;
                            sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", dt1.Rows.Count),

                                new SqlParameter("@FileName", fName), new SqlParameter("@TableName",  datatable.Replace("_TMP","")) };
                            dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
                            DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + fName + "' and tablename = '" + datatable.Replace("_TMP", "") + "' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
                            if (afterUpdateSeq.Rows.Count == 1)
                                GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - dt1.Rows.Count + 1;
                            else
                            {
                                result = result + " more than 1 record for file " + fName + " in HOR_parse_SEQ " + Environment.NewLine;
                            }
                            if (result == "")
                            {
                                foreach (DataRow row in dt1.Rows)
                                {
                                    row["Recnum"] = GRecnum;

                                    GRecnum++;
                                }
                            }
                            GlobalVar.connectionKey = "conStrProd";
                            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                        }
                    }
                    catch (Exception ex)
                    {
                        result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                    }

                    if (result == "")
                    {

                        SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                        Connection.Open();

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                        {
                            bulkCopy.DestinationTableName = datatable + "_TMP";

                            try
                            {
                                bulkCopy.BatchSize = dt1.Rows.Count;
                                bulkCopy.BulkCopyTimeout = 0;
                                bulkCopy.WriteToServer(dt1);
                            }
                            catch (Exception ex)
                            {
                                result = result + ex.Message;
                            }
                        }
                        Connection.Close();
                        if (result == "")
                        {
                            dbU.ExecuteScalar("Insert into " + datatable + " select * from " + datatable  +"_TMP");
                            dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set RecordsNum =" +
                                              dt1.Rows.Count + ", cassreceivedate = GETDATE(), TableName = '" + datatable +
                                              "', Processed = 'Y'" +
                                              ",RecordsOk = " + backfromBCC.Rows.Count +
                                              ",Recordsnondeliverable = " + totinerror + " where filename = '" + fName + "'");
                        }
                    }
                }
            }
            else
            {
                result = result + bccfile.FullName + "  not found..." + Environment.NewLine;
            }

            return result;
        }
    
        public DataTable loadCSV(DataTable columnsDef, int colnum, string fileName)
        {
            DataTable dt = new DataTable();
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            if (columnsDef.Rows.Count > 0)
            {
                foreach (DataRow item in columnsDef.Rows)
                {
                    dt.Columns.Add(item[0].ToString(), typeof(String));
                }
            }


            
            int coll = colnum;
            StreamReader sr = new StreamReader(fileName);
            string line = sr.ReadLine();                            //Regex.Replace(line, @"_$", String.Empty);
            if (line.EndsWith("_"))
                line.TrimEnd('_');
            string[] value = null;
            if (line.IndexOf("|") != -1)
                value = line.Split('|');
            else
                value = line.Split(',');

            DataRow dr = dt.NewRow();
            foreach (string dc in value)
            {
                dr[coll] = dc.ToString();
                coll++;
            }
            dt.Rows.Add(dr);
            
            while (!sr.EndOfStream)
            {
                string Linevalue = sr.ReadLine().ToString();
                if (Linevalue.IndexOf("|") != -1)
                    value = Linevalue.Split('|');
                else
                    value = Linevalue.Split(',');

                DataRow dr2 = dt.NewRow();
                coll = colnum;
                foreach (string dc in value)
                {
                    dr2[coll] = dc.ToString();
                    coll++;
                }
                dt.Rows.Add(dr2);
            }
            int recnum = 1;
            foreach (DataRow item in dt.Rows)
            {
                item["Recnum"] = recnum;
                item["FileName"] = fileInfo.Name;
                item["Importdate"] = DateTime.Now;
                recnum++;
            }
            return dt;
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
