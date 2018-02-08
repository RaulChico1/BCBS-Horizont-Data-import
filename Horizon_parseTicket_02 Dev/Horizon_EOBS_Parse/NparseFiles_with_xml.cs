using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Xml;
using System.Xml.Linq;
using System.Configuration;

namespace Horizon_EOBS_Parse
{
    public class NparseFiles_with_xml
    {

        DBUtility dbU;
        DataTable dt = new DataTable();
        List<string> Valores = new List<string>();


        private static DataTable data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Seqnum", typeof(Int32));
            newt.Columns.Add("ZIP");
            newt.Columns.Add("Fname");
            newt.Columns.Add("Pages");
            newt.Columns.Add("FileInXML");
            return newt;
        }

        public void parse_all_OEINV(string DirLocal)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string results = "";

            DirectoryInfo originalZIPs = new DirectoryInfo(DirLocal + @"from_FTP");
            FileInfo[] FilesZIP = originalZIPs.GetFiles("OEINV*.zip");
            if (FilesZIP.Count() > 0)
            {
                foreach (FileInfo file in FilesZIP)
                {
                    if (file.Name.IndexOf("__OEINV") == 0)
                    { }
                    else
                    {
                        results = parse_OEINV(file.FullName.ToString(), DirLocal);
                        if (results == "")
                        {
                            File.Move(file.FullName, ProcessVars.OtherProcessed +  file.Name);
                            File.Copy(file.FullName.Replace(".zip", ".csv"), ProcessVars.OtherProcessed + file.Name.Replace(".zip", ".csv"));
                        }
                    }

                }
            }

        }
        public string parse_OEINV(string filename, string DirLocal)
        {


            int totf = 0;
            DataTable newData = null;
            DataTable fromPDF = null;
            DataTable datafromPdfs = data_Table();
            datafromPdfs.Columns.Add("pdfName");
            datafromPdfs.Columns.Add("pdfPages");
            datafromPdfs.Columns.Add("pdfAccount");
            datafromPdfs.Columns.Add("pdfInvoice");
            string justXMLname = "";
            string xmlName = "";
            string zipName = "";
            int linenum = 0;
            string errors = "";
            try
            {
                if (Directory.Exists(DirLocal + "\\from_FTP\\tmp"))
                {
                    DirectoryInfo dir = new DirectoryInfo(DirLocal + "\\from_FTP\\tmp");

                    foreach (FileInfo fi in dir.GetFiles())
                    {
                        fi.IsReadOnly = false;
                        fi.Delete();
                    }
                    //Directory.Delete(DirLocal + "\\tmp");
                }
                Directory.CreateDirectory(DirLocal + "\\from_FTP\\tmp");
                Parse_Inv_pdf parseSimple = new Parse_Inv_pdf();
                FileInfo fileinfo = new FileInfo(filename);
                zipName = fileinfo.Name;
                using (ZipArchive archive = ZipFile.OpenRead(filename))
                {

                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                        {
                            if (File.Exists(ProcessVars.InputDirectory + "from_FTP\\" + entry.FullName))
                                File.Delete(ProcessVars.InputDirectory + "from_FTP\\" + entry.FullName);
                            justXMLname = entry.FullName;
                            xmlName = Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName);
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName));
                            totf++;
                        }
                        else
                        {
                            var row = datafromPdfs.NewRow();
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                            linenum++;
                            row["Seqnum"] = linenum;
                            row["ZIP"] = filename.Replace("IN/", "");
                            row["Fname"] = entry.Name.ToString();
                            int totPages = 0;
                            PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\from_FTP\\tmp", entry.FullName));
                            totPages = readerP.NumberOfPages;
                            row["Pages"] = totPages.ToString();
                            row["FileInXML"] = "Y";

                            fromPDF = parseSimple.evaluate_IndividualPdf(Path.Combine(ProcessVars.InputDirectory + "\\from_FTP\\tmp", entry.FullName));
                            row["pdfName"] = fromPDF.Rows[0][0].ToString();
                            row["pdfPages"] = fromPDF.Rows[0][1].ToString();
                            row["pdfAccount"] = fromPDF.Rows[0][2].ToString();
                            row["pdfInvoice"] = fromPDF.Rows[0][3].ToString();
                            datafromPdfs.Rows.Add(row);
                            readerP.Close();
                        }
                    }
                    if (xmlName.Length > 1)
                    {
                        DataTable dataExist = dbU.ExecuteDataTable("Select XmlName, convert(date,importdate), count(*) as records from HOR_Parse_OEINV where XmlName = '" + justXMLname + "' group by  XmlName, convert(date,importdate)");
                        if (dataExist.Rows.Count > 0)
                        {
                            dbU.ExecuteNonQuery("delete from HOR_Parse_OEINV where XmlName = '" + justXMLname + "'");
                            DataTable dataExist2 = dbU.ExecuteDataTable("Select Filename, convert(date,importdate) from HOR_parse_files_to_CASS where Filename = '" + justXMLname + "' ");
                            if (dataExist2.Rows.Count > 0)
                            {
                                dbU.ExecuteNonQuery("delete from HOR_parse_files_to_CASS where Filename = '" + justXMLname + "'");
                            }

                        }
                        DataTable dataExist3 = dbU.ExecuteDataTable("Select recnum, Description, convert(date,datetime) from HOR_parse_SEQ where Description = '" + justXMLname + "' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "' and tablename = 'HOR_Parse_OEINV'");
                        if (dataExist3.Rows.Count > 0)
                        {
                            dbU.ExecuteNonQuery("delete from HOR_parse_SEQ where recnum  = '" + dataExist3.Rows[0][0] + "'");
                        }

                        try
                        {
                            DataSet ds = new DataSet();
                            XmlReader xmlFile;
                            xmlFile = XmlReader.Create(xmlName, new XmlReaderSettings());
                            ds.ReadXml(xmlFile);

                            newData = ds.Tables[0].Clone();
                            newData.Columns.Add("FlightNumber");
                            newData.Columns.Add("Bind");
                            newData.Columns.Add("Filename");
                            newData.Columns.Add("NumberOfCopy");
                            newData.Columns.Add("NameLine1");
                            newData.Columns.Add("NameLine2");
                            newData.Columns.Add("AddressLine1");
                            newData.Columns.Add("AddressLine2");
                            newData.Columns.Add("AddressLine3");
                            newData.Columns.Add("city");
                            newData.Columns.Add("State");
                            newData.Columns.Add("Zip");
                            newData.Columns.Add("OSeq");
                            newData.Columns.Add("Recnum");
                            newData.Columns.Add("XmlName");
                            newData.Columns.Add("ImportDate");
                            newData.Columns.Add("Jobname");
                            newData.Columns.Add("JulianDate");
                            newData.Columns.Add("TStamp");
                            newData.Columns.Add("WhereTo");
                            newData.Columns.Add("JobClass");

                            newData.Columns.Add("pdfName");
                            newData.Columns.Add("pdfPages");
                            newData.Columns.Add("pdfAccount");
                            newData.Columns.Add("pdfInvoice");
                            newData.Columns.Add("ZipName");
                            int i = 0;

                            for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                var row2 = newData.NewRow();
                                row2["batchId"] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
                                row2["batchTransactionId"] = ds.Tables[0].Rows[i].ItemArray[1].ToString();
                                row2["documentDate"] = ds.Tables[0].Rows[i].ItemArray[2].ToString();
                                row2["ClientTransactionID"] = ds.Tables[0].Rows[i].ItemArray[3].ToString();
                                row2["IncludeCoverPage"] = ds.Tables[0].Rows[i].ItemArray[4].ToString();
                                row2["DeliveryMode"] = ds.Tables[0].Rows[i].ItemArray[5].ToString();
                                row2["DocumentIdentifier"] = ds.Tables[0].Rows[i].ItemArray[6].ToString();
                                row2["Group"] = ds.Tables[0].Rows[i].ItemArray[7].ToString();
                                row2["CCID"] = ds.Tables[0].Rows[i].ItemArray[8].ToString();
                                row2["EffectiveDate"] = ds.Tables[0].Rows[i].ItemArray[9].ToString();
                                row2["FlightNumber"] = ds.Tables[2].Rows[i].ItemArray[0].ToString();
                                row2["Bind"] = ds.Tables[2].Rows[i].ItemArray[1].ToString();
                                row2["Filename"] = ds.Tables[3].Rows[i].ItemArray[0].ToString();
                                row2["NumberOfCopy"] = ds.Tables[3].Rows[i].ItemArray[1].ToString();
                                row2["NameLine1"] = ds.Tables[5].Rows[i].ItemArray[0].ToString();
                                row2["NameLine2"] = ds.Tables[5].Rows[i].ItemArray[1].ToString();
                                row2["AddressLine1"] = ds.Tables[5].Rows[i].ItemArray[2].ToString();
                                row2["AddressLine2"] = ds.Tables[5].Rows[i].ItemArray[3].ToString();
                                row2["AddressLine3"] = ds.Tables[5].Rows[i].ItemArray[4].ToString();
                                row2["City"] = ds.Tables[5].Rows[i].ItemArray[5].ToString();
                                row2["State"] = ds.Tables[5].Rows[i].ItemArray[6].ToString();
                                row2["Zip"] = ds.Tables[5].Rows[i].ItemArray[7].ToString();
                                row2["Oseq"] = i.ToString();
                                row2["Recnum"] = i.ToString();
                                row2["XmlName"] = justXMLname;
                                row2["ImportDate"] = DateTime.Now.ToString("yyyy-MM-dd");
                                row2["ZipName"] = zipName;

                                //command = new SqlCommand(sql, connection);
                                //adpter.InsertCommand = command;
                                //adpter.InsertCommand.ExecuteNonQuery();
                                newData.Rows.Add(row2);
                            }



                        }
                        catch (Exception ex)
                        {
                            totf = 0;
                        }

                        if (totf > 0)
                        {
                            newData.PrimaryKey = new DataColumn[] { newData.Columns["Filename"] };

                            datafromPdfs.PrimaryKey = new DataColumn[] { datafromPdfs.Columns["pdfName"] };

                            foreach (DataRow dRNew in datafromPdfs.Rows)
                            {
                                DataRow row = null;
                                try
                                {
                                    row = newData.Rows.Find(dRNew["pdfName"].ToString());
                                }
                                catch (MissingPrimaryKeyException)
                                {
                                    row = newData.Select("Filename=" + dRNew["pdfName"] + "'").First();
                                }
                                if (row != null)
                                {
                                    row["pdfName"] = dRNew["pdfName"];
                                    row["pdfPages"] = dRNew["pdfPages"];
                                    row["pdfAccount"] = dRNew["pdfAccount"];
                                    row["pdfInvoice"] = dRNew["pdfInvoice"];
                                }
                            }

                            GlobalVar.dbaseName = "BCBS_Horizon";
                            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


                            int GRecnum = 1;

                            SqlParameter[] sqlParams2;
                            sqlParams2 = null;
                            sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", newData.Rows.Count), 
                            
                                new SqlParameter("@FileName", justXMLname), new SqlParameter("@TableName", "HOR_Parse_OEINV") };
                            dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
                            DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + justXMLname + "' and tablename = 'HOR_Parse_OEINV' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
                            if (afterUpdateSeq.Rows.Count == 1)
                                GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - newData.Rows.Count + 1;
                            else
                            {
                                errors = "more than 1 record for file " + justXMLname + " in HOR_parse_SEQ";
                                SendMails sendmail = new SendMails();
                                sendmail.SendMailError("error in HOR_upd_Recnum_beforeTMP ", "Error reading recnum after update", "\n\n" + "Error table: HOR_Parse_OEINV,   file " + justXMLname, "");
                            }
                            if (errors == "")
                            {
                                foreach (DataRow row in newData.Rows)
                                {
                                    row["Recnum"] = GRecnum;

                                    GRecnum++;
                                }

                                dbU.ExecuteScalar("delete from HOR_Parse_OEINV_tmp");

                                newData.Columns.Remove("Index_Id");



                                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                                Connection.Open();

                                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                                {
                                    bulkCopy.DestinationTableName = "[dbo].[HOR_Parse_OEINV_tmp]";

                                    try
                                    {
                                        bulkCopy.BatchSize = newData.Rows.Count;
                                        bulkCopy.BulkCopyTimeout = 0;
                                        bulkCopy.WriteToServer(newData);
                                    }
                                    catch (Exception ex)
                                    {
                                        errors = errors + ex.Message;
                                    }
                                }
                                Connection.Close();
                                if (errors == "")
                                {
                                    dbU.ExecuteScalar("Insert into HOR_Parse_OEINV select * from HOR_Parse_OEINV_tmp");
                                    string strsql = "select Recnum, '' as digUId,FileName as FName,'' as artifactId,'' as LetterName, NameLine1 as CoverPageName,NameLine2 as CoverpageAddress1,AddressLine1 as CoverpageAddress2,AddressLine2 as CoverpageAddress3,AddressLine3 as CoverpageAddress4,City,State,Zip,'' as  BRE, '' as TOD, '' as DL" +
                                        " from HOR_Parse_OEINV where XmlName = '" + justXMLname + "' order by recnum";

                                    DataTable tocsv = dbU.ExecuteDataTable(strsql);
                                    string Printname = DirLocal + "from_FTP\\" + justXMLname.Replace(".xml", ".csv");
                                    if (tocsv.Rows.Count > 0)
                                    {

                                        if (File.Exists(Printname))
                                            File.Delete(Printname);
                                        createCSV printcsv = new createCSV();
                                        printcsv.printCSV_fullProcess(Printname, tocsv, "", "");
                                    }

                                    //File.Move(filename, DirLocal + @"from_FTP\\" + zipName.Replace("IN/", "").Replace("OEINV", "__OEINV"));
                                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, " +
                                        "TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                              newData.Rows.Count + ",'" + datafromPdfs.Rows.Count.ToString() + "','" + justXMLname + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + justXMLname + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','HOR_Parse_OEINV','No CASS','No Sysout','','Receive','Y','" + GlobalVar.DateofProcess + "','" + zipName + "')");


                                }
                                else
                                {
                                    var error = errors;
                                }
                            }
                        }
                    }
                }


            }

            catch (Exception ex2)
            {
                errors = ex2.Message;
            }
            return errors;
        }
        public void parse_all_MAPDP(string DirLocal)
            {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string results = "";

            DirectoryInfo originalZIPs = new DirectoryInfo(DirLocal + @"from_FTP");
            FileInfo[] FilesZIP = originalZIPs.GetFiles("MAPDP*.zip");
            if (FilesZIP.Count() > 0)
                {
                foreach (FileInfo file in FilesZIP)
                    {
                    if (file.Name.IndexOf("__MAPDP") == 0)
                    { }
                    else
                        {
                        string checkFile = "select convert(date,importdate) from HOR_Parse_MAPDP where zipname = '" + file.Name + "'";
                        DataTable processed = dbU.ExecuteDataTable(checkFile);
                        if (processed.Rows.Count == 0)
                            {
                            results = parse_MAPDP(file.FullName.ToString(), DirLocal);
                            if (results == "")
                                {
                                if (File.Exists(ProcessVars.OtherProcessed + file.Name))
                                    File.Delete(ProcessVars.OtherProcessed + file.Name);

                                //File.Copy(file.FullName, ProcessVars.OtherProcessed + file.Name);
                                File.Move(file.FullName, ProcessVars.OtherProcessed + file.Name);
                                }
                            else
                                {
                                var msg = "can not process this " + results;
                                }
                            }
                        else
                            {
                            string upderror = "select lognum, commentProcess from HOR_parse_Log_VLTrader where filename = '" + file.Name + "' order by lognum";
                            DataTable processedErr = dbU.ExecuteDataTable(upderror);
                            if (processedErr.Rows.Count > 0)
                                {
                                foreach (DataRow item in processedErr.Rows)
                                    {
                                    string newError = item[1].ToString() + "~" + "already processed on " + processed.Rows[0][0].ToString() + ", Tried: " + DateTime.Now.ToString("yyyy-MM-dd");
                                    string toUpdate = "Update HOR_parse_Log_VLTrader set commentProcess = '" + newError + "' where lognum = '" + item[0].ToString() + "'";
                                    dbU.ExecuteScalar(toUpdate);
                                    File.Move(file.FullName, file.DirectoryName + "\\__error_already_processed" + file.Name);
                                    }
                                }
                            }
                        }
                    }

                }
            }
        public string parse_MAPDP(string filename, string DirLocal)
        {


            int totf = 0;
            DataTable newData = null;
            DataTable fromPDF = null;
            DataTable datafromPdfs = data_Table();
            int totpagesPDFs = 0;

            datafromPdfs.Columns.Add("PagesPDF", typeof(Int32));
            //datafromPdfs.Columns.Add("pdfPages");
            //datafromPdfs.Columns.Add("pdfAccount");
            //datafromPdfs.Columns.Add("pdfInvoice");
            string justXMLname = "";
            string xmlName = "";
            string zipName = "";
            int linenum = 0;
            string errors = "";
            try
            {
                if (Directory.Exists(DirLocal + "\\from_FTP\\tmp"))
                {
                    DirectoryInfo dir = new DirectoryInfo(DirLocal + "\\from_FTP\\tmp");

                    foreach (FileInfo fi in dir.GetFiles())
                    {
                        fi.IsReadOnly = false;
                        fi.Delete();
                    }
                    //Directory.Delete(DirLocal + "\\tmp");
                }
                Directory.CreateDirectory(DirLocal + "\\from_FTP\\tmp");
                Parse_Inv_pdf parseSimple = new Parse_Inv_pdf();
                FileInfo fileinfo = new FileInfo(filename);
                zipName = fileinfo.Name;
                using (ZipArchive archive = ZipFile.OpenRead(filename))
                {

                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                        {
                            if (File.Exists(ProcessVars.InputDirectory + "from_FTP\\" + entry.FullName))
                                File.Delete(ProcessVars.InputDirectory + "from_FTP\\" + entry.FullName);
                            justXMLname = entry.FullName;
                            xmlName = Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName);
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName));
                            totf++;
                        }
                        else
                        {
                            var row = datafromPdfs.NewRow();
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                            linenum++;
                            row["Seqnum"] = linenum;
                            row["ZIP"] = filename.Replace("IN/", "");
                            row["Fname"] = entry.Name.ToString();
                            int totPages = 0;
                            PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\from_FTP\\tmp", entry.FullName));
                            readerP.Close();
                            totPages = readerP.NumberOfPages;
                            row["Pages"] = totPages.ToString();
                            row["FileInXML"] = "Y";
                            row["PagesPDF"] = totPages;
                            totpagesPDFs = totpagesPDFs + totPages;

                            datafromPdfs.Rows.Add(row);
                        }
                    }
                    if (xmlName.Length > 1)
                    {
                        DataTable dataExist = dbU.ExecuteDataTable("Select XmlName, convert(date,importdate), count(*) as records from HOR_Parse_MAPDP where XmlName = '" + justXMLname + "' group by  XmlName, convert(date,importdate)");
                        if (dataExist.Rows.Count > 0)
                        {
                            dbU.ExecuteNonQuery("delete from HOR_Parse_MAPDP where XmlName = '" + justXMLname + "'");
                            DataTable dataExist2 = dbU.ExecuteDataTable("Select Filename, convert(date,importdate) from HOR_parse_files_to_CASS where Filename = '" + justXMLname + "' ");
                            if (dataExist2.Rows.Count > 0)
                            {
                                dbU.ExecuteNonQuery("delete from HOR_parse_files_to_CASS where Filename = '" + justXMLname + "'");
                            }

                        }
                        DataTable dataExist3 = dbU.ExecuteDataTable("Select recnum, Description, convert(date,datetime) from HOR_parse_SEQ where Description = '" + justXMLname + "' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "' and tablename = 'HOR_Parse_MAPDP'");
                        if (dataExist3.Rows.Count > 0)
                        {
                            dbU.ExecuteNonQuery("delete from HOR_parse_SEQ where recnum  = '" + dataExist3.Rows[0][0] + "'");
                        }

                        try
                        {
                            DataSet ds = new DataSet();
                            XmlReader xmlFile;
                            xmlFile = XmlReader.Create(xmlName, new XmlReaderSettings());
                            ds.ReadXml(xmlFile);

                            newData = ds.Tables[0].Clone();
                            newData.Columns.Add("FlightNumber");
                            newData.Columns.Add("Bind");
                            newData.Columns.Add("Filename");
                            newData.Columns.Add("SequenceOrder");

                            newData.Columns.Add("NumberOfCopy");
                            newData.Columns.Add("NameLine1");
                            newData.Columns.Add("NameLine2");
                            newData.Columns.Add("AddressLine1");
                            newData.Columns.Add("AddressLine2");
                            newData.Columns.Add("city");
                            newData.Columns.Add("State");
                            newData.Columns.Add("Zip");
                            newData.Columns.Add("FullName");
                            newData.Columns.Add("OSeq");
                            newData.Columns.Add("Recnum");
                            newData.Columns.Add("XmlName");
                            newData.Columns.Add("ImportDate");
                            newData.Columns.Add("Jobname");
                            newData.Columns.Add("JulianDate");
                            newData.Columns.Add("TStamp");
                            newData.Columns.Add("WhereTo");
                            newData.Columns.Add("JobClass");

                            
                            //newData.Columns.Add("pdfPages");
                            //newData.Columns.Add("pdfAccount");
                            //newData.Columns.Add("pdfInvoice");
                            newData.Columns.Add("ZipName");
                            newData.Columns.Add("PagesPDF");
                            int i = 0;

                            for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                var row2 = newData.NewRow();
                                row2["ClientTransactionID"] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
                                row2["documentDate"] = ds.Tables[0].Rows[i].ItemArray[1].ToString();
                                row2["batchId"] = ds.Tables[0].Rows[i].ItemArray[2].ToString();
                                row2["TransactionNo"] = ds.Tables[0].Rows[i].ItemArray[3].ToString();

                                row2["FlightNumber"] = ds.Tables[2].Rows[i].ItemArray[0].ToString();
                                row2["Bind"] = ds.Tables[2].Rows[i].ItemArray[1].ToString();
                                row2["Filename"] = ds.Tables[3].Rows[i].ItemArray[0].ToString();
                                row2["SequenceOrder"] = ds.Tables[3].Rows[i].ItemArray[1].ToString();
                                row2["NumberOfCopy"] = ds.Tables[3].Rows[i].ItemArray[2].ToString();
                                row2["NameLine1"] = ds.Tables[5].Rows[i].ItemArray[0].ToString();
                                row2["NameLine2"] = ds.Tables[5].Rows[i].ItemArray[1].ToString();
                                row2["AddressLine1"] = ds.Tables[5].Rows[i].ItemArray[2].ToString();
                                row2["AddressLine2"] = ds.Tables[5].Rows[i].ItemArray[3].ToString();
                                row2["City"] = ds.Tables[5].Rows[i].ItemArray[4].ToString();
                                row2["State"] = ds.Tables[5].Rows[i].ItemArray[5].ToString();
                                row2["Zip"] = ds.Tables[5].Rows[i].ItemArray[6].ToString();
                                row2["FullName"] = ds.Tables[5].Rows[i].ItemArray[7].ToString();
                                row2["Oseq"] = i.ToString();
                                row2["Recnum"] = i.ToString();
                                row2["XmlName"] = justXMLname;
                                row2["ImportDate"] = DateTime.Now.ToString("yyyy-MM-dd");
                                row2["ZipName"] = zipName;
                                
                                //command = new SqlCommand(sql, connection);
                                //adpter.InsertCommand = command;
                                //adpter.InsertCommand.ExecuteNonQuery();
                                newData.Rows.Add(row2);
                            }



                        }
                        catch (Exception ex)
                        {
                            totf = 0;
                        }

                        if (totf > 0)
                        {
                            newData.PrimaryKey = new DataColumn[] { newData.Columns["Filename"] };

                            datafromPdfs.PrimaryKey = new DataColumn[] { datafromPdfs.Columns["fName"] };

                            foreach (DataRow dRNew in datafromPdfs.Rows)
                            {
                                DataRow row = null;
                                try
                                {
                                    row = newData.Rows.Find(dRNew["fname"].ToString());
                                }
                                catch (MissingPrimaryKeyException)
                                {
                                    row = newData.Select("Filename=" + dRNew["fname"] + "'").First();
                                }
                                if (row != null)
                                {
                                    row["PagesPDF"] = dRNew["PagesPDF"];
                                   
                                }
                            }

                            GlobalVar.dbaseName = "BCBS_Horizon";
                            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


                            int GRecnum = 1;

                            SqlParameter[] sqlParams2;
                            sqlParams2 = null;
                            sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", newData.Rows.Count), 
                            
                                new SqlParameter("@FileName", justXMLname), new SqlParameter("@TableName", "HOR_Parse_MAPDP") };
                            dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
                            DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + justXMLname + "' and tablename = 'HOR_Parse_MAPDP' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
                            if (afterUpdateSeq.Rows.Count == 1)
                                GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - newData.Rows.Count + 1;
                            else
                            {
                                errors = "more than 1 record for file " + justXMLname + " in HOR_parse_SEQ";
                                SendMails sendmail = new SendMails();
                                sendmail.SendMailError("error in HOR_upd_Recnum_beforeTMP ", "Error reading recnum after update", "\n\n" + "Error table: HOR_Parse_MAPDP,   file " + justXMLname, "");
                            }
                            if (errors == "")
                            {
                                foreach (DataRow row in newData.Rows)
                                {
                                    row["Recnum"] = GRecnum;

                                    GRecnum++;
                                }

                                dbU.ExecuteScalar("delete from HOR_Parse_MAPDP_tmp");

                                //newData.Columns.Remove("Index_Id");

                               // newData.Columns["pagesPDF"].SetOrdinal(28);

                                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                                Connection.Open();

                                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                                {
                                    bulkCopy.DestinationTableName = "[dbo].[HOR_Parse_MAPDP_tmp]";

                                    try
                                    {
                                        bulkCopy.BatchSize = newData.Rows.Count;
                                        bulkCopy.BulkCopyTimeout = 0;
                                        bulkCopy.WriteToServer(newData);
                                    }
                                    catch (Exception ex)
                                    {
                                        errors = errors + ex.Message;
                                    }
                                }
                                Connection.Close();
                                if (errors == "")
                                {
                                    dbU.ExecuteScalar("Insert into HOR_Parse_MAPDP select * from HOR_Parse_MAPDP_tmp");
                                    string strsql = "select Recnum, '' as digUId,FileName as FName,'' as artifactId,'' as LetterName, NameLine1 as CoverPageName,NameLine2 as CoverpageAddress1,AddressLine1 as CoverpageAddress2,AddressLine2 as CoverpageAddress3,'' as CoverpageAddress4,City,State,Zip,'' as  BRE, '' as TOD, '' as DL" +
                                        " from HOR_Parse_MAPDP where XmlName = '" + justXMLname + "' order by recnum";

                                    DataTable tocsv = dbU.ExecuteDataTable(strsql);
                                    string Printname = DirLocal + "from_FTP\\" + justXMLname.Replace(".xml", ".csv");
                                    if (tocsv.Rows.Count > 0)
                                    {

                                        if (File.Exists(Printname))
                                            File.Delete(Printname);
                                        createCSV printcsv = new createCSV();
                                        printcsv.printCSV_fullProcess(Printname, tocsv, "", "");
                                    }

                                    //File.Move(filename, DirLocal + @"from_FTP\\" + zipName.Replace("IN/", "").Replace("OEINV", "__OEINV"));
                                    File.Copy(Printname, ProcessVars.OtherProcessed + justXMLname.Replace(".xml", ".csv"),true);
                                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, " +
                                        "TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                              newData.Rows.Count + ",'" + totpagesPDFs.ToString() + "','" + justXMLname + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + justXMLname + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','HOR_Parse_MAPDP','No CASS','No Sysout','','Receive','Y','" + GlobalVar.DateofProcess + "','" + zipName + "')");


                                }
                                else
                                {
                                    var error = errors;
                                }
                            }
                        }
                    }
                }


            }

            catch (Exception ex2)
            {
                errors = ex2.Message;
            }
            return errors;
        }

        public void parse_all_SVNJCD(string DirLocal)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string results = "";

            DirectoryInfo originalZIPs = new DirectoryInfo(DirLocal + @"from_FTP");
            FileInfo[] FilesZIP = originalZIPs.GetFiles("SVNJCD*.zip");
            if (FilesZIP.Count() > 0)
            {
                foreach (FileInfo file in FilesZIP)
                {
                    if (file.Name.IndexOf("__SVNJCD") == 0)
                    { }
                    else
                    {
                        results = parse_SVNJCD(file.FullName.ToString(), DirLocal);
                        if (results == "")
                        {
                            File.Move(file.FullName, ProcessVars.OtherProcessed + file.Name);
                            File.Copy(file.FullName.Replace(".zip", ".csv"), ProcessVars.OtherProcessed + file.Name.Replace(".zip", ".csv"));
                        }
                    }

                }
            }

        }
        public string parse_SVNJCD(string filename, string DirLocal)
        {


            int totf = 0;
            DataTable newData = null;
            DataTable fromPDF = null;
            DataTable datafromPdfs = data_Table();
            datafromPdfs.Columns.Add("pdfName");
            datafromPdfs.Columns.Add("pdfPages");
            datafromPdfs.Columns.Add("pdfAccount");
            datafromPdfs.Columns.Add("pdfInvoice");
            string justXMLname = "";
            string xmlName = "";
            string zipName = "";
            int linenum = 0;
            string errors = "";
            try
            {
                if (Directory.Exists(DirLocal + "\\from_FTP\\tmp"))
                {
                    DirectoryInfo dir = new DirectoryInfo(DirLocal + "\\from_FTP\\tmp");

                    foreach (FileInfo fi in dir.GetFiles())
                    {
                        fi.IsReadOnly = false;
                        fi.Delete();
                    }
                    //Directory.Delete(DirLocal + "\\tmp");
                }
                Directory.CreateDirectory(DirLocal + "\\from_FTP\\tmp");
                Parse_Inv_pdf parseSimple = new Parse_Inv_pdf();
                FileInfo fileinfo = new FileInfo(filename);
                zipName = fileinfo.Name;
                using (ZipArchive archive = ZipFile.OpenRead(filename))
                {

                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                        {
                            if (File.Exists(ProcessVars.InputDirectory + "from_FTP\\" + entry.FullName))
                                File.Delete(ProcessVars.InputDirectory + "from_FTP\\" + entry.FullName);
                            justXMLname = entry.FullName;
                            xmlName = Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName);
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP", entry.FullName));
                            totf++;
                        }
                        else
                        {
                            var row = datafromPdfs.NewRow();
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                            linenum++;
                            row["Seqnum"] = linenum;
                            row["ZIP"] = filename.Replace("IN/", "");
                            row["Fname"] = entry.Name.ToString();
                            int totPages = 0;
                            PdfReader readerP = new PdfReader(Path.Combine(DirLocal + "\\from_FTP\\tmp", entry.FullName));
                            totPages = readerP.NumberOfPages;
                            row["Pages"] = totPages.ToString();
                            row["FileInXML"] = "Y";

                            fromPDF = parseSimple.evaluate_IndividualPdf(Path.Combine(ProcessVars.InputDirectory + "\\from_FTP\\tmp", entry.FullName));
                            if (fromPDF.Rows.Count > 0)
                            {
                                row["pdfName"] = fromPDF.Rows[0][0].ToString();
                                row["pdfPages"] = fromPDF.Rows[0][1].ToString();
                                row["pdfAccount"] = fromPDF.Rows[0][2].ToString();
                                row["pdfInvoice"] = fromPDF.Rows[0][3].ToString();
                                datafromPdfs.Rows.Add(row);
                            }
                            else
                            {
                                datafromPdfs.Rows.Add(row);
                            }
                        }
                    }
                    if (xmlName.Length > 1)
                    {
                        DataTable dataExist = dbU.ExecuteDataTable("Select XmlName, convert(date,importdate), count(*) as records from HOR_Parse_OEINV where XmlName = '" + justXMLname + "' group by  XmlName, convert(date,importdate)");
                        if (dataExist.Rows.Count > 0)
                        {
                            dbU.ExecuteNonQuery("delete from HOR_parse_SVNJCD where XmlName = '" + justXMLname + "'");
                            DataTable dataExist2 = dbU.ExecuteDataTable("Select Filename, convert(date,importdate) from HOR_parse_files_to_CASS where Filename = '" + justXMLname + "' ");
                            if (dataExist2.Rows.Count > 0)
                            {
                                dbU.ExecuteNonQuery("delete from HOR_parse_files_to_CASS where Filename = '" + justXMLname + "'");
                            }

                        }
                        DataTable dataExist3 = dbU.ExecuteDataTable("Select recnum, Description, convert(date,datetime) from HOR_parse_SEQ where Description = '" + justXMLname + "' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "' and tablename = 'HOR_Parse_OEINV'");
                        if (dataExist3.Rows.Count > 0)
                        {
                            dbU.ExecuteNonQuery("delete from HOR_parse_SEQ where recnum  = '" + dataExist3.Rows[0][0] + "'");
                        }

                        try
                        {
                            DataSet ds = new DataSet();
                            XmlReader xmlFile;
                            xmlFile = XmlReader.Create(xmlName, new XmlReaderSettings());
                            ds.ReadXml(xmlFile);

                            newData = ds.Tables[0].Clone();
                            newData.Columns.Add("FlightNumber");
                            newData.Columns.Add("Bind");
                            newData.Columns.Add("Filename");
                            newData.Columns.Add("NumberOfCopy");
                            newData.Columns.Add("NameLine1");
                            newData.Columns.Add("NameLine2");
                            newData.Columns.Add("AddressLine1");
                            newData.Columns.Add("AddressLine2");
                            newData.Columns.Add("AddressLine3");
                            newData.Columns.Add("city");
                            newData.Columns.Add("State");
                            newData.Columns.Add("Zip");
                            newData.Columns.Add("OSeq");
                            newData.Columns.Add("Recnum");
                            newData.Columns.Add("XmlName");
                            newData.Columns.Add("ImportDate");
                            newData.Columns.Add("Jobname");
                            newData.Columns.Add("JulianDate");
                            newData.Columns.Add("TStamp");
                            newData.Columns.Add("WhereTo");
                            newData.Columns.Add("JobClass");

                            newData.Columns.Add("pdfName");
                            newData.Columns.Add("pdfPages");
                            newData.Columns.Add("pdfAccount");
                            newData.Columns.Add("pdfInvoice");
                            newData.Columns.Add("ZipName");
                            int i = 0;

                            for (i = 0; i <= ds.Tables[0].Rows.Count - 1; i++)
                            {
                                var row2 = newData.NewRow();
                                row2["batchId"] = ds.Tables[0].Rows[i].ItemArray[0].ToString();
                                row2["batchTransactionId"] = ds.Tables[0].Rows[i].ItemArray[1].ToString();
                                row2["documentDate"] = ds.Tables[0].Rows[i].ItemArray[2].ToString();
                                row2["ClientTransactionID"] = ds.Tables[0].Rows[i].ItemArray[3].ToString();
                                row2["IncludeCoverPage"] = ds.Tables[0].Rows[i].ItemArray[4].ToString();
                                row2["DeliveryMode"] = ds.Tables[0].Rows[i].ItemArray[5].ToString();
                                row2["DocumentIdentifier"] = ds.Tables[0].Rows[i].ItemArray[6].ToString();
                                row2["Group"] = ds.Tables[0].Rows[i].ItemArray[7].ToString();
                                row2["CCID"] = ds.Tables[0].Rows[i].ItemArray[8].ToString();
                                row2["EffectiveDate"] = ds.Tables[0].Rows[i].ItemArray[9].ToString();
                                row2["FlightNumber"] = ds.Tables[2].Rows[i].ItemArray[0].ToString();
                                row2["Bind"] = ds.Tables[2].Rows[i].ItemArray[1].ToString();
                                row2["Filename"] = ds.Tables[3].Rows[i].ItemArray[0].ToString();
                                row2["NumberOfCopy"] = ds.Tables[3].Rows[i].ItemArray[1].ToString();
                                row2["NameLine1"] = ds.Tables[5].Rows[i].ItemArray[0].ToString();
                                row2["NameLine2"] = ds.Tables[5].Rows[i].ItemArray[1].ToString();
                                row2["AddressLine1"] = ds.Tables[5].Rows[i].ItemArray[2].ToString();
                                row2["AddressLine2"] = ds.Tables[5].Rows[i].ItemArray[3].ToString();
                                row2["AddressLine3"] = ds.Tables[5].Rows[i].ItemArray[4].ToString();
                                row2["City"] = ds.Tables[5].Rows[i].ItemArray[5].ToString();
                                row2["State"] = ds.Tables[5].Rows[i].ItemArray[6].ToString();
                                row2["Zip"] = ds.Tables[5].Rows[i].ItemArray[7].ToString();
                                row2["Oseq"] = i.ToString();
                                row2["Recnum"] = i.ToString();
                                row2["XmlName"] = justXMLname;
                                row2["ImportDate"] = DateTime.Now.ToString("yyyy-MM-dd");
                                row2["ZipName"] = zipName;

                                //command = new SqlCommand(sql, connection);
                                //adpter.InsertCommand = command;
                                //adpter.InsertCommand.ExecuteNonQuery();
                                newData.Rows.Add(row2);
                            }



                        }
                        catch (Exception ex)
                        {
                            totf = 0;
                        }

                        if (totf > 0)
                        {
                            newData.PrimaryKey = new DataColumn[] { newData.Columns["Filename"] };

                            datafromPdfs.PrimaryKey = new DataColumn[] { datafromPdfs.Columns["pdfName"] };

                            foreach (DataRow dRNew in datafromPdfs.Rows)
                            {
                                DataRow row = null;
                                try
                                {
                                    row = newData.Rows.Find(dRNew["pdfName"].ToString());
                                }
                                catch (MissingPrimaryKeyException)
                                {
                                    row = newData.Select("Filename=" + dRNew["pdfName"] + "'").First();
                                }
                                if (row != null)
                                {
                                    row["pdfName"] = dRNew["pdfName"];
                                    row["pdfPages"] = dRNew["pdfPages"];
                                    row["pdfAccount"] = dRNew["pdfAccount"];
                                    row["pdfInvoice"] = dRNew["pdfInvoice"];
                                }
                            }

                            GlobalVar.dbaseName = "BCBS_Horizon";
                            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


                            int GRecnum = 1;

                            SqlParameter[] sqlParams2;
                            sqlParams2 = null;
                            sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", newData.Rows.Count), 
                            
                                new SqlParameter("@FileName", justXMLname), new SqlParameter("@TableName", "HOR_Parse_OEINV") };
                            dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
                            DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + justXMLname + "' and tablename = 'HOR_Parse_OEINV' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
                            if (afterUpdateSeq.Rows.Count == 1)
                                GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - newData.Rows.Count + 1;
                            else
                            {
                                errors = "more than 1 record for file " + justXMLname + " in HOR_parse_SEQ";
                                SendMails sendmail = new SendMails();
                                sendmail.SendMailError("error in HOR_upd_Recnum_beforeTMP ", "Error reading recnum after update", "\n\n" + "Error table: HOR_Parse_OEINV,   file " + justXMLname, "");
                            }
                            if (errors == "")
                            {
                                foreach (DataRow row in newData.Rows)
                                {
                                    row["Recnum"] = GRecnum;

                                    GRecnum++;
                                }

                                dbU.ExecuteScalar("delete from HOR_Parse_OEINV_tmp");

                                newData.Columns.Remove("Index_Id");



                                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                                Connection.Open();

                                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                                {
                                    bulkCopy.DestinationTableName = "[dbo].[HOR_Parse_OEINV_tmp]";

                                    try
                                    {
                                        bulkCopy.BatchSize = newData.Rows.Count;
                                        bulkCopy.BulkCopyTimeout = 0;
                                        bulkCopy.WriteToServer(newData);
                                    }
                                    catch (Exception ex)
                                    {
                                        errors = errors + ex.Message;
                                    }
                                }
                                Connection.Close();
                                if (errors == "")
                                {
                                    dbU.ExecuteScalar("Insert into HOR_Parse_OEINV select * from HOR_Parse_OEINV_tmp");
                                    string strsql = "select Recnum, '' as digUId,FileName as FName,'' as artifactId,'' as LetterName, NameLine1 as CoverPageName,NameLine2 as CoverpageAddress1,AddressLine1 as CoverpageAddress2,AddressLine2 as CoverpageAddress3,AddressLine3 as CoverpageAddress4,City,State,Zip,'' as  BRE, '' as TOD, '' as DL" +
                                        " from HOR_Parse_OEINV where XmlName = '" + justXMLname + "' order by recnum";

                                    DataTable tocsv = dbU.ExecuteDataTable(strsql);
                                    string Printname = DirLocal + "from_FTP\\" + justXMLname.Replace(".xml", ".csv");
                                    if (tocsv.Rows.Count > 0)
                                    {

                                        if (File.Exists(Printname))
                                            File.Delete(Printname);
                                        createCSV printcsv = new createCSV();
                                        printcsv.printCSV_fullProcess(Printname, tocsv, "", "");
                                    }

                                    //File.Move(filename, DirLocal + @"from_FTP\\" + zipName.Replace("IN/", "").Replace("OEINV", "__OEINV"));
                                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, " +
                                        "TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                              newData.Rows.Count + ",'" + datafromPdfs.Rows.Count.ToString() + "','" + justXMLname + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + justXMLname + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','HOR_Parse_OEINV','No CASS','No Sysout','','Receive','Y','" + GlobalVar.DateofProcess + "','" + zipName + "')");


                                }
                                else
                                {
                                    var error = errors;
                                }
                            }
                        }
                    }
                }


            }

            catch (Exception ex2)
            {
                errors = ex2.Message;
            }
            return errors;
        }
        private static DataTable BuildDataTable(XElement x)
        {
            DataTable dt = new DataTable();

            dt.Columns.Add(new DataColumn(x.Name.ToString()));
            foreach (var d in x.Descendants())
            {
                DataRow drow = dt.NewRow();
                drow[0] = d.Value;
                dt.Rows.Add(drow);
            }

            return dt;
        }

        public void addToTable(int rowNumber)
        {
            var row = dt.NewRow();
            row["Recnum"] = rowNumber;
            for (int x = 1; x < (Valores.Count()); x++)
            {

                row[x - 1] = Valores[x].ToString();
            }
            dt.Rows.Add(row);
        }
        public static DataTable ConvertXmlNodeListToDataTable(XmlNodeList xnl)
        {
            DataTable dt = new DataTable();
            int TempColumn = 0;

            foreach (XmlNode node in xnl.Item(0).ChildNodes)
            {
                TempColumn++;
                DataColumn dc = new DataColumn(node.Name, System.Type.GetType("System.String"));
                if (dt.Columns.Contains(node.Name))
                {
                    dt.Columns.Add(dc.ColumnName = dc.ColumnName + TempColumn.ToString());
                }
                else
                {
                    dt.Columns.Add(dc);
                }
            }

            int ColumnsCount = dt.Columns.Count;
            for (int i = 0; i < xnl.Count; i++)
            {
                DataRow dr = dt.NewRow();
                for (int j = 0; j < ColumnsCount; j++)
                {
                    dr[j] = xnl.Item(i).ChildNodes[j].InnerText;
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }
    }
}
