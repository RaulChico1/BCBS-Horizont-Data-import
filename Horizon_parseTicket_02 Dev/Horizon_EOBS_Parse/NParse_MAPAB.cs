using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Data;
using System.Data.SqlClient;


namespace Horizon_EOBS_Parse
{
    public class NParse_MAPAB
    {
        DBUtility dbU;
        int Seqnum = 1;
        DataTable datafromPdfs = data_Table();
        public string process_filesin_Zip(string zipName, DateTime dateProcess)
        {
            int totf = 0;
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
          

            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_parse_files_downloaded");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Seqnum = 1;
            else
                Seqnum = Convert.ToInt32(recnum.ToString()) + 1;
            DataTable pdfsInXML = new DataTable();
            pdfsInXML.Columns.Add("filename", typeof(String));

            FileInfo fileZip = new FileInfo(zipName);
            try
            {
                System.IO.DirectoryInfo downloadedMessageInfo = new DirectoryInfo(fileZip.Directory + "\\tmp");
                try
                {
                    foreach (FileInfo file in downloadedMessageInfo.GetFiles())
                    {
                        file.Delete();
                    }
                }
                catch (Exception ex)
                {
                    var msg = ex.Message;
                    //LogWriter logerror = new LogWriter();
                    //logerror.WriteLogToTable("error deletig files from dmps", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Parsing files", "start cycle", ex.Message);


                }
                Directory.CreateDirectory(fileZip.Directory + "\\tmp");

                using (ZipArchive archive = ZipFile.OpenRead(zipName))
                {
                    int linenum = 0;
                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase))
                        {
                            var row = datafromPdfs.NewRow();
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                            linenum++;
                            row["Seqnum"] = linenum;
                            row["ZIP"] = zipName;
                            row["Fname"] = entry.Name.ToString();
                            int totPages = 0;
                            PdfReader reader = new PdfReader(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                            totPages = reader.NumberOfPages;
                            row["Pages"] = totPages.ToString();
                            row["FileInXML"] = "N";
                            datafromPdfs.Rows.Add(row);
                        }
                        else if(entry.FullName.EndsWith(".xml", StringComparison.OrdinalIgnoreCase))
                        {
                            entry.ExtractToFile(Path.Combine(fileZip.Directory + "\\", entry.FullName));
                            totf++;

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                errors = errors + ex.Message + "\\n\\n";
                totf = 0;
            }
            // here check pdfs
            //readcsv(csvName, colFileName);

            if (totf > 0)
            {
                int totrecs = datafromPdfs.Rows.Count;
                foreach (DataRow row in datafromPdfs.Rows)
                {
                    dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate,pages,status) values('" +
                         row["ZIP"].ToString() + "','" + row["Fname"].ToString() + "','" + dateProcess + "'," + row["Pages"] + ",'" + row["FileInXML"] + "')");

                    //string filename = 
                    //dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                    //       totrecs + ",'" + totrecs.ToString() + "','" + fileName + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileName + "','" + 
                    //       DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','HOR_parse_" + 
                    //       tablename + "','No CASS','No Sysout','" + jobID + "','Receive','Y','" + GlobalVar.DateofProcess + "','PDF')");


                }




                //dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
                //    Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
                //    lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
                //    DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");


            }
            //else
            //{
            //    foreach (DataRow row in pdfsInXML.Rows)
            //    {
            //        dbU.ExecuteScalar("Insert into HOR_Care_Radius_DataXML_Detail_pdfs(SourceName,FileName, ImportDate) values('" +
            //             filename.Replace("IN/", "") + "','" + row["filename"].ToString() + "','" + dateProcess + "')");
            //    }
            //    dbU.ExecuteScalar("Insert into HOR_parse_files_downloaded(SeqNum, FileName, FileExt, Unziped, FromLocation,ImportDate_Start, ImportDate_end,DateProcess,FilesIn) values(" +
            //        Seqnum + ",'" + filename.Replace("IN/", "") + "','" + ext + "',1,'" + info.Host + "','" +
            //        lastModifiedDate.ToString("MM/dd/yyyy HH:mm:ss") + "', GETDATE(),'" +
            //        DateTime.Now.ToString("yyyy-MM-dd") + "'," + totf + ")");
            //    LogWriter logerror = new LogWriter();
            //    logerror.WriteLogToTable("no files in zip", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Import", "no files in zip " + info.Host + "/" + filename + " " + filesInzip, "email");
            //}


            return errors;
        }
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
    }
}
