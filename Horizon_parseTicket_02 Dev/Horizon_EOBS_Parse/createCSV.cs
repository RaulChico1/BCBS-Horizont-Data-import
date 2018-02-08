using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
namespace Horizon_EOBS_Parse
{

    public class createCSV
    {
        DBUtility dbU;
        public string create_HORIZ_CAS_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                                int Recnum, string LetterProduced, string sysout, string jobID, string LastWriteTime)
        {
            int errorcount = 0;
            string erros = "";
            try
            {

            //    DataTable table_BCC = dbU.ExecuteDataTable(
            //"SELECT Recnum, rtrim(ltrim([Group Name Short])) ,[SBAD_ADDR1],[SBAD_ADDR2], [SBAD_CITY] + ', ' + [SBAD_STATE] + ' ' + [SBAD_ZIP] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_parse_HNJH_WK] where filename ='" + fileInfo.Name + "'");

            //    ////var fieldnames = new List<string>();
            //    //fieldnames.Add("Recnum");
                //fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
                //fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
                //fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");




                string result = "";
                FileInfo fileInfo = new System.IO.FileInfo(fileName);
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    for (int ii = 21; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            //if (ii < 21)
                            //    erros = "";

                            row[21] = row[ii];
                            row[ii] = "";
                            break;

                        }
                    }
                }
                DataTable working_DataTable = dataToUpdate.Copy();
                DataColumnCollection dcCollection = working_DataTable.Columns; // get cols
                if (dcCollection.Contains("Sheet_count"))
                    working_DataTable.Columns.Remove("Sheet_count");
                if (dcCollection.Contains("mailStop"))
                    working_DataTable.Columns.Remove("mailStop");
                if (dcCollection.Contains("MED_Flag"))
                    working_DataTable.Columns.Remove("MED_Flag");
                if (dcCollection.Contains("JobClass"))
                    working_DataTable.Columns.Remove("JobClass");
                createCSV createcsv = new createCSV();

                string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");
                string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";
                string pName = ProcessVars.InputDirectory + pNameToCASS;

                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < working_DataTable.Columns.Count; index++)
                {
                    fieldnames.Add(working_DataTable.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in working_DataTable.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < working_DataTable.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = createcsv.addRecordsCSV(pName, rowData);
                }
                //copy to CASS
                string cassFileName = ProcessVars.gDMPs + pNameToCASS;
                File.Copy(pName, cassFileName);


                int totrecs = working_DataTable.Rows.Count;
                   
                    // create store proc to delete if exist
                    int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
                    try
                    {
                        if (FileCount == 0)
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                               totrecs + ",'" + pNameToCASS + "','" + fileInfo.Name + "','" + LastWriteTime + "','HOR_parse_" + tablename + "','" + directoryAfterCass + "','" + LetterProduced + "','" + sysout + "','" + jobID + "','Receive','" + GlobalVar.DateofProcess + "')");
                        }
                        else
                        {
                            dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set RecordsNum =" +
                                               totrecs + ", SentDate = GETDATE(), TableName = 'HOR_parse_" + tablename + "', Processed = NULL " +
                                               ",DirectoryTo = '" + directoryAfterCass +
                                               ",LettersProduced = '" + LetterProduced +
                                               ",SysOut = '" + sysout +
                                               ",JobId = '" + jobID +
                                               ",DateProcess = '" + GlobalVar.DateofProcess +
                                               "' where FileNameCASS = '" + pNameToCASS + "'");

                        }
                    }
                    catch (Exception ex)
                    {
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
               
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }

        public bool addRecordsCSV(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (value.IndexOf(",") != -1)
                            {
                                if (sb.Length > 0)
                                    sb.Append(",\"" + value + "\"");
                                else
                                    sb.Append("\"" + value + "\"");
                            }
                            else
                            {
                                if (sb.Length > 0)
                                    sb.Append("," + value);
                                else
                                    sb.Append(value);
                                //sb.Append(value.Replace(",", " "));
                            }
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append(",");

                            sb.Append(value.Replace(",", " "));
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }
        public bool addRecordsPipe_CSV(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("|");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("|");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }
        public bool addStringToCSV(string filePath, string rowOutput)
        {
            //var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {

                        wr.WriteLine(rowOutput);
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {

                        wr.WriteLine(rowOutput);
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }
        public bool addRecordsTabDelimited(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\t");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\t");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }

        public bool addRecordsQuoteCommaDelimited(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\",\"");
                            else
                                sb.Append("\"");
                            sb.Append(value.Replace(",", " "));
                        }
                        wr.WriteLine(sb.ToString() + "\"");
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\",\"");
                            else
                                sb.Append("\"");
                            sb.Append(value.Replace(",", " "));
                        }
                        wr.WriteLine(sb.ToString() + "\"");
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }

        public bool printCSV_fullProcess(string filename, DataTable inputData, string suffix, string XMPieHeader)
        {
            bool process = false;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            FileInfo fileInfo = new System.IO.FileInfo(filename);

            if (inputData.Rows.Count > 0)
            {
                string fNewName = fileInfo.Name;
                if(suffix.Length> 0)
                {
                    fNewName = fileInfo.Name.Substring(0, fileInfo.Name.Length - (fileInfo.Extension.Length )) + suffix +  fileInfo.Extension;
                }
                string pName = fileInfo.Directory + "\\" + fNewName;
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < inputData.Columns.Count; index++)
                {
                    fieldnames.Add(inputData.Columns[index].ColumnName);
                }
                bool resp = addRecordsCSV(pName, fieldnames);
                if (XMPieHeader == "Y")
                    resp = addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in inputData.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < inputData.Columns.Count; index++)
                    {
                        if (row[index].ToString() == "")
                            rowData.Add(" ");
                        else
                            rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = addRecordsCSV(pName, rowData);
                }

            }



            return process;
        }

        public bool printCSV_fullProcessNoHeader(string filename, DataTable inputData, string suffix, string XMPieHeader)
        {
            bool process = false;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            FileInfo fileInfo = new System.IO.FileInfo(filename);

            if (inputData.Rows.Count > 0)
            {
                string fNewName = fileInfo.Name;
                if (suffix.Length > 0)
                {
                    fNewName = fileInfo.Name.Substring(0, fileInfo.Name.Length - (fileInfo.Extension.Length)) + suffix + fileInfo.Extension;
                }
                string pName = fileInfo.Directory + "\\" + fNewName;
                //if (File.Exists(pName))
                //    File.Delete(pName);
                //var fieldnames = new List<string>();
                //for (int index = 0; index < inputData.Columns.Count; index++)
                //{
                //    fieldnames.Add(inputData.Columns[index].ColumnName);
                //}
                //bool resp = addRecordsCSV(pName, fieldnames);
                //if (XMPieHeader == "Y")
                //    resp = addRecordsCSV(pName, fieldnames);
                bool resp = false;
                foreach (DataRow row in inputData.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < inputData.Columns.Count; index++)
                    {
                        if (row[index].ToString() == "")
                            rowData.Add(" ");
                        else
                            rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = addRecordsCSV(pName, rowData);
                }

            }



            return process;
        }
    }
}
