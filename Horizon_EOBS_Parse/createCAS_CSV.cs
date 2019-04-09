using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class createCAS_CSV
    {
        DBUtility dbU;
        public string create_Default_CAS_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                                int Recnum, string LetterProduced, string sysout, string jobID, string LastWriteTime)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
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

                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_Tempo");

                
                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_Tempo ([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    int totrecs = dataToUpdate.Rows.Count;
                    try
                    {
                        dbU.ExecuteScalar("Insert into HOR_parse_" + tablename + " select * from HOR_parse_Tempo");
                        dbU.ExecuteScalar("delete from HOR_parse_Tempo");
                    }
                    catch (Exception ex)
                    {
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
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
                        dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_" + tablename + "', GETDATE())");
                    }
                    catch (Exception ex)
                    {
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                else
                {
                    int nerr = errorcount;

                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        public string create_Fraud_CAS_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                               int Recnum, string LetterProduced, string sysout, string jobID, string LastWriteTime)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                string result = "";
                FileInfo fileInfo = new System.IO.FileInfo(fileName);
                int totrecs = dataToUpdate.Rows.Count;
                DataTable working_DataTable = dataToUpdate.Copy();
                DataColumnCollection dcCollection = working_DataTable.Columns; // get cols
                if (dcCollection.Contains("Filename"))
                    working_DataTable.Columns.Remove("Filename");
               
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

               
                if (errorcount == 0)
                {
                    GlobalVar.dbaseName = "BCBS_Horizon";
                    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

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
                        //dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_" + tablename + "', GETDATE())");
                    }
                    catch (Exception ex)
                    {
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                else
                {
                    int nerr = errorcount;

                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        public string create_GBills_CAS_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                               int Recnum, string LetterProduced, string sysout, string jobID, string LastWriteTime)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                string result = "";
                FileInfo fileInfo = new System.IO.FileInfo(fileName);
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    for (int ii = 19; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[19] = row[ii];
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

                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_Tempo");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_Tempo ([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_" + tablename + " select * from HOR_parse_Tempo");
                    dbU.ExecuteScalar("delete from HOR_parse_Tempo");
                    // create store proc to delete if exist
                    int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
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
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_" + tablename + "', GETDATE())");
                }
                else
                {
                    int nerr = errorcount;

                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        public string create_CBills_CAS_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                               int Recnum, string LetterProduced, string sysout, string jobID, string LastWriteTime)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                string result = "";
                FileInfo fileInfo = new System.IO.FileInfo(fileName);
               
                //foreach (DataRow row in dataToUpdate.Rows)
                //{
                //    for (int ii = 20; ii > 0; ii--)
                //    {
                //        if (row[ii].ToString() != "")
                //        {
                //            row[20] = row[ii];
                //            row[ii] = "";
                //            break;
                //        }
                //    }
                //}
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

                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_Tempo");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_Tempo ([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_" + tablename + " select * from HOR_parse_Tempo");
                    dbU.ExecuteScalar("delete from HOR_parse_Tempo");
                    // create store proc to delete if exist
                    int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
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
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_" + tablename + "', GETDATE())");
                }
                else
                {
                    int nerr = errorcount;

                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        public string create_HLGS_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                                int Recnum, string LetterProduced, string sysout, string jobID, string dateHLGS, string cycleDate)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                //string result = "";
                //FileInfo fileInfo = new System.IO.FileInfo(fileName);
                //foreach (DataRow row in dataToUpdate.Rows)
                //{
                //    for (int ii = 21; ii > 0; ii--)
                //    {
                //        if (row[ii].ToString() != "")
                //        {
                //            row[21] = row[ii];
                //            row[ii] = "";
                //            break;
                //        }
                //    }
                //}
                //DataTable working_DataTable = dataToUpdate.Copy();
                //working_DataTable.Columns.Remove("Sheet_count");
                //working_DataTable.Columns.Remove("mailStop");
                //working_DataTable.Columns.Remove("MED_Flag");
                //working_DataTable.Columns.Remove("JobClass");
                //createCSV createcsv = new createCSV();

                //string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                //System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");
                //string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";
                //string pName = ProcessVars.InputDirectory + pNameToCASS;

                //if (File.Exists(pName))
                //    File.Delete(pName);
                //var fieldnames = new List<string>();
                //for (int index = 0; index < working_DataTable.Columns.Count; index++)
                //{
                //    fieldnames.Add(working_DataTable.Columns[index].ColumnName);
                //}
                //bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                //foreach (DataRow row in working_DataTable.Rows)
                //{

                //    var rowData = new List<string>();
                //    for (int index = 0; index < working_DataTable.Columns.Count; index++)
                //    {
                //        rowData.Add(row[index].ToString());
                //    }
                //    resp = false;
                //    resp = createcsv.addRecordsCSV(pName, rowData);
                //}
                ////copy to CASS
                //string cassFileName = ProcessVars.gDMPs + pNameToCASS;
                //File.Copy(pName, cassFileName);

                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_HLGS_Tempo");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_HLGS_Tempo ([ImportDate],[CycleDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 = dateHLGS + "','" + cycleDate + "','";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_" + tablename + " select * from HOR_parse_HLGS_Tempo");
                    dbU.ExecuteScalar("delete from HOR_parse_HLGS_Tempo");
                    // create store proc to delete if exist
                    //int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
                    //if (FileCount == 0)
                    //{
                    //    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task ) values(" +
                    //                       totrecs + ",'" + pNameToCASS + "','" + fileInfo.Name + "', GETDATE(),'HOR_parse_" + tablename + "','" + directoryAfterCass + "','" + LetterProduced + "','" + sysout + "','" + jobID + "','Receive')");
                    //}
                    //else
                    //{
                    //    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set RecordsNum =" +
                    //                       totrecs + ", SentDate = GETDATE(), TableName = 'HOR_parse_" + tablename + "', Processed = NULL " +
                    //                       ",DirectoryTo = '" + directoryAfterCass +
                    //                       ",LettersProduced = '" + LetterProduced +
                    //                       ",SysOut = '" + sysout +
                    //                       ",JobId = '" + jobID +

                    //                       "' where FileNameCASS = '" + pNameToCASS + "'");

                    //}
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_" + tablename + "', GETDATE())");
                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }

        public string create_INV_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                               int Recnum, string LetterProduced, string sysout, string jobID, string dateHLGS, string cycleDate)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
               
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_HIX_Inv_TMP");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_HIX_Inv_TMP ([CycleDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 =  cycleDate + "','";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_HIX_Inv select * from HOR_parse_HIX_Inv_TMP");
                    dbU.ExecuteScalar("delete from HOR_parse_HIX_Inv_TMP");
                    // create store proc to delete if exist
                    //int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
                    //if (FileCount == 0)
                    //{
                    //    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task ) values(" +
                    //                       totrecs + ",'" + pNameToCASS + "','" + fileInfo.Name + "', GETDATE(),'HOR_parse_" + tablename + "','" + directoryAfterCass + "','" + LetterProduced + "','" + sysout + "','" + jobID + "','Receive')");
                    //}
                    //else
                    //{
                    //    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set RecordsNum =" +
                    //                       totrecs + ", SentDate = GETDATE(), TableName = 'HOR_parse_" + tablename + "', Processed = NULL " +
                    //                       ",DirectoryTo = '" + directoryAfterCass +
                    //                       ",LettersProduced = '" + LetterProduced +
                    //                       ",SysOut = '" + sysout +
                    //                       ",JobId = '" + jobID +

                    //                       "' where FileNameCASS = '" + pNameToCASS + "'");

                    //}
                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                           totrecs + ",'" + totrecs.ToString() + "','" + fileName + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileName + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','HOR_parse_HIX_Inv" + "','No CASS','No Sysout','" + jobID + "','Receive','Y','" + GlobalVar.DateofProcess + "','PDF')");



                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_HIX_Inv', GETDATE())");
                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        
        
        public string create_CR2_CSV(string fileName, string fileNameCass, DataTable dataToUpdate, string tablename,
                                                int Recnum, string LetterProduced, 
                                    string sysout, string jobID, string dateHLGS, string cycleDate, DateTime lastW)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_CareRadius_2_TMP");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_CareRadius_2_TMP ([CycleDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 =  cycleDate + "','";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";

                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_CareRadius_2 select * from HOR_parse_CareRadius_2_TMP");
                    dbU.ExecuteScalar("delete from HOR_parse_CareRadius_2_TMP");

                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_CareRadius_2', GETDATE())");
                   // check if LASTW and jobid are proper inserted
                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                   totrecs + ",'" + fileNameCass + "','" + fileName + "','" + lastW + "','HOR_parse_" + tablename + "','" + 
                   directoryAfterCass + "','" + LetterProduced + "','" + sysout + "','" + jobID + "','Receive','" + GlobalVar.DateofProcess + "')");

                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }

        public string create_MBA_CSV(string fileName, string fileNameCass, DataTable dataToUpdate, string tablename,
                                               int Recnum, string LetterProduced,
                                   string sysout, string jobID, string dateHLGS, string cycleDate, DateTime lastW)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    for (int ii = 15; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[15] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }
                
                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_MBA_SMN_TMP");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_MBA_SMN_TMP ([CycleDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 = cycleDate + "','";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";

                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_MBA_SMN select * from HOR_parse_MBA_SMN_TMP");
                    dbU.ExecuteScalar("delete from HOR_parse_MBA_SMN_TMP");

                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_MBA_SMN', GETDATE())");
                    // check if LASTW and jobid are proper inserted
                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                   totrecs + ",'" + fileNameCass + "','" + fileName + "','" + lastW + "','HOR_parse_" + tablename + "','" +
                   directoryAfterCass + "','" + LetterProduced + "','" + sysout + "','" + jobID + "','Receive','" + GlobalVar.DateofProcess + "')");

                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        public string create_SBC_CSV(string fileName, string fileNameCass, DataTable dataToUpdate, string tablename,
                                               int Recnum, string LetterProduced,
                                   string sysout, string jobID, string dateHLGS, string cycleDate, DateTime lastW)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    for (int ii = 15; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[15] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }

                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_SBC_TMP");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_SBC_TMP ([CycleDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 = cycleDate + "','";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";

                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_SBC select * from HOR_parse_SBC_TMP");
                    dbU.ExecuteScalar("delete from HOR_parse_SBC_TMP");

                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_SBC', GETDATE())");
                    // check if LASTW and jobid are proper inserted
                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                   totrecs + ",'" + fileNameCass + "','" + fileName + "','" + lastW + "','HOR_parse_" + tablename + "','" +
                   directoryAfterCass + "','" + LetterProduced + "','" + sysout + "','" + jobID + "','Receive','" + GlobalVar.DateofProcess + "')");

                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        public string create_DueDilligence_CSV(string fileName, DataTable dataToUpdate, string tablename,
                                                int Recnum, string LetterProduced, string sysout, string jobID, string dateHLGS)
        {
            int errorcount = 0;
            string erros = "";
            try
            {
                //string result = "";
                //FileInfo fileInfo = new System.IO.FileInfo(fileName);
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    for (int ii = 11; ii > 0; ii--)
                    {
                        if (row[ii].ToString() != "")
                        {
                            row[11] = row[ii];
                            row[ii] = "";
                            break;
                        }
                    }
                }
                //DataTable working_DataTable = dataToUpdate.Copy();
                //working_DataTable.Columns.Remove("Sheet_count");
                //working_DataTable.Columns.Remove("mailStop");
                //working_DataTable.Columns.Remove("MED_Flag");
                //working_DataTable.Columns.Remove("JobClass");
                //createCSV createcsv = new createCSV();

                //string pNameToCASS = fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                //System.IO.Directory.CreateDirectory(ProcessVars.InputDirectory + "FromCASS");
                //string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";
                //string pName = ProcessVars.InputDirectory + pNameToCASS;

                //if (File.Exists(pName))
                //    File.Delete(pName);
                //var fieldnames = new List<string>();
                //for (int index = 0; index < working_DataTable.Columns.Count; index++)
                //{
                //    fieldnames.Add(working_DataTable.Columns[index].ColumnName);
                //}
                //bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                //foreach (DataRow row in working_DataTable.Rows)
                //{

                //    var rowData = new List<string>();
                //    for (int index = 0; index < working_DataTable.Columns.Count; index++)
                //    {
                //        rowData.Add(row[index].ToString());
                //    }
                //    resp = false;
                //    resp = createcsv.addRecordsCSV(pName, rowData);
                //}
                ////copy to CASS
                //string cassFileName = ProcessVars.gDMPs + pNameToCASS;
                //File.Copy(pName, cassFileName);

                // add to dbase
                string colnames = "";
                for (int index = 0; index < dataToUpdate.Columns.Count; index++)
                {
                    string colname = dataToUpdate.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }

                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_HLGS_Tempo");


                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_HLGS_Tempo ([ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToUpdate.Rows)
                {
                    string insertCommand2 = dateHLGS + "','";
                    for (int index = 0; index < dataToUpdate.Columns.Count; index++)
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
                        errorcount++;
                        erros = erros + ex.Message + "\n\n";
                    }
                }
                if (errorcount == 0)
                {
                    int totrecs = dataToUpdate.Rows.Count;
                    dbU.ExecuteScalar("Insert into HOR_parse_" + tablename + " select * from HOR_parse_HLGS_Tempo");
                    dbU.ExecuteScalar("delete from HOR_parse_HLGS_Tempo");
                    // create store proc to delete if exist
                    //int FileCount = Convert.ToInt16(dbU.ExecuteScalar("select count(filename) from HOR_parse_files_to_CASS where FileNameCASS = '" + pNameToCASS + "'"));
                    //if (FileCount == 0)
                    //{
                    //    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task ) values(" +
                    //                       totrecs + ",'" + pNameToCASS + "','" + fileInfo.Name + "', GETDATE(),'HOR_parse_" + tablename + "','" + directoryAfterCass + "','" + LetterProduced + "','" + sysout + "','" + jobID + "','Receive')");
                    //}
                    //else
                    //{
                    //    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set RecordsNum =" +
                    //                       totrecs + ", SentDate = GETDATE(), TableName = 'HOR_parse_" + tablename + "', Processed = NULL " +
                    //                       ",DirectoryTo = '" + directoryAfterCass +
                    //                       ",LettersProduced = '" + LetterProduced +
                    //                       ",SysOut = '" + sysout +
                    //                       ",JobId = '" + jobID +

                    //                       "' where FileNameCASS = '" + pNameToCASS + "'");

                    //}
                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_parse_" + tablename + "', GETDATE())");
                }
            }
            catch (Exception ex)
            {
                errorcount++;
                erros = erros + ex.Message + "\n\n";
            }

            return erros;
        }
        public void update_w_errors_zero(string filename, string tablename, string MSG, string sysout = "", string jobID = "")
        {
            FileInfo fileInfo = new System.IO.FileInfo(filename);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo, processed, recordsok, recordsnondeliverable,msg,sysout,jobid) values(" +
                              "0,'" + fileInfo.Name + "','" + fileInfo.Name + "', GETDATE(),'HOR_parse_" + tablename + "','No CASS',0,0,0,'" + MSG + "','" + sysout + "','" + jobID +  "')");

           
        }
    }
}
