using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Configuration;
using System.Text.RegularExpressions;


namespace Horizon_EOBS_Parse
{
    public class HNJH_Champs
    {
        DBUtility dbU;

        public string Process_Champs(string filename)
        {
            int updErrors = 0;
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            int GRecnum = 1;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;



            string result = "";
            FileInfo fileInfo = new System.IO.FileInfo(filename);

            DataTable fromXLSTmp = loadXLSX(filename);

            DataTable fromXLS = fromXLSTmp.Select().Where(x => !x.IsNull(0)).CopyToDataTable();
            #region oldCode
           // foreach (var column in fromXLS.Columns.Cast<DataColumn>().ToArray())
           // {
           //     if (fromXLS.AsEnumerable().All(dr => dr.IsNull(column)))
           //         fromXLS.Columns.Remove(column);
           // }
           // DataColumnCollection columns = fromXLS.Columns;
           // fromXLS.Columns["DateOfBirth"].SetOrdinal(3);
           // if (columns.Contains("Telephone"))
           // {
           //     fromXLS.Columns["Telephone"].ColumnName = "PROVIDER_PHONE_NO";
                
           // }
           // fromXLS.Columns["State"].SetOrdinal(5);
           // fromXLS.Columns.Add("MiddleInitial").SetOrdinal(3);
           // fromXLS.Columns.Add("Age").SetOrdinal(5);
           // fromXLS.Columns.Add("Gender");
           // fromXLS.Columns.Add("PcpID");

           // fromXLS.Columns["HHFirstName"].ColumnName = "PROVIDER_FIRST_NAME";
           // fromXLS.Columns["HHLastName"].ColumnName = "PROVIDER_Last_Name";
           // fromXLS.Columns["PROVIDER_FIRST_NAME"].SetOrdinal(15);
           // fromXLS.Columns["PROVIDER_Last_Name"].SetOrdinal(15);
           // fromXLS.Columns.Add("PROVIDER_Title");
           // fromXLS.Columns.Add("PROVIDER_ADDRESS1");
           // fromXLS.Columns.Add("PROVIDER_City");
           // fromXLS.Columns.Add("PROVIDER_state");
           // fromXLS.Columns.Add("PROVIDER_ZIP");
           // fromXLS.Columns["PROVIDER_PHONE_NO"].SetOrdinal(20);

           //// fromXLS.Columns.Add("PROVIDER_PHONE_NO");
           // fromXLS.Columns.Add("PROVIDER_FAX_NO");
           // fromXLS.Columns.Add("PROVIDER_COUNTY");
           // fromXLS.Columns.Add("GROUP_ID");
           // fromXLS.Columns.Add("GROUP_NAME");

           // fromXLS.Columns.Add("Recnum");
           // fromXLS.Columns.Add("FileName");
           // fromXLS.Columns.Add("ImportDate");

           // fromXLS.Columns.Add("UpdAddr1");
           // fromXLS.Columns.Add("UpdAddr2");
           // fromXLS.Columns.Add("UpdAddr3");
           // fromXLS.Columns.Add("UpdAddr4");
           // fromXLS.Columns.Add("UpdAddr5");

           // fromXLS.Columns.Add("UpdCity");
           // fromXLS.Columns.Add("UpdState");
           // fromXLS.Columns.Add("UpdZip");
           // fromXLS.Columns.Add("IMBChar");
           // fromXLS.Columns.Add("DL");
           // fromXLS.Columns.Add("UpdCounty");
#endregion 
            fromXLS.Columns.Add("Recnum");
            fromXLS.Columns.Add("FileName");
            fromXLS.Columns.Add("ImportDate");

            foreach (DataRow row in fromXLS.Rows)
            {
                row["FileName"] = fileInfo.Name;
                row["ImportDate"] = DateTime.Now;
                row["Recnum"] = GRecnum;
                GRecnum++;
            }
            string colnames = "";
            for (int index = 0; index < fromXLS.Columns.Count; index++)
            {
                string colname = fromXLS.Columns[index].ColumnName;
                colnames = colnames + ", [" + colname + "]";
            }


            dbU.ExecuteScalar("delete from HOR_parse_HNJH_Champion_tmp");
            int errorcount = 0;
            string erros = "";
            string recnumError = "";
            string insertCommand1 = "Insert into HOR_parse_HNJH_Champion_tmp (" + colnames.Substring(1,colnames.Length-1) + ") VALUES ('";
            foreach (DataRow row in fromXLS.Rows)
            {
                string insertCommand2 = "";
                for (int index = 0; index < fromXLS.Columns.Count; index++)
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

            // check ZIP CODE if 5 digits....
            //=====================================================

            if (errorcount == 0)
            {
                string pNameT = fileInfo.DirectoryName + "\\HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                string BCCname = "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";

                //GlobalVar.dbaseName = "BCBS_Horizon";
                //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                //dbU.ExecuteScalar("delete from HOR_parse_HNJH_Champion_tmp");


                //SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                //Connection.Open();

                //using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                //{
                //    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_HNJH_Champion_tmp]";

                //    try
                //    {
                //        bulkCopy.WriteToServer(fromXLS);
                //    }
                //    catch (Exception ex)
                //    {
                //        errors = errors + ex.Message;    //colid 27   Member Gender
                //        updErrors++;
                //    }
                //}
                //Connection.Close();

                if (updErrors == 0)
                {
                    //replace nulls
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", "HOR_parse_HNJH_Champion_TMP") };

                    dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);
                    dbU.ExecuteScalar("Insert into HOR_parse_HNJH_Champion select * from HOR_parse_HNJH_Champion_tmp");

                }

                dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_HNJH_Champion', GETDATE())");

                dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                                fromXLS.Rows.Count + ",'" + BCCname + "','" + fileInfo.Name + "','" + fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "','HOR_parse_HNJH_Champion','" +
                                                directoryAfterCass + "','','','','Receive','" + GlobalVar.DateofProcess + "')");







                DataTable table_BCC = dbU.ExecuteDataTable(
                    "SELECT Recnum, rtrim(ltrim([ParentGuardian])) ,[AddressLine1],[AddressLine2], [City] + ', ' + [State] + ' ' + [Zip] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_parse_HNJH_Champion] where filename = '" + fileInfo.Name + "'");
                    //"SELECT Recnum, rtrim(ltrim([Provider_First_Name])) + ' ' + rtrim(ltrim([Provider_last_name])) as Name,[AddressLine2],[AddressLine1], [City] + ', ' + [State] + ' ' + [Zip] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_parse_HNJH_Champion] where filename = '" + fileInfo.Name + "'");
                //CSV  data===================================================================

                if (File.Exists(pNameT))
                    File.Delete(pNameT);

                var fieldnames = new List<string>();
                fieldnames.Add("Recnum");
                fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
                fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
                fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");

                createCSV createcsvT = new createCSV();
                bool resp = createcsvT.addRecordsCSV(pNameT, fieldnames);
                foreach (DataRow row in table_BCC.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < table_BCC.Columns.Count; index++)
                    {
                        if (index == 0)
                            rowData.Add(row[index].ToString());

                        else if (index == 1)
                        {
                            rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add("");
                            rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); rowData.Add(""); ; rowData.Add("");
                            rowData.Add(row[index].ToString());
                        }
                        else if (index == 2)
                            rowData.Add(row[index].ToString());
                        else if (index == 3)
                            rowData.Add(row[index].ToString());
                        else if (index == 4)
                        {
                            rowData.Add(""); rowData.Add(""); rowData.Add(row[index].ToString());
                        }
                    }
                    resp = false;
                    resp = createcsvT.addRecordsCSV(pNameT, rowData);
                    //if (UpdSQL != "")
                    //    dbU.ExecuteScalar(UpdSQL + row[0]);
                }
                //copy to CASS
                string cassFileName = ProcessVars.gDMPs + BCCname;
                File.Copy(pNameT, cassFileName);
                var tR = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * 3);
                });
                tR.Wait();
                ProcessBackData();
                
            }
            else
                result = "errors";

            return result;
        }
        public DataTable loadXLSX(string filename)
        {
            DataTable dtSchema = new DataTable();
            var connectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0}; Extended Properties=Excel 12.0;", filename);
            string sheetName = "";

            using (OleDbConnection conn = new OleDbConnection(connectionString))
            {
                conn.Open();
                dtSchema = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });
                foreach (DataRow row in dtSchema.Rows)
                {
                    if (!row["TABLE_NAME"].ToString().Contains("FilterDatabase"))
                    {
                        // sheetNames.Add(new SheetName() { sheetName = row["TABLE_NAME"].ToString(), sheetType = row["TABLE_TYPE"].ToString(), sheetCatalog = row["TABLE_CATALOG"].ToString(), sheetSchema = row["TABLE_SCHEMA"].ToString() });
                        //if (group == "Commercial")
                        //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");   // was 1  when carry 2 tabs
                        //else
                        //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");
                        sheetName = row["TABLE_NAME"].ToString();
                    }
                }


                //if (group == "Commercial")
                //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");   // was 1  when carry 2 tabs
                //else
                //    sheetName = dtSchema.Rows[0].Field<string>("TABLE_NAME");
            }

            DataTable XLSdataTable = new DataTable();
            var adapter = new OleDbDataAdapter("SELECT * FROM [" + sheetName + "]", connectionString);

            adapter.Fill(XLSdataTable);
            return XLSdataTable;
        }
        public string ProcessBackData()
        {

            appSets appsets = new appSets();
            appsets.setVars();

            BackCASS processRedturns = new BackCASS();
            HNJH_To_XMPie processXMPie = new HNJH_To_XMPie();
            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //string strsql = "select filenamecass from HOR_parse_files_to_CASS where TableName = 'HOR_parse_HNJH_Panel_Roster_Provider' ";
            string strsql = "select filenamecass, Processed from HOR_parse_files_to_CASS where TableName =  " +
                            "'HOR_parse_HNJH_Champion' and (processed is null)";
            DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            foreach (DataRow row in table_BCCToProcess.Rows)
            {
                string Ftxtname = row[0].ToString().Replace("_toBCC.csv", "");
                if (DBNull.Value.Equals(row[1]))
                {
                    result = HNJH_Champion(row[0].ToString());
                }
                //Update XMPie file name
            
            }
            return result;
        }
        public string HNJH_Champion(string fileName)
        {
            int updErrors = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
            if (File.Exists(processedFiles + fileName.Replace(".csv", "-OUTPUT.csv")))
            {
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + fileName + "' and Processed is null");
                DataRow dr;

                dr = ds.Tables[0].Rows[0];
                string FFName = dr[0].ToString();
                string TTName = dr[1].ToString();
                string TTDir = dr[2].ToString();
                BackCASS readFilesBackBCC = new BackCASS();
                DataTable QualifiedRecords = readFilesBackBCC.readcsvHNJH(processedFiles + fileName.Replace(".csv", "-OUTPUT.csv"));
                string resultUPD = "";
                if (QualifiedRecords.Rows.Count > 0)
                {
                    resultUPD = readFilesBackBCC.updateTableSQL_HNJH(QualifiedRecords, "Y", FFName, TTName);
                }
                //READ NON DELIVERABLE
                string files = "";
                string erroFile = ProcessVars.gODMPsMedicaid + fileName.Replace(".csv", "-NON-DELIVERABLE.csv");
                if (File.Exists(erroFile))
                {
                    DataTable NonD_Records = readFilesBackBCC.readcsvError(erroFile);

                    if (NonD_Records.Rows.Count > 0)
                    {
                        if (files != "")
                        {
                            resultUPD =readFilesBackBCC.updateTableSQL_HNJH(NonD_Records, "N", FFName, TTName);
                            string strsql = "update  HOR_parse_HNJH_Champion set dl = 'N' where city = '' and state = ''  and FileName = '" + FFName + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                        foreach (DataRow row in NonD_Records.Rows)
                        {
                            //TextBox1.Text = row["Recordnum"].ToString();
                            string strsql = "update  HOR_parse_HNJH_Champion set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                    }
                }

                if (updErrors == 0)
                {
                    //good and bad records to update
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };

                    DataSet ds2 = new DataSet();
                    try
                    {

                        ds2 = dbU.ExecuteDataSet("HOR_upd_tot_Parse_NonDeliverables", sqlParams2);
                    }
                    catch (Exception ez)
                    {
                        var errors = ez.Message;
                    }
                    DataRow dr2;
                    string toND = "";
                    string totOK = "";
                    if (ds2 != null)
                    {
                        dr2 = ds2.Tables[0].Rows[0];
                        toND = dr2[1].ToString();
                        totOK = dr2[2].ToString();
                    }
                    //string OutputpName = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed\" + FFName.ToUpper().Replace(".PDF", ".csv");
                    //string pNameCass = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                    //File.Copy(pNameCass, OutputpName);  // prevent user open before ready

                    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set processed = 'Y', CASSReceiveDate =  GETDATE() , " +
                                        " RecordsOK = " + totOK + ", " +
                                        " RecordsNonDeliverable = " + toND + " where filename = '" + FFName + "'");

                }

                
            }
            
           

            return updErrors.ToString();
        }
        public string HNJH_Champion_noCass(string fileName, string FFName, string TTName, string TTDir)
        {
            int updErrors = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
            if (File.Exists(processedFiles + fileName.Replace("\\","").Replace(".csv", "-OUTPUT.csv")))
            {
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + fileName + "' and Processed is null");
                DataRow dr;

                //dr = ds.Tables[0].Rows[0];
                //string FFName = dr[0].ToString();
                //string TTName = dr[1].ToString();
                //string TTDir = dr[2].ToString();
                BackCASS readFilesBackBCC = new BackCASS();
                DataTable QualifiedRecords = readFilesBackBCC.readcsvHNJH(processedFiles + fileName.Replace(".csv", "-OUTPUT.csv"));
                string resultUPD = "";
                if (QualifiedRecords.Rows.Count > 0)
                {
                    resultUPD = readFilesBackBCC.updateTableSQL_HNJH(QualifiedRecords, "Y", FFName, TTName);
                }
                //READ NON DELIVERABLE
                string files = "";
                string erroFile = ProcessVars.gODMPsMedicaid + fileName.Replace(".csv", "-NON-DELIVERABLE.csv");
                if (File.Exists(erroFile))
                {
                    DataTable NonD_Records = readFilesBackBCC.readcsvError(erroFile);

                    if (NonD_Records.Rows.Count > 0)
                    {
                        if (files != "")
                        {
                            resultUPD = readFilesBackBCC.updateTableSQL_HNJH(NonD_Records, "N", FFName, TTName);
                            string strsql = "update  " + TTName + " set dl = 'N' where city = '' and state = ''  and FileName = '" + FFName + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                        foreach (DataRow row in NonD_Records.Rows)
                        {
                            //TextBox1.Text = row["Recordnum"].ToString();
                            string strsql = "update   " + TTName + " set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                    }
                }

                if (updErrors == 0)
                {
                    //good and bad records to update
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };

                    DataSet ds2 = new DataSet();
                    try
                    {

                        ds2 = dbU.ExecuteDataSet("HOR_upd_tot_Parse_NonDeliverables", sqlParams2);
                    }
                    catch (Exception ez)
                    {
                        var errors = ez.Message;
                    }
                    DataRow dr2;
                    string toND = "";
                    string totOK = "";
                    if (ds2 != null)
                    {
                        dr2 = ds2.Tables[0].Rows[0];
                        toND = dr2[1].ToString();
                        totOK = dr2[2].ToString();
                    }
                    //string OutputpName = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\CareRadius_Processed\" + FFName.ToUpper().Replace(".PDF", ".csv");
                    //string pNameCass = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                    //File.Copy(pNameCass, OutputpName);  // prevent user open before ready

                    dbU.ExecuteScalar("Update HOR_parse_files_to_CASS set processed = 'Y', CASSReceiveDate =  GETDATE() , " +
                                        " RecordsOK = " + totOK + ", " +
                                        " RecordsNonDeliverable = " + toND + " where filename = '" + FFName + "'");

                }


            }



            return updErrors.ToString();
        }
        public string HNJH_ChampionPrint(string fileName,string  location)
        {
            int updErrors = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
            
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + fileName + "'");
                DataRow dr;

                dr = ds.Tables[0].Rows[0];
                string FFName = dr[0].ToString();
                string TTName = dr[1].ToString();
                string TTDir = dr[2].ToString();
               

                //string strsqlF = "select * from HOR_parse_HNJH_Champion where dl = 'Y' and filename = '" + FFName + "' order by recnum";
                //DataTable datatoXmpie = dbU.ExecuteDataTable(strsqlF);
                //if (datatoXmpie.Rows.Count > 0)
                //{
                //    createCSV createcsv = new createCSV();
                //    //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                //    string pName = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + ".csv";
                //    if (File.Exists(pName))
                //        File.Delete(pName);
                //    var fieldnames = new List<string>();
                //    for (int index = 0; index < datatoXmpie.Columns.Count; index++)
                //    {
                //        fieldnames.Add(datatoXmpie.Columns[index].ColumnName);
                //    }
                //    bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                //    foreach (DataRow row in datatoXmpie.Rows)
                //    {

                //        var rowData = new List<string>();
                //        for (int index = 0; index < datatoXmpie.Columns.Count; index++)
                //        {
                //            rowData.Add(row[index].ToString());
                //        }
                //        resp = false;
                //        resp = createcsv.addRecordsCSV(pName, rowData);
                //        //if (UpdSQL != "")
                //        //    dbU.ExecuteScalar(UpdSQL + row[0]);
                //    }

                //}
                //pritn file to SCI
                //SqlParameter[] sqlParamsSCI;
                //sqlParamsSCI = null;
                //sqlParamsSCI = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };
                //string spName = "HOR_rpt_PARSE_Champions_to_SCI";
                string spName = "select Recnum, ParentGuardian,FirstName, LastName, UpdAddr2 as Addr1,  UpdAddr5 as Addr2, UpdCity + ', ' + updstate + ' ' + Updzip as City_State_Zip, IMBchar from HOR_parse_HNJH_Champion where dl = 'Y' and filename = '" + FFName + "' order by recnum";

                DataTable datato_SCI = dbU.ExecuteDataTable(spName);
                if (datato_SCI.Rows.Count > 0)
                {
                    createCSV createcsv = new createCSV();
                    //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                    string pName = location + "\\" + FFName.Substring(0, FFName.Length - 5) + "_ToSCI.csv";
                    if (File.Exists(pName))
                        File.Delete(pName);
                    var fieldnames = new List<string>();
                    for (int index = 0; index < datato_SCI.Columns.Count; index++)
                    {
                        fieldnames.Add(datato_SCI.Columns[index].ColumnName);
                    }
                    bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                    foreach (DataRow row in datato_SCI.Rows)
                    {

                        var rowData = new List<string>();
                        for (int index = 0; index < datato_SCI.Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());
                        }
                        resp = false;
                        resp = createcsv.addRecordsCSV(pName, rowData);
                        //if (UpdSQL != "")
                        //    dbU.ExecuteScalar(UpdSQL + row[0]);
                    }

                }
            

            return updErrors.ToString();
        }

    }


}
