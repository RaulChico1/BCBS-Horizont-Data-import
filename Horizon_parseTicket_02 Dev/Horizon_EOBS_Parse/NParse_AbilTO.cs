using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Configuration;
using System.Text.RegularExpressions;

namespace Horizon_EOBS_Parse
{
    public class NParse_AbilTO
    {
        DBUtility dbU;
        DataTable dt = new DataTable();
        public string importAbilTo(string location)
        {
            string totErrors = "";
            DirectoryInfo xlss = new DirectoryInfo(location);

            FileInfo[] files = xlss.GetFiles("*.xlsx");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    errors = processAbilTo(file.FullName);
                    if (errors == "")
                    {
                        Directory.Move(file.FullName, file.Directory + "\\__" + file.Name);
                        //errors = printCSV(GlobalVar.DateofProcess.ToShortDateString(), file.FullName);
                       
                    }
                    else
                        totErrors = totErrors + ", " + errors;
                }
            }
            
            //process return BCC
            var t0 = Task.Run(async delegate
            {
                await Task.Delay(1000 * 60 * 2);
                return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            });
            t0.Wait();

            appSets appsets = new appSets();
            appsets.setVars();

            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            //string strsql = "select filenamecass from HOR_parse_files_to_CASS where TableName = 'HOR_parse_HNJH_Panel_Roster_Provider' ";
            string strsql = "select filenamecass, Processed from HOR_parse_files_to_CASS where TableName =  " +
                            "'HOR_parse_Abilto' and Processed is null";
            DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            foreach (DataRow row in table_BCCToProcess.Rows)
            {
                ProcessBackData(row[0].ToString());

               
                //Update XMPie file name

            }

            //output XMpie
            dbU.ExecuteScalar("update HOR_parse_AbilTO set OutputType = 'COM' where filename LIKE '%SHBP%'");
            dbU.ExecuteScalar("update HOR_parse_AbilTO set OutputType = 'COM' where filename LIKE '%COM%'");
            dbU.ExecuteScalar("update HOR_parse_AbilTO set OutputType = 'FEP' where filename LIKE '%FEP%'");
            dbU.ExecuteNonQuery("delete from HOR_XMPIE_AbilTO_COM");
            dbU.ExecuteNonQuery("delete from HOR_XMPIE_AbilTO_FEP");
            SqlParameter[] sqlParams2;
            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@Pdate", DateTime.Now.ToString("yyyy-MM-dd")) };
            dbU.ExecuteScalar("HOR_upd_AbilTo_COM_Xmpie",sqlParams2);
            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@Pdate", DateTime.Now.ToString("yyyy-MM-dd")) };
            dbU.ExecuteScalar("HOR_upd_AbilTo_FEP_Xmpie",sqlParams2);
            
            //results
            // select filename, OutputType, DL, convert(date,dateimport), count(filename)
                //from HOR_parse_AbilTO where convert(date,dateimport) = '2016-06-16' group by filename, OutputType, DL, convert(date,dateimport)
            
            return errors;
        }
        public void sendBCC(string fileDirectory, string Bname, string fname)
        {
            string BCCname = ProcessVars.gDMPs +  Bname;   // "HNJH-PR_" + DateTime.Now.ToString("yyyy-MM-dd") + "_toBCC.csv";
            string pNameT = fileDirectory + "\\" + Bname;
            DataTable table_BCC = dbU.ExecuteDataTable(
             "SELECT Recnum, rtrim(ltrim(first_name)) + ' ' + rtrim(ltrim([Last_Name])) as Name,[Address1],[Address2], [City] + ', ' + [State] + ' ' + [Zip] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_parse_Abilto] where filename ='" +  fname + "'");
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
            string cassFileName = ProcessVars.gDMPs + Bname;
            File.Copy(pNameT, cassFileName);
            //File.Move(pNameT, fileDirectory +  BCCname);
           
          
            
        }
        public void ProcessBackData(string BCCname)
        {

            int updErrors = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DirectoryInfo processedFiles = new DirectoryInfo(ProcessVars.gODMPsMedicaid);
            if (File.Exists(processedFiles + "\\" +  BCCname.Replace(".csv", "-OUTPUT.csv")))
            {
                DataSet ds = new DataSet();
                ds = dbU.ExecuteDataSet("select FileName, TableName, DirectoryTo  from HOR_parse_files_to_CASS where FileNameCASS = '" + BCCname + "' and Processed is null");
                DataRow dr;

                dr = ds.Tables[0].Rows[0];
                string FFName = dr[0].ToString();
                string TTName = dr[1].ToString();
                string TTDir = dr[2].ToString();
                BackCASS readFilesBackBCC = new BackCASS();
                DataTable QualifiedRecords = readFilesBackBCC.readcsvHNJH(processedFiles + BCCname.Replace(".csv", "-OUTPUT.csv"));
                string resultUPD = "";
                if (QualifiedRecords.Rows.Count > 0)
                {
                    resultUPD = readFilesBackBCC.updateTableSQL_HNJH(QualifiedRecords, "Y", FFName, TTName);
                }
                string updateCounty = "update HOR_parse_Abilto set UpdCounty = SBAD_COUNTY " +
                                        "where FileName = '" + FFName +
                                        "' and SBAD_COUNTY <> UpdCounty and (UpdCounty is null or UpdCounty = '')";
                dbU.ExecuteNonQuery(updateCounty);

                //READ NON DELIVERABLE
                string files = "";
                string erroFile = ProcessVars.gODMPsMedicaid + BCCname.Replace(".csv", "-NON-DELIVERABLE.csv");
                if (File.Exists(erroFile))
                {
                    DataTable NonD_Records = readFilesBackBCC.readcsvError(erroFile);

                    if (NonD_Records.Rows.Count > 0)
                    {
                        if (files != "")
                        {
                            resultUPD = readFilesBackBCC.updateTableSQL_HNJH(NonD_Records, "N", FFName, TTName);
                            string strsql = "update  HOR_parse_AbilTO set dl = 'N' where city = '' and state = ''  and FileName = '" + FFName + "'";
                            dbU.ExecuteNonQuery(strsql);
                        }
                        foreach (DataRow row in NonD_Records.Rows)
                        {
                            //TextBox1.Text = row["Recordnum"].ToString();
                            string strsql = "update  HOR_parse_AbilTO set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
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
        }


        public string processAbilTo(string filename)
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

            DataTable fromXLS_tmp = loadXLSX(filename);

            DataTable newXLSTable = fromXLS_tmp.Select().Where(x => !x.IsNull(0)).CopyToDataTable();

            foreach (var column in newXLSTable.Columns.Cast<DataColumn>().ToArray())
            {
                if (newXLSTable.AsEnumerable().All(dr => dr.IsNull(column)))
                    newXLSTable.Columns.Remove(column);
            }

            newXLSTable.Columns.Add("FileName").SetOrdinal(0);
            newXLSTable.Columns.Add("DateImport").SetOrdinal(0);
            newXLSTable.Columns.Add("Recnum").SetOrdinal(0);
            newXLSTable.Columns.Add("FixZip").SetOrdinal(9);
            foreach (DataRow row in newXLSTable.Rows)
            {
                row["FileName"] = fileInfo.Name;
                row["DateImport"] = DateTime.Now;
                row["Recnum"] = GRecnum;
                row["FixZip"] = row["Zip"].ToString().PadLeft(5, '0');
                GRecnum++;
            }
            newXLSTable.Columns.Remove("ZIP");
            newXLSTable.Columns["FixZip"].ColumnName = "ZIP";
            if (!newXLSTable.Columns.Contains("Address Line 2"))
                newXLSTable.Columns.Add("Address Line 2").SetOrdinal(6);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from HOR_parse_AbilTO_tmp");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                bulkCopy.DestinationTableName = "[dbo].[HOR_parse_AbilTO_tmp]";

                try
                {
                    bulkCopy.WriteToServer(newXLSTable);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
            }
            Connection.Close();
            string BCCname = "";
            if (updErrors == 0)
            {
                try
                {
                    //replace nulls
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", "HOR_parse_AbilTO_tmp") };

                    dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);



                    dbU.ExecuteScalar("Insert into HOR_parse_AbilTO select * from HOR_parse_AbilTO_tmp");

                    BCCname = "HNJH-PR_" + fileInfo.Name.Substring(0,fileInfo.Name.Length - fileInfo.Extension.Length) + "_toBCC.csv";
                    string directoryAfterCass = fileInfo.DirectoryName;  // ProcessVars.InputDirectory + "FromCASS";


                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_AbilTO', GETDATE())");

                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                                    newXLSTable.Rows.Count + ",'" + BCCname + "','" + fileInfo.Name + "','" + fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "','HOR_parse_AbilTO','" +
                                                    directoryAfterCass + "','','','','Receive','" + GlobalVar.DateofProcess + "')");
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
                sendBCC(fileInfo.DirectoryName, BCCname, fileInfo.Name);
            }

            return errors;
        }
       

        public string printCSV(string dateProcess, string fileNAME )
        {

            FileInfo finfo = new FileInfo(fileNAME);
            string strsql2 = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


                strsql2 = "select recnum, First_name, Last_name, Address1, Address2, City, State, Zip from  HOR_parse_AbilTO where filename = '" + finfo.Name + "'";
                DataTable datatoPrint = dbU.ExecuteDataTable(strsql2);
                string filename = finfo.Directory + "\\" + finfo.Name.Substring(0,finfo.Name.Length - (finfo.Extension.Length +1)).Replace(".","-")   + ".csv";

                createCSV createcsv = new createCSV();

                bool res = createcsv.printCSV_fullProcess(filename, datatoPrint, "","");
            return "ok";
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
    }
}
