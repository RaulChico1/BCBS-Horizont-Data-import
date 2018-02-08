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
using System.Collections;
//using Excel = Microsoft.Office.Interop.Excel;

namespace Horizon_EOBS_Parse
{
   
        public class HNJH_WK_Medicaid
        {
            DBUtility dbU;
            private static DataTable WK_Table()
            {
                DataTable newt = new DataTable();
                newt.Clear();

                newt.Columns.Add("SBSB_ID");
                newt.Columns.Add("MEME_LAST_NAME");
                newt.Columns.Add("MEME_FIRST_NAME");
                newt.Columns.Add("MEME_MID_INIT");
                newt.Columns.Add("MEPE_EFF_DT");
                newt.Columns.Add("SBAD_ADDR1");
                newt.Columns.Add("SBAD_ADDR2");
                newt.Columns.Add("SBAD_CITY");
                newt.Columns.Add("SBAD_STATE");
                newt.Columns.Add("SBAD_ZIP");
                newt.Columns.Add("MEME_MCTR_LANG");
                newt.Columns.Add("SBAD_COUNTY");
                newt.Columns.Add("CSPI_ID");
                newt.Columns.Add("CSPI_Desc");
                newt.Columns.Add("MCTR_DESC");
                newt.Columns.Add("Source");
                //newt.Columns.Add("KitNum");
                //newt.Columns.Add("Plan");
                //newt.Columns.Add("ItemDesc");
                //newt.Columns.Add("MemProv");

                return newt;
            }
            public string Process_WK(string filename, string locationLocal)
            {
                #region main_Process
                int updErrors = 0;
                string errors = "";
                FileInfo fileInfo = new System.IO.FileInfo(filename);
                string result = "";
                string pNameT = "";
                string BCCname = "";

                DataTable WKTable = WK_Table();

                var lines = File.ReadAllLines(filename).ToList();
                try
                {
                    lines.ForEach(line => WKTable.Rows.Add(line.Split((char)9)));
                }
                catch (Exception ex)
                {
                    var msgbox = ex.Message;
                }
                WKTable.Columns.Add("ImportDate").SetOrdinal(0);
                WKTable.Columns.Add("FileName").SetOrdinal(0);
                WKTable.Columns.Add("Recnum").SetOrdinal(0);
                WKTable.Columns.Add("Translate1");
                WKTable.Columns.Add("Translate2");
                WKTable.Columns.Add("Translate3");
                WKTable.Columns.Add("Translate4");
                WKTable.Columns.Add("O_seq");
                WKTable.Columns.Add("Output");
                WKTable.Columns.Add("DupSeq");
                int x = 1;
                foreach (DataRow row in WKTable.Rows)
                {
                    row["FileName"] = fileInfo.Name;
                    row["ImportDate"] = DateTime.Now;
                    row["O_seq"] = x.ToString("00000");
                    x++;
                }
                var Rows = (from row in WKTable.AsEnumerable()
                            orderby row["SBSB_ID"] ascending
                            select row);
                DataTable sortedData = Rows.AsDataView().ToTable();
                int tot1 = WKTable.Rows.Count;

                string prevCode = "";
                int outputOk = 1;
                int numdups = 0;
                string prevCod = "";
                int SeqPrev = 0;
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                foreach (DataRow rowU in sortedData.Rows)
                {

                    string wMCTR_Desc = "";
                    string strsql = "";
                    if (rowU["MCTR_DESC"].ToString().Contains("PD-"))
                        wMCTR_Desc = "PD-";
                    else if (rowU["MCTR_DESC"].ToString().Contains("Member Hand Book-"))
                        wMCTR_Desc = "Member Hand Book-";
                    else
                        wMCTR_Desc = rowU["MCTR_DESC"].ToString();


                    strsql = "select Translate from HOR_Parse_HNJH_WK_Master_Translation where " +
                       "CPI_desc = '" + rowU["CSPI_Desc"].ToString() + "' and " +
                       "MCTR_Desc like '" + wMCTR_Desc + "%'";


                    var resultQ = dbU.ExecuteScalar(strsql);
                    if (resultQ != null && resultQ.ToString() != "")
                    {
                        if (wMCTR_Desc == "PD-" || wMCTR_Desc == "Member Hand Book-")

                            rowU["Translate1"] = resultQ + rowU["MCTR_DESC"].ToString();

                        else
                            rowU["Translate1"] = resultQ;
                    }
                    else
                    {
                        if (rowU["CSPI_desc"].ToString() == "MLTS" && rowU["MCTR_DESC"].ToString().Contains("PD-"))
                            rowU["Translate1"] = "MLTSS-" + rowU["MCTR_DESC"].ToString();
                        else if (rowU["CSPI_desc"].ToString() != "MLTS" &&
                            (rowU["MCTR_DESC"].ToString().Contains("PD-") || rowU["MCTR_DESC"].ToString().Contains("Member Hand Book-")))
                            rowU["Translate1"] = rowU["MCTR_DESC"].ToString();
                        if (rowU["MCTR_DESC"].ToString().ToUpper().Contains("FORMULARY"))
                            rowU["Translate1"] = "HNJH-FORM-016";
                        if (rowU["MCTR_DESC"].ToString().ToUpper().Contains("SPECIALIST DIRECTORY"))
                            rowU["Translate1"] = "Specialist Directory (HB-4670 _W01015)";
                        if (rowU["MCTR_DESC"].ToString().ToUpper().Contains("PERSONAL REP FORM"))
                            rowU["Translate1"] = "Personal Rep Form (HNJH-5302_W0611)";
                        if (rowU["MCTR_DESC"].ToString().ToUpper().Contains("HIPAA FLIER"))
                            rowU["Translate1"] = "Privacy Flyer (HNJH-7997_0814)";
                    }
                }
                int totdups = 0;
                foreach (DataRow row in sortedData.Rows)
                {


                    if (row["SBSB_ID"].ToString() == prevCod)
                    {
                        numdups++;
                        sortedData.Rows[SeqPrev - 1]["DupSeq"] = numdups;
                        sortedData.Rows[SeqPrev - 1]["Translate2"] = sortedData.Rows[SeqPrev]["Translate1"];
                        numdups++;

                        row["DupSeq"] = numdups;


                    }
                    else
                    {
                        prevCod = row["SBSB_ID"].ToString();

                        row["Output"] = outputOk;
                        outputOk++;

                    }
                    SeqPrev++;
                }
                int GRecnum = 1;
                var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
                int recordnumber = 0;
                if (recnum.ToString() == "")
                    GRecnum = 1;
                else
                    GRecnum = Convert.ToInt32(recnum.ToString()) + 1;


                foreach (DataRow row in sortedData.Rows)
                {
                    if (row["Output"].ToString() != "")
                    {
                        row["Recnum"] = GRecnum;
                        GRecnum++;
                    }
                }
                DataRow[] resultRow = sortedData.Select("Output is null");
                foreach (DataRow row2 in resultRow)
                {
                    sortedData.Rows.Remove(row2);
                }



                dbU.ExecuteScalar("delete from HOR_parse_HNJH_WK_tmp");



                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_HNJH_WK_tmp]";

                    try
                    {
                        bulkCopy.WriteToServer(sortedData);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;    //colid 27   Member Gender
                        updErrors++;
                    }
                }
                Connection.Close();

                if (updErrors == 0)
                {
                    try
                    {
                        dbU.ExecuteScalar("Insert into HOR_parse_HNJH_WK select * from HOR_parse_HNJH_WK_tmp");

                        pNameT = locationLocal + "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                        BCCname = "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
                        string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";


                        dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_HNJH_WK', GETDATE())");

                        dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                                        sortedData.Rows.Count + ",'" + BCCname + "','" + fileInfo.Name + "','" + fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "','HOR_parse_HNJH_WK','" +
                                                        directoryAfterCass + "','','','','Receive','" + GlobalVar.DateofProcess + "')");

                        dbU.ExecuteScalar("delete from HOR_parse_HNJH_WK_tmp");

                        File.Move(fileInfo.FullName, fileInfo.Directory + "\\__" + fileInfo.Name);

                    }
                    catch (Exception ex)
                    {
                        var excc = ex.Message;
                    }
                }
                else
                {
                    var errorsss = "here";
                }


                DataTable table_BCC = dbU.ExecuteDataTable(
                 "SELECT Recnum, rtrim(ltrim([MEME_FIRST_NAME])) + ' ' + rtrim(ltrim([MEME_MID_INIT])) + ' ' + rtrim(ltrim([MEME_LAST_NAME])) as Name,[SBAD_ADDR1],[SBAD_ADDR2], [SBAD_CITY] + ', ' + [SBAD_STATE] + ' ' + [SBAD_ZIP] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_parse_HNJH_WK] where filename ='" + fileInfo.Name + "'");
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
                //File.Move(pNameT, fileInfo.Directory + @"\" +BCCname);
                #endregion
                var t0 = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * 2);
                    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                });
                t0.Wait();

                ProcessBackData();

                //update to DL = N 






                //data to xmpie
                //DataTable alltoXMPie = dbU.ExecuteDataTable("select * from HOR_parse_HNJH_WK where filename = '" + fileInfo.Name + "' order by HOR_parse_HNJH_WK.Recnum");
                //createCSV printFile = new createCSV();
                //printFile.printCSV_fullProcess(filename, alltoXMPie, "_to_XMPie");



                return result;
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
                                "'HOR_parse_HNJH_WK' and Processed is null";
                DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
                foreach (DataRow row in table_BCCToProcess.Rows)
                {
                    string Ftxtname = row[0].ToString().Replace("_toBCC.csv", "");
                    if (DBNull.Value.Equals(row[1]))
                    {
                        result = HNJH_WKits(row[0].ToString());
                    }
                    //Update XMPie file name

                }
                return result;
            }
            public string Print_HNJH_WK(string location, string locationLocal, string dateReport)
            {
                string result = "";
                appSets appsets = new appSets();
                appsets.setVars();
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                createCSV createcsv = new createCSV();


                string strsqlError = "SELECT * FROM HOR_parse_HNJH_WK  where Translate1 = 'ERROR' and convert(date,importdate) = '" + dateReport.Replace("_", "/") + "'";
                DataTable recsError = dbU.ExecuteDataTable(strsqlError);
                string ferrorname = location + @"\HNJH_WKits_Others_ERROR_" + dateReport + ".csv";
                createcsv.printCSV_fullProcess(ferrorname, recsError, "", "N");
                dbU.ExecuteNonQuery("update HOR_Parse_HNJH_WK set DL = 'N' where convert(date,importdate) = '" + dateReport.Replace("_", "/") + "' and  DL is null");
                dbU.ExecuteNonQuery("update HOR_Parse_HNJH_WK set DL = 'N' where convert(date,importdate) = '" + dateReport.Replace("_", "/") + "' and  len(UpdCounty) = 1");

                strsqlError = "SELECT * FROM HOR_parse_HNJH_WK where UpdState <> 'NJ' and Translate1 in ('NJH-01','NJH-02','NJH-03') and convert(date,importdate) = '" + dateReport.Replace("_", "/") + "'";
                DataTable OutState = dbU.ExecuteDataTable(strsqlError);
                ferrorname = location + @"\HNJH_WKits_OUT_OF_STATE_" + dateReport + ".csv";
                createcsv.printCSV_fullProcess(ferrorname, OutState, "", "N");

                strsqlError = "SELECT * FROM HOR_parse_HNJH_WK where  DL = 'N' and convert(date,importdate) = '" + dateReport.Replace("_", "/") + "'";
                DataTable DELno = dbU.ExecuteDataTable(strsqlError);
                ferrorname = location + @"\HNJH_WKits_Non_Deliverable_" + dateReport + ".csv";
                createcsv.printCSV_fullProcess(ferrorname, DELno, "", "N");

                dbU.ExecuteNonQuery("update HOR_parse_HNJH_WK set DL = 'E'  where Translate1 = 'ERROR' and convert(date,importdate) = '" + dateReport.Replace("_", "/") + "'");
                dbU.ExecuteNonQuery("update HOR_parse_HNJH_WK set DL = 'O'  where UpdState <> 'NJ' and Translate1 in ('NJH-01','NJH-02','NJH-03') and convert(date,importdate) = '" + dateReport.Replace("_", "/") + "'");

                //string TTName = dr[1].ToString();
                string TTDir = location; // to SQL
                string FFName = @"\HNJH_WKits_NJH" + dateReport + ".csv";
                #region testing
                //dbU.ExecuteNonQuery("delete from HNJH_WKits_NJH_XMpie_1");
                //dbU.ExecuteNonQuery("delete from HNJH_WKits_NJH_XMpie_2");
                //dbU.ExecuteNonQuery("delete from HNJH_WKits_NJH_XMpie_3");
                SqlParameter[] sqlParamsXMpie;
                sqlParamsXMpie = null;
                sqlParamsXMpie = new SqlParameter[] { new SqlParameter("@Date", dateReport.Replace("_", "/")) };
                dbU.ExecuteScalar("HOR_rpt_HNJH_WK__XMpie_Date_hnjh_01_02_03", sqlParamsXMpie);



                SqlParameter[] sqlParamsLabels;
                sqlParamsLabels = null;
                sqlParamsLabels = new SqlParameter[] { new SqlParameter("@Translate", "NJH-01") };
                string LabelsFFName = @"\HNJH_WKits_NJH-01_Labels__" + dateReport + ".csv";
                DataTable NJH01_Labels = dbU.ExecuteDataTable("HOR_rpt_HNJH_WK__NJH01_Labels", sqlParamsLabels);
                createcsv.printCSV_fullProcess(TTDir + LabelsFFName, NJH01_Labels, "", "");


                sqlParamsLabels = null;
                sqlParamsLabels = new SqlParameter[] { new SqlParameter("@Translate", "NJH-02") };
                LabelsFFName = @"\HNJH_WKits_NJH-02_Labels__" + dateReport + ".csv";
                DataTable NJH02_Labels = dbU.ExecuteDataTable("HOR_rpt_HNJH_WK__NJH02_Labels", sqlParamsLabels);
                createcsv.printCSV_fullProcess(TTDir + LabelsFFName, NJH02_Labels, "", "");

                sqlParamsLabels = null;
                sqlParamsLabels = new SqlParameter[] { new SqlParameter("@Translate", "NJH-03") };
                LabelsFFName = @"\HNJH_WKits_NJH-03_Labels__" + dateReport + ".csv";
                DataTable NJH03_Labels = dbU.ExecuteDataTable("HOR_rpt_HNJH_WK__NJH03_Labels", sqlParamsLabels);
                createcsv.printCSV_fullProcess(TTDir + LabelsFFName, NJH03_Labels, "", "");
                //DataTable datatoXmpie = dbU.ExecuteDataTable("HOR_rpt_HNJH_WK__XMpie_Date_hnjh_01_02_03", sqlParamsXMpie);
                //if (datatoXmpie.Rows.Count > 0)
                //{

                //    string pName = TTDir +  FFName ; //.Substring(0, FFName.Length - 4) + "_to_XMpie.csv";
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



                FFName = @"\HNJH_WKits_Others" + dateReport + ".csv";   // to sql

                SqlParameter[] sqlParamsXMpie2;
                sqlParamsXMpie2 = null;
                sqlParamsXMpie2 = new SqlParameter[] { new SqlParameter("@Date", dateReport.Replace("_", "/")) };
                dbU.ExecuteScalar("HOR_rpt_HNJH_WK__XMpie_Date_Others", sqlParamsXMpie2);
                //if (datatoXmpie2.Rows.Count > 0)
                //{
                //    //createCSV createcsv = new createCSV();
                //    string pName = TTDir + FFName; //.Substring(0, FFName.Length - 4) + "_to_XMpie.csv";
                //    if (File.Exists(pName))
                //        File.Delete(pName);
                //    var fieldnames = new List<string>();
                //    for (int index = 0; index < datatoXmpie2.Columns.Count; index++)
                //    {
                //        fieldnames.Add(datatoXmpie2.Columns[index].ColumnName);
                //    }
                //    bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                //    foreach (DataRow row in datatoXmpie2.Rows)
                //    {

                //        var rowData = new List<string>();
                //        for (int index = 0; index < datatoXmpie2.Columns.Count; index++)
                //        {
                //            rowData.Add(row[index].ToString());
                //        }
                //        resp = false;
                //        resp = createcsv.addRecordsCSV(pName, rowData);
                //    }

                //}


                SqlParameter[] sqlParamsXMpie3;
                sqlParamsXMpie3 = null;
                sqlParamsXMpie3 = new SqlParameter[] { new SqlParameter("@Date", dateReport.Replace("_", "/")) };
                dbU.ExecuteNonQuery("HOR_rpt_HNJH_WK__XMpie_Date_Others_toXMPie", sqlParamsXMpie3);


                string strsqSCI = "select Recnum,SBSB_ID, MEME_LAST_NAME,MEME_FIRST_NAME, MEME_MID_INIT, MEPE_EFF_DT, UpdAddr1 , UpdAddr2 , UpdAddr4,UpdAddr5, UpdCity, UpdState, UpdZip  ,[CSPI_ID]" +
                                   ",CSPI_Desc,MCTR_DESC,[Source],Translate1,Translate2 " +
                                   "from HOR_parse_HNJH_WK where  convert(date,ImportDate) = '" + dateReport.Replace("_", "/") + "' and SUBSTRING(Translate1,1,6) = 'NJH-03' order by MCTR_DESC desc,  Translate1, UpdCounty";

                DataTable datato_SCI = dbU.ExecuteDataTable(strsqSCI);
                if (datato_SCI.Rows.Count > 0)
                {

                    //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                    string pName = TTDir + FFName.Substring(0, FFName.Length - 4) + "_NJH-03_ToSCI.csv";
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
                #endregion


                // summary to Excel
                // SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);
                SqlDataReader rdr = null;
                DataTable Summary_File = new DataTable();
                DataTable Detail_DL_N = new DataTable();
                DataTable Summary_County = new DataTable();
                DataTable Summary_NJH = new DataTable();
                string xName = TTDir + "\\HNJH_WKits_NJH" + dateReport + "_SUMMARY_ToSCI.xls";
                if (File.Exists(xName))
                    File.Delete(xName);
                using (SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("HOR_rpt_HNJH_WK__Summary_Date", Connection))
                    {
                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.Add("@Date", SqlDbType.VarChar).Value = dateReport.Replace("_", "/");
                        int recType = 1;
                        Connection.Open();

                        rdr = cmd.ExecuteReader();

                        if (rdr.HasRows)
                        {

                            Summary_File.Load(rdr);
                            Detail_DL_N.Load(rdr);
                            Summary_NJH.Load(rdr);
                        }

                        else
                        {
                            Console.WriteLine("No rows found.");
                        }
                        rdr.Close();
                        Export_XLSX createxls = new Export_XLSX();
                        createxls.CreateExcelFileTables(Summary_File, "Summary Files", xName
                                        , Detail_DL_N, "Non Deliverables"

                                        , Summary_NJH, "Summary NJH__1-3");


                    }
                }



                return result;
            }

            public string HNJH_WKits(string fileName)
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
                    string updateCounty = "update HOR_parse_HNJH_WK set UpdCounty = SBAD_COUNTY " +
                                            "where FileName = '" + FFName +
                                            "' and SBAD_COUNTY <> UpdCounty and (UpdCounty is null or UpdCounty = '')";
                    dbU.ExecuteNonQuery(updateCounty);

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
                                string strsql = "update  HOR_parse_HNJH_WK set dl = 'N' where city = '' and state = ''  and FileName = '" + FFName + "'";
                                dbU.ExecuteNonQuery(strsql);
                            }
                            foreach (DataRow row in NonD_Records.Rows)
                            {
                                //TextBox1.Text = row["Recordnum"].ToString();
                                string strsql = "update  HOR_parse_HNJH_WK set dl = 'N' where Recnum = '" + row["Recnum"].ToString() + "'";
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
                    // SqlParameter[] sqlParamsXMpie;
                    // sqlParamsXMpie = null;
                    // sqlParamsXMpie = new SqlParameter[] { new SqlParameter("@FileName", FFName) };


                    // DataTable datatoXmpie = dbU.ExecuteDataTable("HOR_rpt_HNJH_WK__XMpie", sqlParamsXMpie);
                    // if (datatoXmpie.Rows.Count > 0)
                    // {
                    //     createCSV createcsv = new createCSV();
                    //     //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                    //     string pName = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + "_to_XMpie.csv";
                    //     if (File.Exists(pName))
                    //         File.Delete(pName);
                    //     var fieldnames = new List<string>();
                    //     for (int index = 0; index < datatoXmpie.Columns.Count; index++)
                    //     {
                    //         fieldnames.Add(datatoXmpie.Columns[index].ColumnName);
                    //     }
                    //     bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                    //     foreach (DataRow row in datatoXmpie.Rows)
                    //     {

                    //         var rowData = new List<string>();
                    //         for (int index = 0; index < datatoXmpie.Columns.Count; index++)
                    //         {
                    //             rowData.Add(row[index].ToString());
                    //         }
                    //         resp = false;
                    //         resp = createcsv.addRecordsCSV(pName, rowData);
                    //         //if (UpdSQL != "")
                    //         //    dbU.ExecuteScalar(UpdSQL + row[0]);
                    //     }

                    // }
                    // //pritn file to SCI
                    // //SqlParameter[] sqlParamsSCI;
                    // //sqlParamsSCI = null;
                    // //sqlParamsSCI = new SqlParameter[] { new SqlParameter("@FileName", FFName), new SqlParameter("@table", TTName) };
                    //// string spName = "HOR_rpt_PARSE_Champions_to_SCI";
                    // string strsqSCI = "select Recnum,SBSB_ID, MEME_LAST_NAME,MEME_FIRST_NAME, MEME_MID_INIT, MEPE_EFF_DT, UpdAddr1 , UpdAddr2 , UpdAddr4,UpdAddr5, UpdCity, UpdState, UpdZip  ,[CSPI_ID]" +
                    //                    ",CSPI_Desc,MCTR_DESC,[Source],Translate1,Translate2 " +
                    //                    "from HOR_parse_HNJH_WK where filename = '" + FFName + "' order by MCTR_DESC desc, , Translate1, UpdCounty";

                    // DataTable datato_SCI = dbU.ExecuteDataTable(strsqSCI);
                    // if (datato_SCI.Rows.Count > 0)
                    // {
                    //     createCSV createcsv = new createCSV();
                    //     //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                    //     string pName = TTDir + "\\" + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                    //     if (File.Exists(pName))
                    //         File.Delete(pName);
                    //     var fieldnames = new List<string>();
                    //     for (int index = 0; index < datato_SCI.Columns.Count; index++)
                    //     {
                    //         fieldnames.Add(datato_SCI.Columns[index].ColumnName);
                    //     }
                    //     bool resp = createcsv.addRecordsCSV(pName, fieldnames);
                    //     foreach (DataRow row in datato_SCI.Rows)
                    //     {

                    //         var rowData = new List<string>();
                    //         for (int index = 0; index < datato_SCI.Columns.Count; index++)
                    //         {
                    //             rowData.Add(row[index].ToString());
                    //         }
                    //         resp = false;
                    //         resp = createcsv.addRecordsCSV(pName, rowData);
                    //         //if (UpdSQL != "")
                    //         //    dbU.ExecuteScalar(UpdSQL + row[0]);
                    //     }

                    // }
                }



                return updErrors.ToString();
            }
        }
    
}
