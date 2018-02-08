using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.IO;

namespace Horizon_EOBS_Parse
{
    public class IMBProcess_Back
    {
        DBUtility dbU;
        DBUtility dbUIMB;
        public string  update_IMB_back(string todayProcess)
        {
            int totrecs = 0;
            
            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            GlobalVar.connectionKey = "conStrProd";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable limits = dbU.ExecuteDataTable("select * from HOR_parse_SEQ order by recnum");
            DataTable dataSeqNumbers = Result_data_Table();
            int prevVal = 0;
            foreach (DataRow row in limits.Rows)
            {
                DataRow _ravi = dataSeqNumbers.NewRow();
                _ravi["TableName"] = row["Tablename"];
                if (prevVal == 0)
                {
                    _ravi["Recnum1"] = 0;
                    prevVal = Convert.ToInt32(row["Recnum"].ToString())+1;
                }
                else
                {
                    _ravi["Recnum1"] = prevVal;
                    prevVal = Convert.ToInt32(row["Recnum"].ToString()) + 1;
                }
                _ravi["Recnum2"] = Convert.ToInt32(row["Recnum"].ToString());
                dataSeqNumbers.Rows.Add(_ravi);
            }
            int minLavel = Convert.ToInt32(dataSeqNumbers.Compute("max([Recnum2])", string.Empty));

            string here = "";
            GlobalVar.dbaseName = "USPS_IMBSCAN";
            GlobalVar.connectionKey = "BCBS_Horizon_IMB";
            dbUIMB = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable dataFile =null;
            try
            {
                dataFile = dbUIMB.ExecuteDataTable(" select Account_Number as Record_Number, opr_code, [Date] as opr_Date, [Friendly Status] as opr_Status, '' as Source_File, '' as Name, '' as Member_ID, '' as Destination_zip from [IMBDATA_IMPORT] " +
                                                                    " where convert(date,DATETIME_IMPORTED) = '" + todayProcess + "' order by Account_Number");
            }
            catch (Exception ex)
            {
                var error = ex.Message;
            }
            if(dataFile != null)
            {
                dataFile.Columns.Add("TableName", typeof(String));
            totrecs = dataFile.Rows.Count;
            foreach (DataRow row in dataFile.Rows)
            {

                Int32 recordnumber = Convert.ToInt32(row["Record_Number"].ToString());
                foreach (DataRow rowC in dataSeqNumbers.Rows)
                {
                    if (Convert.ToInt32(row["Record_Number"].ToString()) == 17605715)
                        here = "xx";

                    if (Convert.ToInt32(rowC["recnum1"].ToString()) == 17605679)
                        here = "xx";


                    if ((recordnumber  >  Convert.ToInt32(rowC["recnum1"].ToString())  ||
                        recordnumber == Convert.ToInt32(rowC["recnum1"].ToString())
                        )&&
                        (recordnumber  <  Convert.ToInt32(rowC["recnum2"].ToString()) ||
                        recordnumber  ==  Convert.ToInt32(rowC["recnum2"].ToString())
                        ))
                    {
                        row["TableName"] = rowC["Tablename"];

                        string strsql = "";
                        //if (rowC["Tablename"].ToString() == "HOR_parse_Maintenance_ID_Cards" || rowC["Tablename"].ToString() == "HOR_DirectMail")
                        //{
                        //    //strsql = "select filename,UpdAddr1, '', UpdZip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //    strsql = "select filename from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //    DataTable Processed = dbU.ExecuteDataTable(strsql);
                        //    if (Processed.Rows.Count > 0)
                        //    {
                        //        row["Source_File"] = Processed.Rows[0][0].ToString();
                        //        //row["Name"] = Processed.Rows[0][1].ToString();
                        //        //row["Member_ID"] = Processed.Rows[0][2].ToString();
                        //        //row["Destination_Zip"] = Processed.Rows[0][3].ToString();
                        //    }
                        //}


                        //string strsql = "";
                        //if (rowC["Tablename"].ToString() == "HNJH_IDCards")
                        //    strsql = "select filename,[Upd Name], meme_ID, [Upd Zip] from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if(rowC["Tablename"].ToString() == "HOR_parse_HNJH_Panel_Roster_Provider")
                        //    strsql = "select filename,UpdAddr1, ProvGroupID, Zip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if(rowC["Tablename"].ToString()  == "HOR_parse_HNJH_WK")
                        //    strsql = "select filename,UpdAddr1, SBSB_ID, UpdZip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HOR_parse_AbilTO")
                        //    strsql = "select filename,UpdAddr1, '' , UpdZip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HNJH_SAPD")
                        //    strsql = "select filename,[Group Name], [Main Group] , [Upd Zip] from " + rowC["Tablename"] + "_Master where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HOR_parse_Maintenance_ID_Cards")
                        //{
                        //    strsql = "select filename,UpdAddr1, [member ID], UpdZip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //}
                        //else if (rowC["Tablename"].ToString() == "HOR_parse_HNJH_Dental")
                        //    strsql = "select filename,UpdAddr1, pcpid, UpdZip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());

                        //else if (rowC["Tablename"].ToString() == "HOR_parse_Involuntary_Disenrollment")
                        //    strsql = "select filename,UpdAddr1, Member_ID, UpdZip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HOR_parse_UCDS")
                        //    strsql = "select filename,UpdAddr1, '' , Zip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HOR_parse_ALGS")
                        //    strsql = "select filename,UpdAddr1, '' , Zip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //    else if (rowC["Tablename"].ToString() == "HOR_DirectMail")
                        //    strsql = "select filename,UpdAddr1, '' , UpdZip from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HOR_DirectMail__Seeds")
                        //    strsql = "select filename,UpdAddr1, '' , UpdZip from HOR_DirectMail__Seeds where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HOR_parse_CBill_PastDueNotice" || rowC["Tablename"].ToString() ==  "HOR_parse_IM")
                        //    strsql = "select filename,UpdAddr1, '' , '' from HOR_DirectMail where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else if (rowC["Tablename"].ToString() == "HOR_parse_CareRadius_2")
                        //    strsql = "select fname,UpdAddr1, '' , '' from HOR_parse_CareRadius_2 where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());
                        //else
                        //    strsql = "select filename,UpdAddr1, '', '' from " + rowC["Tablename"] + " where recnum = " + Convert.ToInt32(row["Record_Number"].ToString());

                        //DataTable Processed = dbU.ExecuteDataTable(strsql);
                        //if (Processed.Rows.Count > 0)
                        //{
                        //    row["Source_File"] = Processed.Rows[0][0].ToString();
                        //    row["Name"] = Processed.Rows[0][1].ToString();
                        //    row["Member_ID"] = Processed.Rows[0][2].ToString();
                        //    row["Destination_Zip"] = Processed.Rows[0][3].ToString();
                        //}
                        //else
                        //    row["Source_File"] = strsql;
                        break;
                    }
                }

            }
            }
            string colnames = "";
            createCSV createcsv = new createCSV();
            string pName = @"C:\CierantProjects_dataLocal\ZZ_requested reports\IMB\Data_Updated_from_IMB" + todayProcess + ".csv";//ProcessVars.oNoticeDirectory + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
            //if (File.Exists(pName))
            //    File.Delete(pName);
            //var fieldnames = new List<string>();
            //for (int index = 0; index < dataFile.Columns.Count; index++)
            //{
            //    string nColname = "";
            //    string colname = dataFile.Columns[index].ColumnName;
            //    colnames = colnames + ", [" + colname + "]";
               
            //        fieldnames.Add(colname);
               
            //}
            //bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            //resp = createcsv.addRecordsCSV(pName, fieldnames);
            //foreach (DataRow row in dataFile.Rows)
            //{

            //    var rowData = new List<string>();
            //    for (int index = 0; index < dataFile.Columns.Count; index++)
            //    {
            //        rowData.Add(row[index].ToString());
            //    }
            //    resp = false;
            //    resp = createcsv.addRecordsCSV(pName, rowData);
            //    //if (UpdSQL != "")
            //    //    dbU.ExecuteScalar(UpdSQL + row[0]);
            //}
            ////HOR_parse_IMB_received
            GlobalVar.dbaseName = "BCBS_Horizon";
            GlobalVar.connectionKey = "conStrProd";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //dbU.ExecuteScalar("delete from HOR_parse_IMB_received");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                //bulkCopy.DestinationTableName =
                //    "[dbo].[Tempo_fsaData]";
                bulkCopy.DestinationTableName = "[dbo].[HOR_parse_IMB_received]";

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.BatchSize = dataFile.Rows.Count;
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.WriteToServer(dataFile);

                    //bulkCopy.WriteToServer(dataFile);
                }
                catch (Exception ex)
                {
                    var error = ex.Message;
                }
            }
            Connection.Close();
            dbU.ExecuteNonQuery("update HOR_parse_IMB_received set import_date = '" + todayProcess + "' where import_date is null");

            return "Tot Recs: " + totrecs;
        }
        private static DataTable Result_data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("TableName");
            newt.Columns.Add("Recnum1");
            newt.Columns.Add("recnum2");
            return newt;
        }
        public string Update_IdCards(string todayProcess)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            GlobalVar.connectionKey = "conStrProd";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable onlyIdCards = dbU.ExecuteDataTable("SELECT  record_number, opr_code, opr_date, opr_Status  FROM HOR_parse_IMB_received where tablename = 'HOR_parse_Maintenance_ID_Cards' " +
                                                            " and import_date = '" + todayProcess + "' order by record_number, opr_date");
            int prevRecNum = 0;
            int prevCode = 0;
            int countrecs = 0;
            int totUpds = 0;
            string prevStatus = "";
            string prevDate = "";
            foreach (DataRow row in onlyIdCards.Rows)
            {
                if(prevRecNum != Convert.ToInt32(row["record_number"].ToString()))
                {
                    if(countrecs == 0)
                    {
                        countrecs++;
                        prevRecNum = Convert.ToInt32(row["record_number"].ToString());
                        prevCode = Convert.ToInt32(row["opr_code"].ToString());
                        prevStatus = row["opr_Status"].ToString();
                        prevDate = row["opr_date"].ToString().Substring(0,row["opr_date"].ToString().IndexOf(" "));
                    }
                    else
                    {
                        countrecs++;
                        dbU.ExecuteNonQuery("update HOR_parse_Maintenance_ID_Cards set opr_code = " + prevCode + 
                                            ", opr_date = '" + prevDate + "', opr_Status = '" + prevStatus + "' where recnum = " + prevRecNum );
                       
                        totUpds++;
                        prevRecNum = Convert.ToInt32(row["record_number"].ToString());
                        prevCode = Convert.ToInt32(row["opr_code"].ToString());
                        prevStatus = row["opr_Status"].ToString();
                        prevDate = row["opr_date"].ToString().Substring(0, row["opr_date"].ToString().IndexOf(" ") );
                    }
                }
                else
                {
                    prevCode = Convert.ToInt32(row["opr_code"].ToString());
                    prevStatus = row["opr_Status"].ToString();
                    prevDate = row["opr_date"].ToString().Substring(0, row["opr_date"].ToString().IndexOf(" "));
                }
            }
            if (prevRecNum != 0)
            {
                dbU.ExecuteNonQuery("update HOR_parse_Maintenance_ID_Cards set opr_code = " + prevCode +
                                           ", opr_date = '" + prevDate + "', opr_Status = '" + prevStatus + "' where recnum = " + prevRecNum);
                totUpds++;
            }
            return "Total updates " + totUpds;
            //SELECT count(*) FROM [USPS_IMBSCAN].[dbo].[IMBDATA_IMPORT] where convert(date,DATETIME_IMPORTED) = '2016-07-22'
            //select tablename, count(tablename) as records from [BCBS_Horizon].[dbo].[HOR_parse_IMB_received] where import_date = '2016-07-22' group by tablename order by tablename
            
            // count only ID cards updated:
            //select count(distinct record_number)  as records from [BCBS_Horizon].[dbo].[HOR_parse_IMB_received] where import_date = '2016-07-22' and tablename = 'HOR_parse_Maintenance_ID_Cards' 
        }

        public string Update_DirectMail(string todayProcess)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            GlobalVar.connectionKey = "conStrProd";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable onlyIdCards = dbU.ExecuteDataTable("SELECT  record_number, opr_code, opr_date, opr_Status  FROM HOR_parse_IMB_received where tablename = 'HOR_DirectMail' and  Import_Date = '" + DateTime.Now.AddDays(0).ToString("yyyy-MM-dd") + "'" +
                                                            " order by record_number, opr_date");
            //" and import_date = '" + todayProcess + "' order by record_number, opr_date");
            int prevRecNum = 0;
            int prevCode = 0;
            int countrecs = 0;
            int totUpds = 0;
            string prevStatus = "";
            string prevDate = "";
            foreach (DataRow row in onlyIdCards.Rows)
            {
                if (prevRecNum != Convert.ToInt32(row["record_number"].ToString()))
                {
                    if (countrecs == 0)
                    {
                        countrecs++;
                        prevRecNum = Convert.ToInt32(row["record_number"].ToString());
                        prevCode = Convert.ToInt32(row["opr_code"].ToString());
                        prevStatus = row["opr_Status"].ToString();
                        prevDate = row["opr_date"].ToString().Substring(0, row["opr_date"].ToString().IndexOf(" "));
                    }
                    else
                    {
                        countrecs++;
                        dbU.ExecuteNonQuery("update HOR_parse_Maintenance_ID_Cards set opr_code = " + prevCode + ", opr_Update = '" + todayProcess +
                                            "', opr_date = '" + prevDate + "', opr_Status = '" + prevStatus + "' where recnum = " + prevRecNum);

                        totUpds++;
                        prevRecNum = Convert.ToInt32(row["record_number"].ToString());
                        prevCode = Convert.ToInt32(row["opr_code"].ToString());
                        prevStatus = row["opr_Status"].ToString();
                        prevDate = row["opr_date"].ToString().Substring(0, row["opr_date"].ToString().IndexOf(" "));
                    }
                }
                else
                {
                    prevCode = Convert.ToInt32(row["opr_code"].ToString());
                    prevStatus = row["opr_Status"].ToString();
                    prevDate = row["opr_date"].ToString().Substring(0, row["opr_date"].ToString().IndexOf(" "));
                }
            }
            if (prevRecNum != 0)
            {
                dbU.ExecuteNonQuery("update HOR_DirectMail set opr_code = " + prevCode +
                                           ", opr_date = '" + prevDate + "', opr_Status = '" + prevStatus + "' where recnum = " + prevRecNum);
                totUpds++;
            }
            return "Total updates Direct MaIL" + totUpds;
            //SELECT count(*) FROM [USPS_IMBSCAN].[dbo].[IMBDATA_IMPORT] where convert(date,DATETIME_IMPORTED) = '2016-07-22'
            //select tablename, count(tablename) as records from [BCBS_Horizon].[dbo].[HOR_parse_IMB_received] where import_date = '2016-07-22' group by tablename order by tablename

            // count only ID cards updated:
            //select count(distinct record_number)  as records from [BCBS_Horizon].[dbo].[HOR_parse_IMB_received] where import_date = '2016-07-22' and tablename = 'HOR_parse_Maintenance_ID_Cards' 
        }
    }
}
