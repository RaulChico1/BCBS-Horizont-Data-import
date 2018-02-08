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
using System.Threading;
using System.Text.RegularExpressions;
using Microsoft.VisualBasic.FileIO;


namespace Horizon_EOBS_Parse
{

    public class NParse_GiftCards
    {
        DBUtility dbU;
        public string Process_GiftCards(string filename, string locationLocal)
        {
            string result = "";
            string[] daysWeek = GetWeekRange(DateTime.Now);
            FileInfo fileInfo = new System.IO.FileInfo(filename);
            string LetterType = "";
            if (fileInfo.Name.IndexOf("ACA") != -1)
                LetterType = "ACA";
            else if (fileInfo.Name.IndexOf("PPO") != -1)
                LetterType = "MA_PPO";
            else if (fileInfo.Name.IndexOf("MA HMO") != -1)
                LetterType = "MA_HMO";
            else if (fileInfo.Name.IndexOf("DSNP") != -1)
                LetterType = "DSNP";
            else
                result = "";   // no definition


            DataTable fromXLSTmp = loadXLSX(filename);
            fromXLSTmp.Columns.Add("letter_Type").SetOrdinal(0);
            fromXLSTmp.Columns.Add("Campaign_Name").SetOrdinal(0);
            fromXLSTmp.Columns.Add("ImportDate").SetOrdinal(0);
            fromXLSTmp.Columns.Add("FileName").SetOrdinal(0);
            fromXLSTmp.Columns.Add("Recnum").SetOrdinal(0);
            fromXLSTmp.Columns.Add("CycleDate");
            fromXLSTmp.Columns.Add("UpdAddr1");
            fromXLSTmp.Columns.Add("UpdAddr2");
            fromXLSTmp.Columns.Add("UpdAddr3");
            fromXLSTmp.Columns.Add("UpdAddr4");
            fromXLSTmp.Columns.Add("UpdAddr5");
            fromXLSTmp.Columns.Add("UpdCity");
            fromXLSTmp.Columns.Add("UpdState");
            fromXLSTmp.Columns.Add("UpdZip");
            fromXLSTmp.Columns.Add("DL");
            fromXLSTmp.Columns.Add("OutputFileName");
            fromXLSTmp.Columns.Add("IMBChar");
            fromXLSTmp.Columns.Add("IMBDig");
            //int x = 1;

            int GRecnum = 1;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            SqlParameter[] sqlParams2;
            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", fromXLSTmp.Rows.Count), 
                new SqlParameter("@FileName", fileInfo.Name), new SqlParameter("@TableName", "HOR_parse_Campaigns") };

            dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
            DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + fileInfo.Name + "' and tablename = 'HOR_parse_Campaigns'");
            if (afterUpdateSeq.Rows.Count == 1)
                GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - fromXLSTmp.Rows.Count + 1;
            else
            {
                SendMails sendmail = new SendMails();
                sendmail.SendMailError("error in HOR_upd_Recnum_beforeTMP ", "Error reading recnum after update", "\n\n" + "Error table: HOR_parse_Campaigns,   file " + fileInfo.Name, "");
            }


            foreach (DataRow row in fromXLSTmp.Rows)
            {
                row["FileName"] = fileInfo.Name;
                row["Campaign_Name"] = "GiftCards";
                row["ImportDate"] = DateTime.Now;
                row["CycleDate"] = DateTime.Now.ToString("yyyy-MM-dd");
                row["Recnum"] = GRecnum;
                GRecnum++;
                row["letter_Type"] = LetterType;
            }
            submit_to_BCC(fromXLSTmp, fileInfo, locationLocal);
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

                sheetName = dtSchema.Rows[1].Field<string>("TABLE_NAME");
               
            }

            DataTable XLSdataTable = new DataTable();
            var adapter = new OleDbDataAdapter("SELECT * FROM [" + sheetName + "]", connectionString);

            adapter.Fill(XLSdataTable);
            DataColumnCollection columns = XLSdataTable.Columns;
            if (!columns.Contains("SG_Ind"))
            {
                XLSdataTable.Columns.Add("SG_Ind");
            }
            for (int i = XLSdataTable.Rows.Count - 1; i >= 0; i--)
            {
                if (XLSdataTable.Rows[i]["LastName"] == DBNull.Value && XLSdataTable.Rows[i]["FirstName"] == DBNull.Value)
                {
                    XLSdataTable.Rows[i].Delete();
                }
            }
            XLSdataTable.AcceptChanges();
          

            return XLSdataTable;
        }
        public bool submit_to_BCC(DataTable fromXLSTmp, FileInfo finfo, string locationLocal)
        {
            bool result = false;
            DataTable toBCC = new System.Data.DataTable();
            toBCC.Columns.Add("Recnum");
            toBCC.Columns.Add("FullName");
            toBCC.Columns.Add("Addr1");
            toBCC.Columns.Add("Addr2");
            toBCC.Columns.Add("Addr5");
            foreach (DataRow row in fromXLSTmp.Rows)
            {
                    var rowBCC = toBCC.NewRow();
                    rowBCC["Recnum"] = row["Recnum"].ToString();
                    rowBCC["FullName"] = row["FirstName"].ToString().Trim() + ' ' + row["LastName"].ToString().Trim();
                    rowBCC["Addr1"] = row["Address1"].ToString();
                    rowBCC["Addr2"] = row["Address2"].ToString();
                    rowBCC["Addr5"] = (row["City"].ToString() + ' ' + row["State"].ToString() + ' ' + row["ZipCode"].ToString()).Trim();
                    toBCC.Rows.Add(rowBCC);
             
            }
            for (int i = 0; i < 13; i++)
            {
                toBCC.Columns.Add("F" + i, typeof(string)).SetOrdinal(1);
            }

            toBCC.Columns.Add("Add4", typeof(string)).SetOrdinal(17);
            toBCC.Columns.Add("Add3", typeof(string)).SetOrdinal(17);


            string wbccName = locationLocal + @"\" + "HORIZ_" + finfo.Name.Substring(0, finfo.Name.Length - 5) + "_toBCC.csv";
            string bccName = ProcessVars.dmpsWatched + "HORIZ_" + finfo.Name.Substring(0, finfo.Name.Length - 5) + "_toBCC.csv";
            string bccready = ProcessVars.gODMPs_IMB + "HORIZ_" + finfo.Name.Substring(0, finfo.Name.Length - 5) + "_toBCC-OUTPUT.csv";

            if (File.Exists(bccready))
                File.Delete(bccready);

            //HORIZ_CON2_20170215_NSR_NASCO_HIX_76119_PROCESSED_toBCC.csv
            createCSV printcsv = new createCSV();

            if (File.Exists(wbccName))
                File.Delete(wbccName);

            printcsv.printCSV_fullProcess(wbccName, toBCC, "", "");

            if (File.Exists(bccName))
                File.Delete(bccName);
            File.Copy(wbccName, bccName);

            //=================================================

            int numberTry = 0;

            FileInfo infoBCCreadfy = new FileInfo(bccready);
            string getBCCready = "";
            while (IsFileReady(infoBCCreadfy))
            {
                Thread.Sleep(500);
                numberTry++;
                if (numberTry > 200)
                {
                    getBCCready = "not found file after 200 attempts : " + bccready;
                    SendMails sendmail = new SendMails();
                    sendmail.SendMailError("Horizon Gift Card", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");

                    break;
                }
            }
            string resultBCC = "";
            if (getBCCready == "")
            {
                 resultBCC = processReturnBCC_and_upd_Sql(fromXLSTmp, infoBCCreadfy, finfo.Name);

                 if (resultBCC == "")
                 {
                    
                 }

                 else
                 {
                     SendMails sendmail = new SendMails();
                     sendmail.SendMailError("Horizon Gift Card EOC Back from BCC", "ErrorinProcess", "\n\n" + "Error " + resultBCC, "");
                 }
            }
            else
            {
                SendMails sendmail = new SendMails();
                sendmail.SendMailError("Horizon Gift Card sending to BCC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
            }

            return result;
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
                    sendmail.SendMailError("Horizon Gift Card", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                    break;
                }
            }
            if (getBCCready == "")
            {
                if (File.Exists(bccfile.FullName))
                {

                   // BackCASS readresults = new BackCASS();
                    DataTable backfromBCC = readcsvMAS023(bccfile.FullName, fName);
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
                                    //row["UpdCounty"] = "";
                                    row["IMBChar"] = dRNew["Intelligent Mail barcode"];
                                    row["IMBDig"] = dRNew["Intelligent Mail barcode DIG"];
                                    //row["DL"] = "";
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                        }
                        dt1.PrimaryKey = null;

                        GlobalVar.dbaseName = "BCBS_Horizon";
                        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                        string errors = "";
                        dbU.ExecuteScalar("delete from HOR_parse_Campaigns_tmp");

                        string Ver = "00001";
                        //int GRecnum = 1;
                        //var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
                        //int recordnumber = 0;
                        //if (recnum.ToString() == "")
                        //    GRecnum = 1;
                        //else
                        //    GRecnum = Convert.ToInt32(recnum.ToString()) + 1;
                        foreach (DataRow dr in dt1.Rows)
                        {
                            //dr["Recnum"] = GRecnum;
                            if (dr["letter_Type"].ToString() == "ACA")
                                dr["OutputFileName"] = "HOR_GiftCard_ACA_" + DateTime.Now.ToString("yyyyMMdd") + "_" + Ver;
                            else
                                dr["OutputFileName"] = "HOR_GiftCard_" + DateTime.Now.ToString("yyyyMMdd") + "_" + Ver;

                            // GRecnum++;
                        }
                        DataColumnCollection columns = dt1.Columns;        
                            if (columns.Contains("F10"))
                            {
                                dt1.Columns.Remove("F10");
                            }
                        SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                        Connection.Open();

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                        {
                            bulkCopy.DestinationTableName = "[dbo].[HOR_parse_Campaigns_tmp]";

                            try
                            {
                                bulkCopy.BatchSize = backfromBCC.Rows.Count;
                                bulkCopy.BulkCopyTimeout = 0;
                                bulkCopy.WriteToServer(dt1);
                            }
                            catch (Exception ex)
                            {
                                errors = errors + ex.Message;
                                result = result + ex.Message;
                            }
                        }
                        Connection.Close();
                        if (errors == "")
                        {
                            dbU.ExecuteScalar("Insert into HOR_parse_Campaigns select * from HOR_parse_Campaigns_tmp");
                            //dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_Campaigns', GETDATE())");
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
        public DataTable readcsvMAS023(string fileName, string fname)
        {
            DataTable csvData = new DataTable();

            try
            {

                using (TextFieldParser csvReader = new TextFieldParser(fileName))
                {

                    csvReader.SetDelimiters(new string[] { "," });

                    csvReader.HasFieldsEnclosedInQuotes = true;

                    string[] colFields = csvReader.ReadFields();

                    foreach (string column in colFields)
                    {

                        DataColumn datecolumn = new DataColumn(column);

                        datecolumn.AllowDBNull = true;

                        csvData.Columns.Add(datecolumn);

                    }
                    csvData.Columns.Add("FileName");
                    while (!csvReader.EndOfData)
                    {

                        string[] fieldData = csvReader.ReadFields();

                        //Making empty value as null

                        for (int i = 0; i < fieldData.Length; i++)
                        {

                            if (fieldData[i] == "")
                            {

                                fieldData[i] = null;

                            }

                        }

                        csvData.Rows.Add(fieldData);

                    }

                    foreach (DataRow dr in csvData.Rows)
                    {
                        dr["FileName"] = fname;
                    }
                }

            }

            catch (Exception ex)
            {

            }

            return csvData;

        }
        public string[] GetWeekRange(DateTime dateToCheck)
        {
            string[] result = new string[2];
            TimeSpan duration = new TimeSpan(0, 0, 0, 0); //One day 
            DateTime dateRangeBegin = dateToCheck;
            DateTime dateRangeEnd = DateTime.Today.Add(duration);

            dateRangeBegin = dateToCheck.AddDays(-(int)dateToCheck.DayOfWeek);
            dateRangeEnd = dateToCheck.AddDays(6 - (int)dateToCheck.DayOfWeek);

            result[0] = dateRangeBegin.Date.ToString("yyyy-MM-dd");
            result[1] = dateRangeEnd.Date.ToString("yyyy-MM-dd");
            return result;

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
