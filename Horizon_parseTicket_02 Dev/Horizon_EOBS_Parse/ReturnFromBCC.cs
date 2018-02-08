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
using System.Configuration;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{

    public class ReturnFromBCC
    {
        DBUtility dbU;
        public string process_HORIZ_ReturnBCC_and_upd_Sql(DataTable dt1, FileInfo bccfile, string fName, string tableName, string Nondel)
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
                    //sendMails sendmail = new sendMails();
                    //sendmail.SendMailError("BCBS_MA_Processing EOC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                    break;
                }
            }
            if (getBCCready == "")
            {
                if (File.Exists(bccfile.FullName))
                {

                    BackCASS readresults = new BackCASS();
                    DataTable backfromBCC = readresults.readQualifiedHorizIMB(bccfile.FullName, fName);
                    //DataTable NonD_Records = readresults.readNonDeliverable(bccfile.FullName.Replace(".csv", "-NON-DELIVERABLE.csv"));
                    dt1.PrimaryKey = new DataColumn[] { dt1.Columns["Recnum"] };
                    if (backfromBCC.Rows.Count > 0)
                    {
                        try
                        {
                            backfromBCC.Columns["RETURN_FIELD_01"].ColumnName = "Recnum";
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
                                    row["City"] = dRNew["CITY"];
                                    row["State"] = dRNew["State"];
                                    row["Zip"] = dRNew["ZIP+4"];
                                    //row["UpdCounty"] = "";
                                    //row["IMBChar"] = dRNew["Intelligent Mail barcode"];
                                    //row["IMBDig"] = dRNew["IMPB DIGITS FOR XMPIE"];
                                    if (dRNew["Return Code"].ToString() == "17")
                                        row["DL"] = "N";
                                    else
                                        row["DL"] = "Y";
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                        }

                        if (File.Exists(Nondel))
                        {

                            BackCASS readresultsND = new BackCASS();
                            DataTable backfromBCC_ND = readresults.readQualifiedHorizIMB(Nondel, fName);
                            backfromBCC_ND.Columns["Sysout"].ColumnName = "Recnum";

                            if(backfromBCC_ND.Rows.Count > 1)
                            {
                                foreach (DataRow dRNew in backfromBCC_ND.Rows)
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
                                            row["DL"] = "N";
                                    }
                                }
                            }

                        }



                        int GRecnum = 0;
                        GlobalVar.dbaseName = "BCBS_Horizon";
                        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                        SqlParameter[] sqlParams2;
                        sqlParams2 = null;
                        sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", dt1.Rows.Count),

                                            new SqlParameter("@FileName", fName ), new SqlParameter("@TableName",  tableName) };
                        dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
                        DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + fName + "' and tablename = '" + tableName + "' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
                        if (afterUpdateSeq.Rows.Count == 1)
                            GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - dt1.Rows.Count + 1;
                        else
                        {
                            result = " more than 1 record for file ";
                        }
                        if (result == "")
                        {
                            foreach (DataRow row in dt1.Rows)
                            {
                                row["Recnum"] = GRecnum;

                                GRecnum++;
                            }
                        }


                        GlobalVar.dbaseName = "BCBS_Horizon";
                        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


                        string errors = "";
                        dbU.ExecuteScalar("delete from " + tableName + "_tmp");



                        SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                        Connection.Open();

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                        {
                            bulkCopy.DestinationTableName = "[dbo].[" + tableName + "_tmp]";

                            try
                            {
                                bulkCopy.BatchSize = dt1.Rows.Count;
                                bulkCopy.BulkCopyTimeout = 0;
                                bulkCopy.WriteToServer(dt1);
                            }
                            catch (Exception ex)
                            {
                                errors = errors + ex.Message;
                            }
                        }
                        Connection.Close();
                        if (errors == "")
                            dbU.ExecuteScalar("Insert into " + tableName + " select * from " + tableName + "_tmp");
                    }
                   
                }
                else
                {
                    result = result + bccfile.Name + " OUTPUT.csv not found..." + Environment.NewLine;
                }
            }


            return result;
        }

        public string process_MAS023_ReturnBCC_and_upd_Sql(DataTable dt1, FileInfo bccfile, string fName, string tableName)
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
                    //sendMails sendmail = new sendMails();
                    //sendmail.SendMailError("BCBS_MA_Processing EOC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                    break;
                }
            }
            if (getBCCready == "")
            {
                if (File.Exists(bccfile.FullName))
                {

                    BackCASS readresults = new BackCASS();
                    DataTable backfromBCC = readresults.readQualifiedMAS023(bccfile.FullName, fName);
                    //DataTable NonD_Records = readresults.readNonDeliverable(bccfile.FullName.Replace(".csv", "-NON-DELIVERABLE.csv"));
                    dt1.PrimaryKey = new DataColumn[] { dt1.Columns["Recnum"] };
                    if (backfromBCC.Rows.Count > 0)
                    {
                        try
                        {
                            backfromBCC.Columns["LINE_01"].ColumnName = "Recnum";
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
                                    row["UpdAddr1"] = dRNew["ST_ATTENTION"];
                                    row["UpdAddr2"] = dRNew["ST_COMPANYNAME"];
                                    row["UpdAddr3"] = dRNew["ST_ADDRESS1"];
                                    row["UpdAddr4"] = dRNew["ST_ADDRESS2"];
                                    row["UpdAddr5"] = dRNew["ST_ADDRESS3"];
                                    row["City"] = dRNew["ST_CITY"];
                                    row["State"] = dRNew["ST_STATE_PROV"];
                                    row["Zip"] = dRNew["ST_POSTALCODE"];
                                    //row["UpdCounty"] = "";
                                    //row["IMBChar"] = dRNew["Intelligent Mail barcode"];
                                    //row["IMBDig"] = dRNew["IMPB DIGITS FOR XMPIE"];
                                    row["DL"] = "";
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                        }
                        int GRecnum = 0;
                        SqlParameter[] sqlParams2;
                        sqlParams2 = null;
                        sqlParams2 = new SqlParameter[] { new SqlParameter("@numRecords", dt1.Rows.Count),

                                            new SqlParameter("@FileName", fName + DateTime.Now.ToString("_yyyy_MM_dd")), new SqlParameter("@TableName",  tableName) };
                        dbU.ExecuteNonQuery("HOR_upd_Recnum_beforeTMP", sqlParams2);
                        DataTable afterUpdateSeq = dbU.ExecuteDataTable("Select recnum from HOR_parse_SEQ where Description = '" + fName + DateTime.Now.ToString("_yyyy_MM_dd") + "' and tablename = 'HOR_parse_Commercial_Correspondence_Detail_pdfs' and convert(date,datetime) = '" + DateTime.Now.ToString("yyyy-MM-dd") + "'");
                        if (afterUpdateSeq.Rows.Count == 1)
                            GRecnum = Int32.Parse(afterUpdateSeq.Rows[0][0].ToString()) - dt1.Rows.Count + 1;
                        else
                        {
                            result = " more than 1 record for file ";
                        }
                        if (result == "")
                        {
                            foreach (DataRow row in dt1.Rows)
                            {
                                row["Recnum"] = GRecnum;

                                GRecnum++;
                            }
                        }




                        string errors = "";
                        dbU.ExecuteScalar("delete from " + tableName + "_tmp");



                        SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                        Connection.Open();

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                        {
                            bulkCopy.DestinationTableName = "[dbo].[" + tableName + "_tmp]";

                            try
                            {
                                bulkCopy.BatchSize = backfromBCC.Rows.Count;
                                bulkCopy.BulkCopyTimeout = 0;
                                bulkCopy.WriteToServer(backfromBCC);
                            }
                            catch (Exception ex)
                            {
                                errors = errors + ex.Message;
                            }
                        }
                        Connection.Close();
                        if (errors == "")
                            dbU.ExecuteScalar("Insert into " + tableName + " select * from " + tableName + "_tmp");
                    }

                }
                else
                {
                    result = result + bccfile.Name + " OUTPUT.csv not found..." + Environment.NewLine;
                }
            }


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
