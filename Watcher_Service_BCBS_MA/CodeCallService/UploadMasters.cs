using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Configuration;
using System.Text.RegularExpressions;

namespace CodeCallService
{
    public class UploadMasters
    {
        CodeCallService.DBUtility dbU;
        string directoryName = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Library";
        public string uploadMasters()
        {
            string result = "";

            var files = from fileName in
                            Directory.EnumerateFiles(directoryName)
                        where fileName.ToLower().Contains(".txt")
                        select fileName;
            foreach (var fileName in files)
            {
                if (fileName.IndexOf("__") == 0)
                {

                }
                else
                {
                    if (fileName.IndexOf("OLB_Commercial") != -1)
                    {
                        if (fileName.Contains("Collaterals"))
                            upload_Collaterals(fileName, "Collaterals", "X,");
                        else if (fileName.Contains("EOC"))
                            upload_Collaterals(fileName, "EOC", "C,");
                        else if (fileName.Contains("MCC"))
                            upload_Collaterals(fileName, "MCC", "M,");   // CPC must be unique    select cpc, count(cpc) as counts from Master_OLB_CommercialMCC group by CPC having count(cpc) > 1
                        else if (fileName.Contains("Prodcats"))
                            upload_Collaterals(fileName, "Prodcats", "P,");
                        else if (fileName.Contains("Riders"))
                            upload_Collaterals(fileName, "Riders", "R,");
                    }
                }
            }

            return "Upload Masters files Done!!";
        }

        public bool upload_Collaterals(string filename, string dTable, string cpc)
        {
            bool result = false;
            FileInfo finfo = new FileInfo(filename);
            int seq = 1;
            DataTable DataColl = Data_creataTable(dTable);
            string rundate = "";
            string totals = "";
            string[] aa = File.ReadAllLines(filename);
            foreach (var item in aa)
            {
                if (item.ToString().Contains("Report Run Date"))
                {
                    rundate = item.ToString(); ;
                    DataRow drA = DataColl.NewRow();
                    drA[0] = seq.ToString("000000");
                    drA[1] = DateTime.Now.ToString("yyyy-MM-dd");
                    drA[2] = rundate;
                    drA[3] = finfo.Name;
                    drA[4] = "A";
                    DataColl.Rows.Add(drA);
                    seq++;
                }
                else if (item.ToString().Contains(cpc))
                {
                    string newdata = "";
                    if (cpc == "M,")
                        newdata = item + ",";
                    else
                        newdata = item;
                    string[] data = newdata.Split(',');
                    DataRow dr = DataColl.NewRow();
                    dr[0] = seq.ToString("000000"); ;
                    dr[1] = DateTime.Now.ToString("yyyy-MM-dd");
                    dr[2] = rundate;
                    dr[3] = finfo.Name;
                    dr[4] = data[0].ToString();
                    dr[5] = data[1].ToString();
                    dr[6] = data[2].ToString();
                    dr[7] = data[3].ToString();
                    dr[8] = data[4].ToString();
                    DataColl.Rows.Add(dr);
                    seq++;
                }
                else if (item.ToString().Length > 1)
                {
                    totals = totals + item.ToString() + "~";
                }
            }
            if (totals.Length > 1)
                DataColl.Rows[0]["ImportDetails"] = totals;
            if (DataColl.Rows.Count > 1)
            {
                CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
                dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
                SqlParameter[] sqlParams2;
                sqlParams2 = null;
                sqlParams2 = new SqlParameter[] { new SqlParameter("@rdate", rundate) };

                if (dTable == "Collaterals")

                    dbU.ExecuteNonQuery("BCBS_MA_upd_Historical_Collaterals", sqlParams2);
                else if (dTable == "EOC")
                    dbU.ExecuteNonQuery("BCBS_MA_upd_Historical_EOC", sqlParams2);
                else if (dTable == "MMC")
                    dbU.ExecuteNonQuery("BCBS_MA_upd_Historical_MCC", sqlParams2);
                else if (dTable == "Prodcats")
                    dbU.ExecuteNonQuery("BCBS_MA_upd_Historical_Prodcats", sqlParams2);
                else if (dTable == "Riders")
                    dbU.ExecuteNonQuery("BCBS_MA_upd_Historical_Riders", sqlParams2);

                dbU.ExecuteScalar("delete from Master_OLB_Commercial" + dTable + "_Tmp");
                dbU.ExecuteScalar("delete from Master_OLB_Commercial" + dTable);

                int updErrors = 0;
                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[CodeCallService.GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    bulkCopy.DestinationTableName = "[dbo].[Master_OLB_Commercial" + dTable + "_Tmp]";

                    try
                    {
                        bulkCopy.BatchSize = DataColl.Rows.Count;
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.WriteToServer(DataColl);
                    }
                    catch (Exception ex)
                    {
                        //errors = errors + ex.Message;    //colid 27   Member Gender
                        updErrors++;
                    }
                }
                Connection.Close();

                if (updErrors == 0)
                {
                    try
                    {
                        dbU.ExecuteScalar("Insert into Master_OLB_Commercial" + dTable + " select * from Master_OLB_Commercial" + dTable + "_Tmp");

                    }
                    catch (Exception ex)
                    {
                        //errors = errors + ex.Message;    //colid 27   Member Gender
                        updErrors++;
                    }

                }
                if (dTable == "EOC")
                {
                    string wDocType = "";
                    string pattern = @"G([1234567890]+)SOB";
                    dbU.ExecuteNonQuery("Update Master_OLB_Commercial" + dTable + " set DocType = ''");
                    DataTable toupdate = dbU.ExecuteDataTable("Select Seq, FileNames from Master_OLB_Commercial" + dTable);
                    if (toupdate.Rows.Count > 0)
                    {
                        foreach (DataRow row in toupdate.Rows)
                        {

                            Regex rgx = new Regex(pattern);

                            Match match = rgx.Match(row[1].ToString().ToUpper());
                            if (match.Success)
                            {
                                wDocType = "GSOB";
                            }
                            else if (row[1].ToString().ToUpper().IndexOf("SOB") != -1)
                            {
                                wDocType = "SOB";
                            }
                            else
                                wDocType = "EOC";

                            dbU.ExecuteNonQuery("Update Master_OLB_Commercial" + dTable + " set DocType = '" + wDocType + "' where seq = " + row[0].ToString() + " and OLBType <> 'A'");
                        }
                    }
                }
            }
            else
                result = true;
            if (!result)
                File.Move(finfo.FullName, finfo.Directory.ToString() + "\\__" + finfo.Name);
            return result;
        }

        private static DataTable Data_creataTable(string tabName)
        {
            if (tabName == "Collaterals")
            {
                DataTable newt = new DataTable();
                newt.Clear();
                newt.Columns.Add("Seq");
                newt.Columns.Add("Import");
                newt.Columns.Add("RunDate");
                newt.Columns.Add("FileName");
                newt.Columns.Add("OLBtype");
                newt.Columns.Add("CPC");
                newt.Columns.Add("FileDesc");
                newt.Columns.Add("Vfield1");
                newt.Columns.Add("Vfield2");
                newt.Columns.Add("ImportDetails");

                return newt;
            }
            else if (tabName == "EOC")
            {
                DataTable newt = new DataTable();
                newt.Clear();
                newt.Columns.Add("Seq");
                newt.Columns.Add("Import");
                newt.Columns.Add("RunDate");
                newt.Columns.Add("FileName");
                newt.Columns.Add("OLBtype");
                newt.Columns.Add("CPC");
                newt.Columns.Add("FileNames");
                newt.Columns.Add("Vfield1");
                newt.Columns.Add("Vfield2");
                newt.Columns.Add("ImportDetails");

                return newt;
            }
            else if (tabName == "MCC")
            {
                DataTable newt = new DataTable();
                newt.Clear();
                newt.Columns.Add("Seq");
                newt.Columns.Add("Import");
                newt.Columns.Add("RunDate");
                newt.Columns.Add("FileName");
                newt.Columns.Add("OLBtype");
                newt.Columns.Add("CPC");
                newt.Columns.Add("MCCStatus");
                newt.Columns.Add("Vfield1");
                newt.Columns.Add("Vfield2");
                newt.Columns.Add("ImportDetails");

                return newt;
            }
            else if (tabName == "Prodcats")
            {
                DataTable newt = new DataTable();
                newt.Clear();
                newt.Columns.Add("Seq");
                newt.Columns.Add("Import");
                newt.Columns.Add("RunDate");
                newt.Columns.Add("FileName");
                newt.Columns.Add("OLBtype");
                newt.Columns.Add("CPC");
                newt.Columns.Add("PlanGroup");
                newt.Columns.Add("Vfield1");
                newt.Columns.Add("Vfield2");
                newt.Columns.Add("ImportDetails");

                return newt;
            }
            else if (tabName == "Riders")
            {
                DataTable newt = new DataTable();
                newt.Clear();
                newt.Columns.Add("Seq");
                newt.Columns.Add("Import");
                newt.Columns.Add("RunDate");
                newt.Columns.Add("FileName");
                newt.Columns.Add("OLBtype");
                newt.Columns.Add("CPC");
                newt.Columns.Add("FileNames");
                newt.Columns.Add("Vfield1");
                newt.Columns.Add("Vfield2");
                newt.Columns.Add("ImportDetails");

                return newt;
            }
            return null;
        }
    }
}
