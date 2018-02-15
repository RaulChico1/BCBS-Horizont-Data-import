using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.Configuration;
using System.Xml;
using System.Xml.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Globalization;

namespace CodeCallService
{
    public class UploadEOC
    {
        CodeCallService.DBUtility dbU;
        int GRecnum = 0;

        public string uploadData()
        {

            appSets checkD = new appSets();
            string drivesOk = checkD.checkDrives();
            if (drivesOk == "")
            {

                string resultEOC = "";
                var directory = new DirectoryInfo(ProcessVars.dataEOC);
                var masks = new[] { "*.txt", "*.xls" };
                var files = masks.SelectMany(directory.EnumerateFiles);

                //var files = from fileName in
                //                Directory.EnumerateFiles(ProcessVars.dataEOC)
                //            where fileName.ToLower().Contains(".txt")
                //            select fileName;
                foreach (var fileName in files)
                {
                    FileInfo file = new FileInfo(fileName.FullName.ToString());
                    if (file.Name.IndexOf("__") == -1)
                    {
                        if (file.Name.Contains("EOC"))
                        {
                            if (!file.Name.Contains("MKIT") && !file.Name.Contains("Summary"))
                            {
                                resultEOC = upload_EOC(file.FullName);
                                if (resultEOC == "Process ok")
                                {
                                    string nfilename = file.Directory + "\\__" + file.Name;
                                    if (File.Exists(nfilename))
                                        File.Delete(nfilename);
                                    File.Move(file.FullName, nfilename);
                                    drivesOk = drivesOk + resultEOC + " " + file.Name + Environment.NewLine;
                                }
                            }
                            else
                            {
                                if (file.Name.Contains("MKIT") && !file.Name.Contains("Summary"))
                                {
                                    resultEOC = upload_EOCKit(file.FullName);
                                    if (resultEOC == "process ok")
                                    {
                                        string nfilename = file.Directory + "\\__" + file.Name;
                                        if (File.Exists(nfilename))
                                            File.Delete(nfilename);
                                        File.Move(file.FullName, nfilename);
                                        drivesOk = drivesOk + resultEOC + " " + file.Name + Environment.NewLine;
                                    }
                                }
                            }
                        }
                    }
                }

            }
            return drivesOk;
        }
        public string uploadData_Reprint(string filename)
        {
            string result = "";

            

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            SqlParameter[] sqlParams;

            try
            {
                sqlParams = null;
                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };

                // clean fileds 
                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1", sqlParams);
            }
            catch (Exception exx)
            {
                result = result + exx.Message;
            }
            try
            {
                sqlParams = null;
                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };

                // Updates with ~ 
                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1_MED", sqlParams);
            }
            catch (Exception exx)
            {
                result = result + exx.Message;
            }

            DataTable ProdFamiy = dbU.ExecuteDataTable("select distinct E.Prodfamily, M.[Kit_Type Value], M.Expression from BCBS_MA_parse_eoc E join Master_LetterType M on E.Prodfamily = M.Prodfamily where FileName ='" + filename + "'");
            if (ProdFamiy.Rows.Count > 0)
            {
                foreach (DataRow rowf in ProdFamiy.Rows)
                {
                    try
                    {
                        sqlParams = null;
                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename), new SqlParameter("@Kit", rowf[1].ToString()),
                                                                          new SqlParameter("@expression", rowf[2].ToString())};
                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_2", sqlParams);
                    }
                    catch (Exception exx)
                    {
                        result = result + exx.Message;
                    }

                }
                sqlParams = null;
                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };
                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_3", sqlParams);



            }



            CreateCSV printcsv = new CreateCSV();

            string strsql2 = "select distinct typeassembly, ProdFamily from BCBS_MA_parse_eoc where FileName = '" + filename + "'  and len(ProdFamily) > 0";
            DataTable versions = dbU.ExecuteDataTable(strsql2);
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_DNTL");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_FULL_MEDX");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_FULL_SNR");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_MED");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_MEDX");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_PARTIAL_DNTL");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_PARTIAL_MED");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_PARTIAL_MEDX");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_SNR");

            if (versions.Rows.Count > 0)
            {
                foreach (DataRow rowV in versions.Rows)
                {
                    string Assembly = rowV[0].ToString();
                    string version = rowV[1].ToString();
                    string  strsqlPartial = "select count(*) from BCBS_MA_parse_eoc_XMPI_batch where typeassembly = '" + Assembly + "' and prodfamily = '" + version + "'";

                    int totPartials = Int32.Parse(dbU.ExecuteScalar(strsqlPartial).ToString());
                    if (totPartials == 0)
                    {
                        SqlParameter[] sqlParams2;
                        sqlParams2 = null;
                        sqlParams2 = new SqlParameter[] { new SqlParameter("@Assembly", Assembly),
                                                                        new SqlParameter("@FileName", filename), new SqlParameter("@Version", version) };

                        dbU.ExecuteScalar("BCBS_MA_rpt_EOC_to_XMPie", sqlParams2);
                        string strsql3 = "select * from R_XMPie_eoc_" + Assembly + "_" + version + " order by recnum";
                        DataTable resultXmpie = dbU.ExecuteDataTable(strsql3);
                        CreateCSV printcsv2 = new CreateCSV();
                        string printfile = ProcessVars.dataEOC + @"\" + filename.Substring(0, filename.Length - 4) + "_" + Assembly + "_" + version + "_toXMPie.csv";

                        if (File.Exists(printfile))
                            File.Delete(printfile);

                        printcsv.printCSV_fullProcess(printfile, resultXmpie, "", "Y");
                    }
                }
            }
            strsql2 = "select distinct typeassembly, ProdFamily from BCBS_MA_parse_eoc where FileName = '" + filename + "'  and len(ProdFamily) > 0";
            DataTable versionsB = dbU.ExecuteDataTable(strsql2);
            if (versionsB.Rows.Count > 0)
            {
                //string batchNum = "1";
                //foreach (DataRow rowV in versionsB.Rows)
                //{
                //    string Assembly = rowV[0].ToString();
                //    string version = rowV[1].ToString();
                //    string strsqlPartial = "select * from BCBS_MA_parse_eoc_XMPI_batches where typeassembly = '" + Assembly + "' and prodfamily = '" + version + "' and splitmode ='" + batchNum + "'";

                //    DataTable tblPartials =dbU.ExecuteDataTable(strsqlPartial);
                //    if (tblPartials.Rows.Count > 0)
                //    {
                //        int lim1 = Int32.Parse(tblPartials.Rows[0][5].ToString());
                //        int lim2 = Int32.Parse(tblPartials.Rows[0][6].ToString());
                //        SqlParameter[] sqlParams2;
                //        sqlParams2 = null;
                //        sqlParams2 = new SqlParameter[] { new SqlParameter("@Assembly", Assembly),
                //            new SqlParameter("@FileName", filename), new SqlParameter("@Version", version),
                //            new SqlParameter("@batch", batchNum),
                //            new SqlParameter("@Limit1", lim1), new SqlParameter("@Limit2", lim2) };

                //        dbU.ExecuteScalar("BCBS_MA_rpt_EOC_to_XMPie_batch", sqlParams2);
                //        string strsql3 = "select * from R_XMPie_eoc_" + Assembly + "_" + version + " order by recnum";
                //        DataTable resultXmpie = dbU.ExecuteDataTable(strsql3);
                //        CreateCSV printcsv2 = new CreateCSV();
                //        string printfile = ProcessVars.dataEOC + @"\" + filename.Substring(0, filename.Length - 4) + "_" + Assembly + "_" + version + "_toXMPie.csv";

                //        if (File.Exists(printfile))
                //            File.Delete(printfile);

                //        printcsv.printCSV_fullProcess(printfile, resultXmpie, "", "Y");
                //    }
                //}
            }


            printSummary(filename);

            return result;
        }
        public string uploadData_ReprintBatch(string filename)
        {
            string result = "";

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);

            SqlParameter[] sqlParams;

            try
            {
                sqlParams = null;
                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };

                // clean fileds 
                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1", sqlParams);
            }
            catch (Exception exx)
            {
                result = result + exx.Message;
            }
            try
            {
                sqlParams = null;
                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };

                // Updates with ~ 
                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1_MED", sqlParams);
            }
            catch (Exception exx)
            {
                result = result + exx.Message;
            }

            DataTable ProdFamiy = dbU.ExecuteDataTable("select distinct E.Prodfamily, M.[Kit_Type Value], M.Expression from BCBS_MA_parse_eoc E join Master_LetterType M on E.Prodfamily = M.Prodfamily where FileName ='" + filename + "'");
            if (ProdFamiy.Rows.Count > 0)
            {
                foreach (DataRow rowf in ProdFamiy.Rows)
                {
                    try
                    {
                        sqlParams = null;
                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename), new SqlParameter("@Kit", rowf[1].ToString()),
                                                                          new SqlParameter("@expression", rowf[2].ToString())};
                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_2", sqlParams);
                    }
                    catch (Exception exx)
                    {
                        result = result + exx.Message;
                    }

                }
                try
                {
                    sqlParams = null;
                    sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };
                    dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_3", sqlParams);
                }
                catch (Exception exx)
                {
                    result = result + exx.Message;
                }


            }
            string batchNum = "14";

            //SqlParameter[] sqlParams;

            

            CreateCSV printcsv = new CreateCSV();

            string strsql2 = "select distinct typeassembly, ProdFamily from BCBS_MA_parse_eoc where FileName = '" + filename + "'  and len(ProdFamily) > 0";
            DataTable versions = dbU.ExecuteDataTable(strsql2);
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_DNTL");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_FULL_MEDX");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_FULL_SNR");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_MED");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_MEDX");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_PARTIAL_DNTL");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_PARTIAL_MED");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_PARTIAL_MEDX");
            dbU.ExecuteNonQuery("DELETE FROM R_XMPie_eoc_SNR");

            if (versions.Rows.Count > 0)
            {
                foreach (DataRow rowV in versions.Rows)
                {
                  
                    string Assembly = rowV[0].ToString();
                    string version = rowV[1].ToString();
                    string strsqlPartial = "select * from BCBS_MA_parse_eoc_XMPI_batches where typeassembly = '" + Assembly + "' and prodfamily = '" 
                        + version + "' and splitmode ='" + batchNum + "'";

                    DataTable tblPartials = dbU.ExecuteDataTable(strsqlPartial);
                    if (tblPartials.Rows.Count > 0)
                    {
                        int lim1 = Int32.Parse(tblPartials.Rows[0][5].ToString());
                        int lim2 = Int32.Parse(tblPartials.Rows[0][6].ToString());
                        SqlParameter[] sqlParams2;
                        sqlParams2 = null;
                        sqlParams2 = new SqlParameter[] { new SqlParameter("@Assembly", Assembly),
                            new SqlParameter("@FileName", filename), new SqlParameter("@Version", version),
                            new SqlParameter("@batch", batchNum),
                            new SqlParameter("@Limit1", lim1), new SqlParameter("@Limit2", lim2) };

                        dbU.ExecuteScalar("BCBS_MA_rpt_EOC_to_XMPie_batch", sqlParams2);
                        string strsql3 = "select * from R_XMPie_eoc_" + Assembly + "_" + version + " order by recnum";
                        DataTable resultXmpie = dbU.ExecuteDataTable(strsql3);
                        CreateCSV printcsv2 = new CreateCSV();
                        string printfile = ProcessVars.dataEOC + @"\" + filename.Substring(0, filename.Length - 4) + "_" + Assembly + "_" + version + "_toXMPie.csv";

                        if (File.Exists(printfile))
                            File.Delete(printfile);

                        printcsv.printCSV_fullProcess(printfile, resultXmpie, "", "Y");
                    }
                }
            }
            

            printSummary(filename);

            return result;
        }
        public string uploadData_AcctReprint(string filename)
        {
            string result = "";
            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);

            SqlParameter[] sqlParams;
            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };
            try
            {
                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Acct_Step_1", sqlParams);
            }
            catch (Exception ex)
            {
                var errors1 = ex.Message;
            }

            SqlParameter[] sqlParams2;
            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", filename) };
            try
            {
                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Acct_Directories_new", sqlParams2);
            }
            catch (Exception ex2)
            {
                var errors1 = ex2.Message;
            }

            DataTable DirestoUpdate = dbU.ExecuteDataTable(" SELECT recnum, wdirectory FROM BCBS_MA_parse_eoc_Acct WHERE FILENAME = '" + filename + "' AND Ltr_Plan IN ('L') and dup = 'N' and len(wdirectory) > 0");
            foreach (DataRow rows in DirestoUpdate.Rows)
            {
                string[] dirs = rows[1].ToString().Split('~');
                var distinctDirs = dirs.Distinct().ToArray();
                string CDs = "";
                foreach (string item in distinctDirs)
                {
                    var valCd = dbU.ExecuteScalar("select CD from Master_DirType where dirtype = '" + item + "'");
                    if (valCd == null)
                        CDs = CDs;
                    else if (valCd.ToString() != "")
                        CDs = CDs + "," + valCd.ToString();
                }
                string oCDs = "";
                if (CDs.Substring(0, 1) == ",")
                    oCDs = CDs.Substring(1, CDs.Length - 1);
                else
                    oCDs = CDs;
                dbU.ExecuteScalar("Update BCBS_MA_parse_eoc_Acct set oDirectory = '" + oCDs + "' where recnum = " + rows[0].ToString());

            }

            sqlParams2 = null;
            sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", filename) };
            dbU.ExecuteScalar("BCBS_MA_rpt_EOC_Acctto_XMPie", sqlParams2);


            CreateCSV printcsv = new CreateCSV();

            DataTable lettertoXmpie = dbU.ExecuteDataTable("select * from R_XMPie_eoc_Acct order by recnum");


            string letterFname = ProcessVars.dataEOC + filename.Substring(0, filename.Length - 5) + "_Letter_toXMPie.csv";
            if (File.Exists(letterFname))
                File.Delete(letterFname);

            printcsv.printCSV_fullProcessNoBlank(letterFname, lettertoXmpie, "", "Y");

            DataTable PlanTabletoXmpie = dbU.ExecuteDataTable("select * from PLAN_TABLE order by recnum");


            letterFname = ProcessVars.dataEOC + @"\Plan_Table.csv";
            if (File.Exists(letterFname))
                File.Delete(letterFname);

            printcsv.printCSV_fullProcessNoBlank(letterFname, PlanTabletoXmpie, "", "Y");
            //====================================
            DataTable letterNewtoXmpie = dbU.ExecuteDataTable("select * from R_XMPie_eoc_Acct_new order by recnum");

            letterFname = ProcessVars.dataEOC + filename.Substring(0, filename.Length - 5) + "_NEW_Letter_toXMPie.csv";
            if (File.Exists(letterFname))
                File.Delete(letterFname);

            printcsv.printCSV_fullProcessNoBlank(letterFname, letterNewtoXmpie, "", "Y");

            //DataTable PlanTabletoXmpieNew = dbU.ExecuteDataTable("select * from PLAN_TABLE_new order by recnum");


            //letterFname = ProcessVars.dataEOC + @"\New_Plan_Table.csv";
            //if (File.Exists(letterFname))
            //    File.Delete(letterFname);

            //printcsv.printCSV_fullProcessNoBlank(letterFname, PlanTabletoXmpieNew, "", "Y");

            string strsql = "select oDirectory from BCBS_MA_parse_eoc_Acct where filename = '" + filename + "' and Ltr_Plan = 'L' and status = 'New'";
            DataTable dataTosplit = dbU.ExecuteDataTable(strsql);
            DataTable working_dataTosplit = dataTosplit.Clone();

            foreach (DataRow dr in dataTosplit.Rows)
            {
                string[] CDs = dr[0].ToString().Split(',');
                foreach (string item in CDs)
                {
                    if (item.Length > 1)
                    {
                        var rowNew = working_dataTosplit.NewRow();
                        rowNew["oDirectory"] = item.Trim();
                        working_dataTosplit.Rows.Add(rowNew);

                    }
                }
            }
            var groupedData = from b in working_dataTosplit.AsEnumerable()
                              group b by b.Field<string>("oDirectory") into g
                              select new
                              {
                                  ChargeTag = g.Key,
                                  Count = g.Count()
                                  // ChargeSum = g.Sum(x => x.Field<int>("charge"))
                              };
            DataTable PickList = new DataTable();
            PickList.Columns.Add("CD", typeof(string));
            PickList.Columns.Add("Qty", typeof(String));
            foreach (var element in groupedData)
            {
                var row2 = PickList.NewRow();
                row2["CD"] = element.ChargeTag;
                row2["Qty"] = element.Count;
                PickList.Rows.Add(row2);
            }
            string strsqlSumm = "select cycle, filename,Status , count(*) as Counts from BCBS_MA_parse_eoc_Acct where filename = '" + filename + "'  and Ltr_Plan = 'L' group by cycle, filename, Status";
            DataTable dataSummary = dbU.ExecuteDataTable(strsqlSumm);
            var row2a = PickList.NewRow();
            row2a["CD"] = "";
            row2a["Qty"] = "";
            PickList.Rows.Add(row2a);
            var row2s = PickList.NewRow();
            row2s["CD"] = "Status";
            row2s["Qty"] = "Qty";
            PickList.Rows.Add(row2s);

            foreach (DataRow rowS in dataSummary.Rows)
            {
                var row2 = PickList.NewRow();
                row2["CD"] = rowS[2].ToString();
                row2["Qty"] = rowS[3].ToString();
                PickList.Rows.Add(row2);

            }

            //var StatusData = from b in working_dataTosplit.AsEnumerable()
            //                  group b by b.Field<string>("Status") into g
            //                  select new
            //                  {
            //                      ChargeTag = g.Key,
            //                      Count = g.Count()
            //                      // ChargeSum = g.Sum(x => x.Field<int>("charge"))
            //                  };
            //var row = PickList.NewRow();
            //row["CD"] = "";
            //row["Qty"] = "";
            //PickList.Rows.Add(row);

            //row = PickList.NewRow();
            //row["CD"] = "-----";
            //row["Qty"] = "-----";
            //PickList.Rows.Add(row);




            //foreach (var element in StatusData)
            //{
            //    var row2 = PickList.NewRow();
            //    row2["CD"] = element.ChargeTag;
            //    row2["Qty"] = element.Count;
            //    PickList.Rows.Add(row2);
            //}


            letterFname = ProcessVars.dataEOC + @"\Pick_Summary_" + filename.Substring(0, filename.Length - 4) + ".csv";
            if (File.Exists(letterFname))
                File.Delete(letterFname);

            printcsv.printCSV_fullProcessNoBlank(letterFname, PickList, "", "");

            return result;
            //  Check this total by new 
            //SELECT   XMPiePrinted, count(*) as records   FROM [BCBS_MA].[dbo].[BCBS_MA_parse_eoc_Acct] where filename = '062017_AcctEOC.xlsx' and Ltr_Plan = 'L'   group by XMPiePrinted
        }
        public string uploadDataAcct()
        {

            appSets checkD = new appSets();
            string drivesOk = checkD.checkDrives();
            if (drivesOk == "")
            {

                string resultEOC = "";
                var files = from fileName in
                                Directory.EnumerateFiles(ProcessVars.dataEOC)
                            where fileName.ToUpper().Contains("ACCT")
                            select fileName;
                foreach (var fileName in files)
                {
                    FileInfo file = new FileInfo(fileName);
                    if (fileName.Contains(".xlsx"))
                        resultEOC = upload_EOCAcct(fileName);
                    if (resultEOC == "process ok")
                    {
                        string nfilename = file.Directory + "\\__" + file.Name;
                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(file.FullName, nfilename);
                        drivesOk = drivesOk + resultEOC + " " + file.Name + Environment.NewLine;
                    }

                }
                //summary qry:
                //SELECT   XMPiePrinted, count(*) as records   FROM [BCBS_MA].[dbo].[BCBS_MA_parse_eoc_Acct] where filename = '062017_AcctEOC.xlsx' and Ltr_Plan = 'L'   group by XMPiePrinted
            }
            return drivesOk;
        }
        public string upload_EOCAcct(string filename)
        {
            string result = "";
            FileInfo finfo = new FileInfo(filename);
            try
            {

                int errorcount = 0;
                if (!File.Exists(CodeCallService.ProcessVars.gmappingFile))
                    throw new Exception("Mapping file not found.");

                CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
                dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from BCBS_MA_parse_eoc_Acct where filename = '" + finfo.Name + "'");
                dbU.ExecuteScalar("delete from BCBS_MA_parse_fromBCC where filename = '" + finfo.Name + "'");
                var recnum = dbU.ExecuteScalar("select max(Recnum) from Master_SEQ");
                int GRecnum = 0;
                int recordnumber = 0;
                if (recnum.ToString() == "")
                    GRecnum = 1;
                else
                    GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

                DataTable dt1 = new DataTable();

                //===============================
                DataTable fromXLSTmp = loadXLSX(filename);
                fromXLSTmp.Columns.Add("ImportDate").SetOrdinal(0);
                fromXLSTmp.Columns.Add("FileName").SetOrdinal(0);
                fromXLSTmp.Columns.Add("Recnum").SetOrdinal(0);
                fromXLSTmp.Columns.Add("Dup");


                fromXLSTmp.Columns.Add("UpdAddr1");
                fromXLSTmp.Columns.Add("UpdAddr2");
                fromXLSTmp.Columns.Add("UpdAddr3");
                fromXLSTmp.Columns.Add("UpdAddr4");
                fromXLSTmp.Columns.Add("UpdAddr5");
                fromXLSTmp.Columns.Add("UpdCity");
                fromXLSTmp.Columns.Add("UpdState");
                fromXLSTmp.Columns.Add("UpdZip");
                fromXLSTmp.Columns.Add("UpdCounty");
                fromXLSTmp.Columns.Add("IMBChar");
                fromXLSTmp.Columns.Add("IMBDig");
                fromXLSTmp.Columns.Add("DL");
                fromXLSTmp.Columns.Add("ReturnCode");



                fromXLSTmp.Columns.Add("In_Network");
                fromXLSTmp.Columns.Add("Out_of_Network");
                fromXLSTmp.Columns.Add("Total");
                fromXLSTmp.Columns.Add("ind_rate_OON");
                fromXLSTmp.Columns.Add("ind_rate_IN");
                fromXLSTmp.Columns.Add("ind_rate_TOTAL");

                fromXLSTmp.Columns.Add("coup_rate_OON");
                fromXLSTmp.Columns.Add("coup_rate_IN");
                fromXLSTmp.Columns.Add("coup_rate_TOTAL");

                fromXLSTmp.Columns.Add("spwmd_OON");
                fromXLSTmp.Columns.Add("spwmd_IN");
                fromXLSTmp.Columns.Add("spwmd_TOTAL");

                fromXLSTmp.Columns.Add("fam_rate_OON");
                fromXLSTmp.Columns.Add("fam_rate_IN");
                fromXLSTmp.Columns.Add("fam_rate_TOTAL");

                fromXLSTmp.Columns.Add("o_ind_rate");
                fromXLSTmp.Columns.Add("o_coup_rate");

                fromXLSTmp.Columns.Add("o_spwmd");
                fromXLSTmp.Columns.Add("o_fam_rate");
                fromXLSTmp.Columns.Add("Ltr_Plan");

                foreach (DataRow row in fromXLSTmp.Rows)
                {

                    row["ImportDate"] = DateTime.Now;
                    row["FileName"] = finfo.Name;
                    row["Recnum"] = GRecnum;
                    row["Dup"] = "D";
                    if (GRecnum == 206)
                        result = result;
                    GRecnum++;
                    string choicePlan = "";
                    //if(row["ua_cov_cd"].ToString() == )
                    string[] choiceP = new string[] { "0052", "0060", "0073", "0090", "0091", "0093", "0117", "0118", "0140", "0141", "0142", "0143", "0277", "0278" };
                    foreach (string sS in choiceP)
                    {
                        switch (row["ua_cov_cd"].ToString().Contains(sS))
                        {
                            case true:
                                choicePlan = "Y";
                                break;
                            default:
                                break;
                        }
                    }
                    if (choicePlan == "Y")
                    {
                        row["In_Network"] = "In-Network";
                        row["Out_of_Network"] = "Out-of-Network";
                        row["Total"] = "Total";
                    }
                    else
                    {
                        row["In_Network"] = "";
                        row["Out_of_Network"] = "";
                        row["Total"] = "";
                    }
                    double number_indRate;
                    double number_coupRate;
                    double number_spwmd;
                    double number_fam_rate;

                    if (choicePlan == "Y")
                    {
                        if (Double.TryParse(row["ind_rate"].ToString(), out number_indRate))
                        {
                            if (number_indRate == 0)
                            {
                                row["ind_rate_OON"] = "-";
                                row["ind_rate_IN"] = "-";
                                row["ind_rate_TOTAL"] = "-";
                                row["o_ind_rate"] = " ";
                            }
                            else
                            {
                                row["ind_rate_OON"] = (number_indRate * 0.05).ToString("C", CultureInfo.CurrentCulture);
                                row["ind_rate_IN"] = (number_indRate * 0.95).ToString("C", CultureInfo.CurrentCulture);
                                row["ind_rate_TOTAL"] = (number_indRate).ToString("C", CultureInfo.CurrentCulture);
                                row["o_ind_rate"] = " ";
                            }
                        }
                        else
                        {
                            row["ind_rate_OON"] = "";
                            row["ind_rate_IN"] = "";
                            row["ind_rate_TOTAL"] = "";
                            row["o_ind_rate"] = " ";
                        }
                    }
                    else
                    {
                        if (Double.TryParse(row["ind_rate"].ToString(), out number_indRate))
                        {
                            if (number_indRate == 0)
                            {
                                row["ind_rate_OON"] = "";
                                row["ind_rate_IN"] = "";
                                row["ind_rate_TOTAL"] = "";
                                row["o_ind_rate"] = "-";
                            }
                            else
                            {
                                row["ind_rate_OON"] = "";
                                row["ind_rate_IN"] = "";
                                row["ind_rate_TOTAL"] = "";
                                row["o_ind_rate"] = (number_indRate).ToString("C", CultureInfo.CurrentCulture);
                            }
                        }
                        if (Double.TryParse(row["coup_rate"].ToString(), out number_coupRate))
                        {
                            if (number_coupRate == 0)
                            {
                                row["coup_rate_OON"] = "";
                                row["coup_rate_IN"] = "";
                                row["coup_rate_TOTAL"] = "";
                                row["o_coup_rate"] = "-";
                            }
                            else
                            {
                                row["coup_rate_OON"] = "";
                                row["coup_rate_IN"] = "";
                                row["coup_rate_TOTAL"] = "";
                                row["o_coup_rate"] = (number_coupRate).ToString("C", CultureInfo.CurrentCulture);
                            }
                        }
                        if (Double.TryParse(row["spwmd"].ToString(), out number_spwmd))
                        {
                            if (number_spwmd == 0)
                            {
                                row["spwmd_OON"] = "";
                                row["spwmd_IN"] = "";
                                row["spwmd_TOTAL"] = "";
                                row["o_spwmd"] = "-";
                            }
                            else
                            {
                                row["spwmd_OON"] = "";
                                row["spwmd_IN"] = "";
                                row["spwmd_TOTAL"] = "";
                                row["o_spwmd"] = (number_spwmd).ToString("C", CultureInfo.CurrentCulture);
                            }
                        }
                        if (Double.TryParse(row["fam_rate"].ToString(), out number_fam_rate))
                        {
                            if (number_fam_rate == 0)
                            {
                                row["fam_rate_OON"] = "";
                                row["fam_rate_IN"] = "";
                                row["fam_rate_TOTAL"] = "";
                                row["o_fam_rate"] = "-";
                            }
                            else
                            {
                                row["fam_rate_OON"] = "";
                                row["fam_rate_IN"] = "";
                                row["fam_rate_TOTAL"] = "";
                                row["o_fam_rate"] = (number_fam_rate).ToString("C", CultureInfo.CurrentCulture);
                            }
                        }
                    }




                    if (choicePlan == "Y")
                    {
                        if (Double.TryParse(row["coup_rate"].ToString(), out number_coupRate))
                        {
                            if (number_coupRate == 0)
                            {
                                row["coup_rate_OON"] = "-";
                                row["coup_rate_IN"] = "-";
                                row["coup_rate_TOTAL"] = "-";
                                row["o_coup_rate"] = " ";
                            }
                            else
                            {
                                row["coup_rate_OON"] = (number_coupRate * 0.05).ToString("C", CultureInfo.CurrentCulture);
                                row["coup_rate_IN"] = (number_coupRate * 0.95).ToString("C", CultureInfo.CurrentCulture);
                                row["coup_rate_TOTAL"] = (number_coupRate).ToString("C", CultureInfo.CurrentCulture);
                                row["o_coup_rate"] = " "; // (number_coupRate).ToString("C", CultureInfo.CurrentCulture);
                            }
                        }
                        else
                        {
                            row["coup_rate_OON"] = "";
                            row["coup_rate_IN"] = "";
                            row["coup_rate_TOTAL"] = "";
                            row["o_coup_rate"] = " ";
                        }
                    }




                    if (choicePlan == "Y")
                    {
                        if (Double.TryParse(row["spwmd"].ToString(), out number_spwmd))
                        {
                            if (number_spwmd == 0)
                            {
                                row["spwmd_OON"] = "-";
                                row["spwmd_IN"] = "-";
                                row["spwmd_TOTAL"] = "-";
                                row["o_spwmd"] = " ";
                            }
                            else
                            {
                                row["spwmd_OON"] = (number_spwmd * 0.05).ToString("C", CultureInfo.CurrentCulture);
                                row["spwmd_IN"] = (number_spwmd * 0.95).ToString("C", CultureInfo.CurrentCulture);
                                row["spwmd_TOTAL"] = (number_spwmd).ToString("C", CultureInfo.CurrentCulture);
                                row["o_spwmd"] = " "; // (number_spwmd).ToString("C", CultureInfo.CurrentCulture);
                            }
                        }
                        else
                        {
                            row["spwmd_OON"] = "";
                            row["spwmd_IN"] = "";
                            row["spwmd_TOTAL"] = "";
                            row["o_spwmd"] = " ";
                        }
                    }




                    if (choicePlan == "Y")
                    {
                        if (Double.TryParse(row["fam_rate"].ToString(), out number_fam_rate))
                        {
                            if (number_fam_rate == 0)
                            {
                                row["fam_rate_OON"] = "-";
                                row["fam_rate_IN"] = "-";
                                row["fam_rate_TOTAL"] = "-";
                                row["o_fam_rate"] = " ";
                            }
                            else
                            {
                                row["fam_rate_OON"] = (number_fam_rate * 0.05).ToString("C", CultureInfo.CurrentCulture);
                                row["fam_rate_IN"] = (number_fam_rate * 0.95).ToString("C", CultureInfo.CurrentCulture);
                                row["fam_rate_TOTAL"] = (number_fam_rate).ToString("C", CultureInfo.CurrentCulture);
                                row["o_fam_rate"] = " "; //  (number_fam_rate).ToString("C", CultureInfo.CurrentCulture);
                            }
                        }
                        else
                        {
                            row["fam_rate_OON"] = "";
                            row["fam_rate_IN"] = "";
                            row["fam_rate_TOTAL"] = "";
                            row["o_fam_rate"] = " ";
                        }
                    }
                }


                //DataTable uniqueContacts = fromXLSTmp.AsEnumerable()
                //           .GroupBy(x => x.Field<string>("acct_num"))
                //           .Select(g => g.First()).CopyToDataTable();

                DataTable uniqueContacts = fromXLSTmp.AsEnumerable()
                       .GroupBy(r => new { Status = r["Status"], acctnum = r["acct_num"], covpack = r["cov_pack"] })
                       .Select(g => g.First()).CopyToDataTable();

                foreach (DataRow dRNew in uniqueContacts.Rows)
                {
                    DataRow row = null;
                    try
                    {
                        row = fromXLSTmp.Rows.Find(dRNew["Recnum"].ToString());
                    }
                    catch (MissingPrimaryKeyException)
                    {
                        row = fromXLSTmp.Select("Recnum='" + dRNew["Recnum"] + "'").First();
                    }
                    if (row != null)
                    {
                        row["Dup"] = "N";
                    }
                }

                DataTable Letter_or_Plan = fromXLSTmp.AsEnumerable()
                    .Where(r => r["Dup"] == "N")
                    .GroupBy(r => new { acctnum = r["acct_num"] })
                      .Select(g => g.First()).CopyToDataTable();

                foreach (DataRow dRNew in Letter_or_Plan.Rows)
                {
                    DataRow row = null;
                    try
                    {
                        row = fromXLSTmp.Rows.Find(dRNew["Recnum"].ToString());
                    }
                    catch (MissingPrimaryKeyException)
                    {
                        row = fromXLSTmp.Select("Recnum='" + dRNew["Recnum"] + "'").First();
                    }
                    if (row != null)
                    {
                        row["Ltr_Plan"] = "L";
                    }
                }

                foreach (DataRow row in fromXLSTmp.Rows)
                {
                    if (row["Ltr_Plan"] != "L" && row["Dup"] == "N")
                        row["Ltr_Plan"] = "P";
                }


                if (fromXLSTmp.Rows.Count > 0)
                {
                    DataTable toBCC = new System.Data.DataTable();
                    toBCC.Columns.Add("Recnum");
                    toBCC.Columns.Add("FullName");
                    toBCC.Columns.Add("Addr1");
                    toBCC.Columns.Add("Addr2");
                    toBCC.Columns.Add("Addr5");
                    foreach (DataRow row in fromXLSTmp.Rows)
                    {
                        if (row["Dup"].ToString() == "N")
                        {
                            var rowBCC = toBCC.NewRow();
                            rowBCC["Recnum"] = row["Recnum"].ToString();
                            rowBCC["FullName"] = row["Contact_1_name"].ToString().Trim();
                            rowBCC["Addr1"] = row["Contact_1_addr_1"].ToString();
                            rowBCC["Addr2"] = row["Contact_1_addr_2"].ToString();
                            rowBCC["Addr5"] = (row["Contact_1_city"].ToString() + ' ' + row["Contact_1_state"].ToString() + ' ' + row["Contact_1_zip"].ToString()).Trim();
                            toBCC.Rows.Add(rowBCC);
                        }
                    }
                    for (int i = 0; i < 13; i++)
                    {
                        toBCC.Columns.Add("F" + i, typeof(string)).SetOrdinal(1);
                    }

                    toBCC.Columns.Add("Add4", typeof(string)).SetOrdinal(17);
                    toBCC.Columns.Add("Add3", typeof(string)).SetOrdinal(17);


                    string wbccName = ProcessVars.dataEOC + @"\" + "MAS021_" + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_toBCC.csv";
                    string bccName = ProcessVars.dmpsWatched + "MAS021_" + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_toBCC.csv";
                    string bccready = ProcessVars.oDMPsDirectoryM + "MAS023_MAS021_" + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_toBCC_PROCESSED.csv";

                    if (File.Exists(bccready))
                        File.Delete(bccready);

                    //HORIZ_CON2_20170215_NSR_NASCO_HIX_76119_PROCESSED_toBCC.csv
                    CreateCSV printcsv = new CreateCSV();

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
                            sendMails sendmail = new sendMails();
                            sendmail.SendMailError("BCBS_MA_Processing EOC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");

                            break;
                        }
                    }
                    string resultBCC = "";
                    if (getBCCready == "")
                    {




                        resultBCC = processReturnBCC_and_upd_Sql(fromXLSTmp, infoBCCreadfy, finfo.Name);

                        if (resultBCC == "")
                        {

                            string testname = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Test_BCBS_MA\test.csv";
                            if (File.Exists(testname))
                                File.Delete(testname);

                            printcsv.printCSV_fullProcess(testname, fromXLSTmp, "", "");


                            string errors = "";
                            dbU.ExecuteScalar("delete from BCBS_MA_parse_eoc_Acct_tmp");



                            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                            Connection.Open();

                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                            {
                                bulkCopy.DestinationTableName = "[dbo].[BCBS_MA_parse_eoc_Acct_tmp]";

                                try
                                {
                                    bulkCopy.BatchSize = fromXLSTmp.Rows.Count;
                                    bulkCopy.BulkCopyTimeout = 0;
                                    bulkCopy.WriteToServer(fromXLSTmp);
                                }
                                catch (Exception ex)
                                {
                                    errors = errors + ex.Message;
                                }
                            }
                            Connection.Close();

                            if (errors == "")
                            {
                                try
                                {
                                    dbU.ExecuteScalar("Insert into BCBS_MA_parse_eoc_Acct select * from BCBS_MA_parse_eoc_Acct_tmp");

                                    dbU.ExecuteScalar("Insert into Master_SEQ(recnum, Tablename, Description, DateTime) values(" + (GRecnum - 1) + ",'BCBS_MA_parse_eoc_Acct','" + finfo.Name + "',GETDATE())");

                                    var lastCycle = dbU.ExecuteScalar("select max( cycle) from BCBS_MA_parse_eoc_Acct");
                                    string inputCycle = Microsoft.VisualBasic.Interaction.InputBox("Enter Cycle", "Last Cycle " + lastCycle.ToString(), lastCycle.ToString(), -1, -1);
                                    var cycleExist = dbU.ExecuteScalar("select cycle from BCBS_MA_parse_eoc_Acct where cycle = '" + inputCycle.ToString() + "'");
                                    if (inputCycle != "" && cycleExist == null)
                                        dbU.ExecuteScalar("update BCBS_MA_parse_eoc_Acct set cycle = '" + inputCycle.ToString() + "' where filename = '" + finfo.Name + "'");
                                    else
                                        errors = "wrong cycle entered";


                                    // string testname = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Test_BCBS_MA\test.csv";
                                    SqlParameter[] sqlParams;


                                    //update ProdFamilies
                                    sqlParams = null;
                                    sqlParams = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };
                                    try
                                    {
                                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Acct_Step_1", sqlParams);
                                    }
                                    catch (Exception ex)
                                    {
                                        var errors1 = ex.Message;
                                    }
                                    SqlParameter[] sqlParams2;
                                    sqlParams2 = null;
                                    sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };

                                    dbU.ExecuteScalar("BCBS_MA_upd_EOC_Acct_Directories_new", sqlParams2);
                                    DataTable DirestoUpdate = dbU.ExecuteDataTable(" SELECT recnum, wdirectory FROM BCBS_MA_parse_eoc_Acct WHERE FILENAME = '" + finfo.Name + "' AND Ltr_Plan IN ('L') and dup = 'N' and len(wdirectory) > 0");
                                    foreach (DataRow row in DirestoUpdate.Rows)
                                    {

                                        string[] dirs = row[1].ToString().Split('~');
                                        var distinctDirs = dirs.Distinct().ToArray();
                                        string CDs = "";
                                        try
                                        {
                                            foreach (string item in distinctDirs)
                                            {
                                                var valCd = dbU.ExecuteScalar("select CD from Master_DirType where dirtype = '" + item + "'");
                                                if (valCd == null)
                                                    CDs = CDs;
                                                else if (valCd.ToString() != "")
                                                    CDs = CDs + "," + valCd.ToString();
                                            }
                                            string oCDs = "";
                                            if (CDs.Length > 0)
                                            {
                                                if (CDs.Substring(0, 1) == ",")
                                                    oCDs = CDs.Substring(1, CDs.Length - 1);
                                                else
                                                    oCDs = CDs;
                                                dbU.ExecuteScalar("Update BCBS_MA_parse_eoc_Acct set oDirectory = '" + oCDs + "' where recnum = " + row[0].ToString());
                                            }
                                            else
                                                oCDs = CDs;
                                        }
                                        catch (Exception ex)
                                        {
                                            var errors1 = ex.Message;
                                        }
                                    }

                                    sqlParams2 = null;
                                    sqlParams2 = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };
                                    dbU.ExecuteScalar("BCBS_MA_rpt_EOC_Acctto_XMPie", sqlParams2);


                                    //CreateCSV printcsv = new CreateCSV();

                                    DataTable lettertoXmpie = dbU.ExecuteDataTable("select * from R_XMPie_eoc_Acct order by recnum");


                                    string letterFname = ProcessVars.dataEOC + @"\" + finfo.Name.Substring(0, finfo.Name.Length - 5) + "_Retent_Letter_toXMPie.csv";
                                    if (File.Exists(letterFname))
                                        File.Delete(letterFname);

                                    printcsv.printCSV_fullProcessNoBlank(letterFname, lettertoXmpie, "", "Y");

                                    DataTable PlanTabletoXmpie = dbU.ExecuteDataTable("select * from PLAN_TABLE order by recnum");
                                    //DataTable PlanTabletoXmpie = dbU.ExecuteDataTable("select * from R_XMPie_eoc_Acct_PLAN_TABLE order by recnum");

                                    letterFname = ProcessVars.dataEOC + @"\Plan_Table.csv";
                                    if (File.Exists(letterFname))
                                        File.Delete(letterFname);

                                    printcsv.printCSV_fullProcessNoBlank(letterFname, PlanTabletoXmpie, "", "Y");
                                    //====================================
                                    DataTable letterNewtoXmpie = dbU.ExecuteDataTable("select * from R_XMPie_eoc_Acct_new order by recnum");

                                    letterFname = ProcessVars.dataEOC + @"\" + finfo.Name.Substring(0, finfo.Name.Length - 5) + "_NEW_Letter_toXMPie.csv";
                                    if (File.Exists(letterFname))
                                        File.Delete(letterFname);

                                    printcsv.printCSV_fullProcessNoBlank(letterFname, letterNewtoXmpie, "", "Y");

                                    //DataTable PlanTabletoXmpieNew = dbU.ExecuteDataTable("select * from R_XMPie_eoc_Acct_PLAN_TABLE_new order by recnum");


                                    //letterFname = ProcessVars.dataEOC + @"\New_Plan_Table.csv";
                                    //if (File.Exists(letterFname))
                                    //    File.Delete(letterFname);

                                    //printcsv.printCSV_fullProcessNoBlank(letterFname, PlanTabletoXmpieNew, "", "Y");

                                }
                                catch (Exception ex)
                                {
                                    errors = errors + ex.Message;
                                }

                            }


                        }
                        else
                        {
                            sendMails sendmail = new sendMails();
                            sendmail.SendMailError("BCBS_MA_Processing EOC Back from BCC", "ErrorinProcess", "\n\n" + "Error " + resultBCC, "");
                        }
                    }
                    else
                    {
                        sendMails sendmail = new sendMails();
                        sendmail.SendMailError("BCBS_MA_Processing EOC sending to BCC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                    }
                }
                else
                {
                    sendMails sendmail = new sendMails();
                    sendmail.SendMailError("BCBS_MA_Processing EOC No Records", "ErrorinProcess", "\n\n" + "No records in  " + finfo.FullName, "");
                }
            }
            catch (Exception ex)
            {
                sendMails sendmail = new sendMails();
                sendmail.SendMailError("BCBS_MA_Uploading EOC BCC error", "ErrorinProcess", "\n\n" + "Error " + ex.Message, "");
            }
            return result;
        }

        public string upload_EOCKit(string filename)
        {
            string result = "";
            FileInfo finfo = new FileInfo(filename);

            int errorcount = 0;
            if (!File.Exists(CodeCallService.ProcessVars.gmappingFile))
                throw new Exception("Mapping file not found.");

            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from BCBS_MA_parse_eoc_MKIT where filename = '" + finfo.Name + "'");
            dbU.ExecuteScalar("delete from BCBS_MA_parse_fromBCC where filename = '" + finfo.Name + "'");

            var recnum = dbU.ExecuteScalar("select max(recnum) from Master_SEQ");
            GRecnum = 0;
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            DataTable dt1 = new DataTable();



            List<Field> fields = GetFieldsKits();
            foreach (Field field in fields)
            {
                dt1.Columns.Add(field.Name);

            }
            dt1 = ParseFiletoTable(filename, dt1, true);
            string wbccName = ProcessVars.dataEOC + @"\data_to_Presort_" + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_toBCC.csv";
            if (dt1.Rows.Count > 0)
            {

                string errors = "";
                dbU.ExecuteScalar("delete from BCBS_MA_parse_eoc_MKIT_tmp");

                dt1.Columns.Remove("Cycle");

                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    bulkCopy.DestinationTableName = "[dbo].[BCBS_MA_parse_eoc_MKIT]";

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
                {
                    try
                    {
                        dbU.ExecuteScalar("Insert into BCBS_MA_parse_eoc_MKIT select * from BCBS_MA_parse_eoc_MKIT_tmp");
                        dbU.ExecuteScalar("update BCBS_MA_parse_eoc_MKIT set kitid = 'PPO-P1' where filename = '" + finfo.Name + "'");
                        // dbU.ExecuteScalar("Update Master_SEQ set recnum = " + (GRecnum - 1) + ", TableName = 'BCBS_MA_parse_eoc', filename ' '" + finfo.Name + "'");
                        var lastCycle = dbU.ExecuteScalar("select max( cycle) from BCBS_MA_parse_eoc_MKIT where cycle is not null");
                        string inputCycle = Microsoft.VisualBasic.Interaction.InputBox("Enter Cycle", "Last Cycle " + lastCycle.ToString(), lastCycle.ToString(), -1, -1);
                        var cycleExist = dbU.ExecuteScalar("select cycle from BCBS_MA_parse_eoc_MKIT where cycle = '" + inputCycle.ToString() + "'");
                        if (inputCycle != "" && cycleExist == null)
                            dbU.ExecuteScalar("update BCBS_MA_parse_eoc_MKIT set cycle = '" + inputCycle.ToString() + "' where filename = '" + finfo.Name + "'");
                        else
                            errors = "wrong cycle entered";



                        dbU.ExecuteScalar("Insert into Master_SEQ(recnum, Tablename, Description, DateTime) values(" + (GRecnum - 1) + ",'BCBS_MA_parse_eoc_MKIT','" + finfo.Name + "',GETDATE())");

                        CreateCSV printcsv = new CreateCSV();

                        if (File.Exists(wbccName))
                            File.Delete(wbccName);

                        DataTable dataToPresort = dbU.ExecuteDataTable("select * from BCBS_MA_parse_eoc_MKIT where filename = '" + finfo.Name + "'");
                        printcsv.printCSV_fullProcess(wbccName, dataToPresort, "", "");
                        //\\CIERANT-TAPER\Clients\Horizon BCBS\BCBS_MA

                        result = "process ok";
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;
                    }

                }


            }

            return result;
        }
        public string upload_EOC(string filename)
        {
            string errors = "";
            FileInfo finfo = new FileInfo(filename);
            try
            {

                int errorcount = 0;
                if (!File.Exists(CodeCallService.ProcessVars.gmappingFile))
                    throw new Exception("Mapping file not found.");

                CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
                dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
                CreateCSV printcsv = new CreateCSV();
                dbU.ExecuteScalar("delete from BCBS_MA_parse_eoc where filename = '" + finfo.Name + "'");
                dbU.ExecuteScalar("delete from BCBS_MA_parse_fromBCC where filename = '" + finfo.Name + "'");
                var recnum = dbU.ExecuteScalar("select max(recnum) from Master_SEQ");
                GRecnum = 0;
                int recordnumber = 0;
                if (recnum.ToString() == "")
                    GRecnum = 1;
                else
                    GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

                DataTable dt1 = new DataTable();



                List<Field> fields = GetFields();
                foreach (Field field in fields)
                {
                    dt1.Columns.Add(field.Name);

                }
                dt1 = ParseFiletoTable(filename, dt1, false);


                //string testname = finfo.Directory + "\\test_raw_data.csv";
                //printcsv.printCSV_fullProcess(testname, dt1, "", "");


                if (dt1.Rows.Count > 0)
                {
                    DataTable toBCC = new System.Data.DataTable();
                    toBCC.Columns.Add("Recnum");
                    toBCC.Columns.Add("FullName");
                    toBCC.Columns.Add("Addr1");
                    toBCC.Columns.Add("Addr2");
                    toBCC.Columns.Add("Addr5");
                    foreach (DataRow row in dt1.Rows)
                    {
                        var rowBCC = toBCC.NewRow();
                        rowBCC["Recnum"] = row["Recnum"].ToString();
                        rowBCC["FullName"] = (row["fname"].ToString() + ' ' + row["mname"].ToString() + ' ' + row["lname"].ToString()).Trim();
                        rowBCC["Addr1"] = row["Addr1"].ToString();
                        rowBCC["Addr2"] = row["Addr2"].ToString();
                        rowBCC["Addr5"] = (row["city"].ToString() + ' ' + row["state"].ToString() + ' ' + row["zip10"].ToString()).Trim();
                        toBCC.Rows.Add(rowBCC);
                    }
                    for (int i = 0; i < 13; i++)
                    {
                        toBCC.Columns.Add("F" + i, typeof(string)).SetOrdinal(1);
                    }

                    toBCC.Columns.Add("Add4", typeof(string)).SetOrdinal(17);
                    toBCC.Columns.Add("Add3", typeof(string)).SetOrdinal(17);
                    // toBCC.Columns.Add("Cycle", typeof(string)).SetOrdinal(2);


                    string wbccName = ProcessVars.dataEOC + @"\" + "MAS021_" + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_toBCC.csv";
                    string bccName = ProcessVars.dmpsWatched + "MAS021_" + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_toBCC.csv";
                    string bccready = ProcessVars.oDMPsDirectoryM + "MAS023_MAS021_" + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_toBCC_PROCESSED.csv";

                    if (File.Exists(bccready))
                        File.Delete(bccready);

                    //HORIZ_CON2_20170215_NSR_NASCO_HIX_76119_PROCESSED_toBCC.csv
                   

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
                        if (numberTry > 300)
                        {
                            getBCCready = "not found file after 200 attempts : " + bccready;
                            sendMails sendmail = new sendMails();
                            sendmail.SendMailError("BCBS_MA_Processing EOC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                            errors = errors + "Error " + getBCCready;
                            break;
                        }
                    }
                    string resultBCC = "";
                    if (getBCCready == "")
                    {
                        resultBCC = processReturnBCC_and_upd_Sql(dt1, infoBCCreadfy, finfo.Name);

                        if (resultBCC == "")
                        {

                            dbU.ExecuteScalar("delete from BCBS_MA_parse_eoc_tmp");



                            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                            Connection.Open();

                            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                            {
                                bulkCopy.DestinationTableName = "[dbo].[BCBS_MA_parse_eoc_tmp]";

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
                            {
                                try
                                {
                                    dbU.ExecuteScalar("Insert into BCBS_MA_parse_eoc select * from BCBS_MA_parse_eoc_tmp");
                                    try
                                    {
                                        var lastCycle = dbU.ExecuteScalar("select max( cycle) from BCBS_MA_parse_eoc where cycle is not null");
                                        string inputCycle = Microsoft.VisualBasic.Interaction.InputBox("Enter Cycle", "Last Cycle " + lastCycle.ToString(), lastCycle.ToString(), -1, -1);
                                        var cycleExist = dbU.ExecuteScalar("select cycle from BCBS_MA_parse_eoc where cycle = '" + inputCycle.ToString() + "'");
                                        if (inputCycle != "" && cycleExist == null)
                                            dbU.ExecuteScalar("update BCBS_MA_parse_eoc set cycle = '" + inputCycle.ToString() + "' where filename = '" + finfo.Name + "'");
                                        else
                                            errors = "wrong cycle entered";
                                    }
                                    catch (Exception exx)
                                    {
                                        errors = errors + exx.Message;
                                    }
                                    SqlParameter[] sqlParams;
                                    try
                                    {
                                        sqlParams = null;
                                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };

                                        // clean fileds 
                                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1", sqlParams);
                                    }
                                    catch (Exception exx)
                                    {
                                        errors = errors + exx.Message;
                                    }
                                    try
                                    {
                                        sqlParams = null;
                                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };

                                        // Updates with ~ 
                                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1_MED", sqlParams);
                                    }
                                    catch (Exception exx)
                                    {
                                        errors = errors + exx.Message;
                                    }

                                    DataTable ProdFamiy = dbU.ExecuteDataTable("select distinct E.Prodfamily, M.[Kit_Type Value], M.Expression from BCBS_MA_parse_eoc E join Master_LetterType M on E.Prodfamily = M.Prodfamily where FileName ='" + finfo.Name + "'");
                                    if (ProdFamiy.Rows.Count > 0)
                                    {
                                        foreach (DataRow rowf in ProdFamiy.Rows)
                                        {
                                            try
                                            {
                                                sqlParams = null;
                                                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name), new SqlParameter("@Kit", rowf[1].ToString()),
                                                                          new SqlParameter("@expression", rowf[2].ToString())};
                                                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_2", sqlParams);
                                            }
                                            catch (Exception exx)
                                            {
                                                errors = errors + exx.Message;
                                            }

                                        }
                                        sqlParams = null;
                                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };
                                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_3", sqlParams);



                                    }

                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S1', KitID = '' " +
                                                "where filename = '" + finfo.Name + "' and KitID like 'den%' and PREMAMT1 = '0000000000' and filenamesMED like '%SHPSob%'");

                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S2', KitID = '' " +
                                                "where filename = '" + finfo.Name + "' and KitID like 'den%' and PREMAMT1 = '0000000000'");

                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S4', KitID = '' " +
                                                "where filename = '" + finfo.Name + "' and ([group] like '%4063785%' or [group] like '%002360571%' or [group] like '%2360569%' or [group] like '%4065110%' or [group] like '%4065111%')");

                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S4', KitID = '' " +
                                                                                   "where filename = '" + finfo.Name + "' and ([group] like '%2353891%' and EFF_DATE = '20160826')");
                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S4', KitID = '' " +
                                                                                  "where filename = '" + finfo.Name + "' and ([group] like '%2360572%' or [group] like '%002360573%')");
                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S5', KitID = '' " +
                                                                                 "where filename = '" + finfo.Name + "' and ([group] like '%2360575%' or [group] like '%2360574%' or [group] like '%4065112%')");
                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S6', KitID = '' " +
                                                                                "where filename = '" + finfo.Name + "' and (" +
                                                                                "[group] like '%2360577%' or [group] like '%2360576%')");
                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S7', KitID = '' " +
                                                                               "where filename = '" + finfo.Name + "' and (" +
                                                                               "[group] like '%2348079%' or [group] like '%2348081%' or" +
                                                                                "[group] like '%2348083%' or [group] like '%2348084%' or" +
                                                                                "[group] like '%2348184%' or [group] like '%2339755%' or" +
                                                                                 "[group] like '%2348078%' or [group] like '%2329294%'  or [group] like '%4065113%' or" +
                                                                                  "[group] like '%2359829%' or [group] like '%2359897%')");

                                    //    sqlParams = null;  4065113
                                    //sqlParams = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };


                                    //    dbU.ExecuteScalar("BCBS_MA_upd_EOC", sqlParams);
                                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set KitId = 'DEN-PM' where FileName = '" + finfo.Name + "' and KitID = 'DEN-P1, DEN-PM'");
                                    string strsql = "select recnum, COV_CODE, UACOVCODE1, ProdFamily, KitID from BCBS_MA_parse_eoc where FileName = '" + finfo.Name + "' and KitID LIKE '%,%'";
                                    DataTable errorsTable = dbU.ExecuteDataTable(strsql);
                                    if (errorsTable.Rows.Count > 0)
                                    {
                                        string errorsUpdating = "Filename : " + finfo.FullName + Environment.NewLine;
                                        errorsUpdating = errorsUpdating + "Recnum\tCOV_CODE\tUACOVCODE1\tProdFamily\tKitID" + Environment.NewLine;
                                        foreach (DataRow row in errorsTable.Rows)
                                        {
                                            errorsUpdating = errorsUpdating + row[0].ToString() + "\t" + row[1].ToString() + "\t" + row[2].ToString() + "\t" + row[3].ToString() + "\t" + row[4].ToString() + Environment.NewLine;
                                        }
                                        sendMails sendmail = new sendMails();
                                        sendmail.SendMailError("BCBS_MA_Processing Updating EOC", "ErrorinProcess", "\n\n" + "Error " + errorsUpdating, "");
                                    }
                                    else
                                    {
                                        try
                                        {
                                            string strsql2 = "select distinct typeassembly, ProdFamily from BCBS_MA_parse_eoc where FileName = '" + finfo.Name + "'  and len(kitid) > 0";
                                            DataTable versions = dbU.ExecuteDataTable(strsql2);
                                            if (versions.Rows.Count > 0)
                                            {
                                                foreach (DataRow rowV in versions.Rows)
                                                {
                                                    string Assembly = rowV[0].ToString();
                                                    string version = rowV[1].ToString();
                                                    SqlParameter[] sqlParams2;
                                                    sqlParams2 = null;
                                                    sqlParams2 = new SqlParameter[] { new SqlParameter("@Assembly", Assembly),
                                                                        new SqlParameter("@FileName", finfo.Name), new SqlParameter("@Version", version) };

                                                    dbU.ExecuteScalar("BCBS_MA_rpt_EOC_to_XMPie", sqlParams2);
                                                    string strsql3 = "select * from R_XMPie_eoc_" + Assembly + "_" + version + " order by recnum";
                                                    DataTable resultXmpie = dbU.ExecuteDataTable(strsql3);
                                                    CreateCSV printcsv2 = new CreateCSV();
                                                    string printfile = ProcessVars.dataEOC + finfo.Name.Substring(0, finfo.Name.Length - 4) + "_" + Assembly + "_" + version + "_toXMPie.csv";

                                                    if (File.Exists(printfile))
                                                        File.Delete(printfile);

                                                    printcsv.printCSV_fullProcess(printfile, resultXmpie, "", "Y");
                                                }
                                            }
                                        }
                                        catch (Exception exx)
                                        {
                                            errors = errors + exx.Message;
                                        }
                                    }
                                    // dbU.ExecuteScalar("Update Master_SEQ set recnum = " + (GRecnum - 1) + ", TableName = 'BCBS_MA_parse_eoc', filename ' '" + finfo.Name + "'");
                                    dbU.ExecuteScalar("Insert into Master_SEQ(recnum, Tablename, Description, DateTime) values(" + (GRecnum - 1) + ",'BCBS_MA_parse_eoc','" + finfo.Name + "',GETDATE())");

                                    //summary

                                    //                                    select typeassembly, KitID, ProdFamily, Count(*) as Count from BCBS_MA_parse_eoc where FileName = 'EOC_file_20170415.txt' group by typeassembly,KitID, ProdFamily

                                    //select UACOVCODE1, typeassembly,KitID, ProdFamily, count(*) as count from BCBS_MA_parse_eoc where FileName = 'EOC_file_20170415.txt' and typeassembly = '' group by UACOVCODE1,typeassembly,KitID, ProdFamily


                                    printSummary(finfo.Name);

                                }
                                catch (Exception ex)
                                {
                                    errors = errors + ex.Message;
                                }

                            }


                        }
                        else
                        {
                            sendMails sendmail = new sendMails();
                            sendmail.SendMailError("BCBS_MA_Processing EOC Back from BCC", "ErrorinProcess", "\n\n" + "Error " + resultBCC, "");
                            errors = errors + "Error " + resultBCC;
                        }
                    }
                    else
                    {
                        sendMails sendmail = new sendMails();
                        sendmail.SendMailError("BCBS_MA_Processing EOC sending to BCC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
                        errors = errors + "Error " + getBCCready;
                    }
                }
                else
                {
                    sendMails sendmail = new sendMails();
                    sendmail.SendMailError("BCBS_MA_Processing EOC No Records", "ErrorinProcess", "\n\n" + "No records in  " + finfo.FullName, "");
                    errors = errors + "Error " + "No records in  " + finfo.FullName;
                }
            }
            catch (Exception ex)
            {
                sendMails sendmail = new sendMails();
                sendmail.SendMailError("BCBS_MA_Uploading EOC BCC error", "ErrorinProcess", "\n\n" + "Error " + ex.Message, "");
                errors = errors + "Error " + "Error " + ex.Message;
            }

            if (errors == "")
                errors = "Process ok";

            return errors;
        }
        public void printSummary(string fName)
        {
            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            CreateCSV printcsv = new CreateCSV();
            string StoredProc = "BCBS_MA_upd_EOC_Summary";
            string xName = ProcessVars.dataEOC + @"Summary EOC_" + fName.Substring(0, fName.Length - 4) + ".xls";
            SqlDataReader rdr = null;
            DataTable Summary_File = new DataTable();
            DataTable DetailErrors = new DataTable();
            DataTable Supress = new DataTable();

            if (File.Exists(xName))
                File.Delete(xName);
            using (SqlConnection Connection2 = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(StoredProc, Connection2))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add("@FileName", SqlDbType.VarChar).Value = fName;
                    int recType = 1;
                    Connection2.Open();

                    rdr = cmd.ExecuteReader();

                    if (rdr.HasRows)
                    {

                        Summary_File.Load(rdr);
                        DetailErrors.Load(rdr);

                    }

                    else
                    {
                        Console.WriteLine("No rows found.");
                    }
                    rdr.Close();
                    Export_XLSX createxls = new Export_XLSX();
                    createxls.CreateExcelFileTables(Summary_File, "Summary Files", xName
                                    , DetailErrors, "Summary Errors", Supress, ""

                                    );


                }
            }
            string xNameSupression = ProcessVars.dataEOC + @"Supressed EOC_" + fName.Substring(0, fName.Length - 4) + ".xls";
            if (File.Exists(xNameSupression))
                File.Delete(xNameSupression);
            SqlParameter[] sqlParams3;
            sqlParams3 = null;
            sqlParams3 = new SqlParameter[] { new SqlParameter("@FileName", fName) };

            DataTable supressed = dbU.ExecuteDataTable("BCBS_MA_upd_EOC_Supressed", sqlParams3);
            printcsv.printCSV_fullProcess(xNameSupression, supressed, "", "");
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
                    sendMails sendmail = new sendMails();
                    sendmail.SendMailError("BCBS_MA_Processing EOC", "ErrorinProcess", "\n\n" + "Error " + getBCCready, "");
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
                                    row["UpdCity"] = dRNew["ST_CITY"];
                                    row["UpdState"] = dRNew["ST_STATE_PROV"];
                                    row["UpdZip"] = dRNew["ST_POSTALCODE"];
                                    row["UpdCounty"] = "";
                                    row["IMBChar"] = dRNew["Intelligent Mail barcode"];
                                    row["IMBDig"] = dRNew["IMPB DIGITS FOR XMPIE"];
                                    row["DL"] = "";
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                        }

                        string errors = "";
                        dbU.ExecuteScalar("delete from BCBS_MA_parse_fromBCC_tmp");



                        SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                        Connection.Open();

                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                        {
                            bulkCopy.DestinationTableName = "[dbo].[BCBS_MA_parse_fromBCC_tmp]";

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
                            dbU.ExecuteScalar("Insert into BCBS_MA_parse_fromBCC select * from BCBS_MA_parse_fromBCC_tmp");
                    }
                    //if (NonD_Records.Rows.Count > 0)
                    //{
                    //    try
                    //    {
                    //        NonD_Records.PrimaryKey = new DataColumn[] { NonD_Records.Columns["Recnum"] };
                    //        foreach (DataRow dRNew in NonD_Records.Rows)
                    //        {
                    //            DataRow row = null;
                    //            try
                    //            {
                    //                row = dt1.Rows.Find(dRNew["Recnum"].ToString());
                    //            }
                    //            catch (MissingPrimaryKeyException)
                    //            {
                    //                row = dt1.Select("Recnum='" + dRNew["Recnum"] + "'").First();
                    //            }
                    //            if (row != null)
                    //            {

                    //                row["ReturnCode"] = dRNew["ReturnCode"];
                    //                row["DL"] = "N";
                    //            }
                    //        }
                    //    }
                    //    catch (Exception ex)
                    //    {
                    //        result = result + bccfile.Name + " " + ex.Message + Environment.NewLine;
                    //    }
                    //}
                }
                else
                {
                    result = result + bccfile.Name + " OUTPUT.csv not found..." + Environment.NewLine;
                }
            }


            return result;
        }

        private DataTable ParseFiletoTable(string inputFile, DataTable table, bool isMKIT)
        {

            FileInfo finfo = new FileInfo(inputFile);
            int interactions = 0;
            string processDate = ""; string TotalRecs = "";
            int toterrors = 0;
            List<Field> fields = null;
            if (isMKIT)
                fields = GetFieldsKits();
            else
                fields = GetFields();
            if (finfo.Extension == ".xls")
            {
                table = loadXLSX(inputFile);
            }
            else
            {

                ArrayList aList;

                int linenumber = 0;
                string error = "";
                using (StreamReader reader = new StreamReader(inputFile))
                {
                    string line = reader.ReadLine();
                    try
                    {
                        while (line != null)
                        {
                            linenumber++;
                            if (line.Length > 1)
                            {
                                if (line.Substring(0, 5) == "TOTAL" || line.Substring(0, 4) == "DATE")
                                {
                                    if (line.Substring(0, 5) == "TOTAL")
                                        TotalRecs = line.Trim();
                                    if (line.Substring(0, 4) == "DATE")
                                        processDate = line.Trim();
                                }
                                else
                                {
                                    interactions = 0;
                                    aList = new ArrayList();
                                    foreach (Field field in fields)
                                    {

                                        aList.Add(line.Substring(field.Start, field.Length).Trim());
                                        //error = error + field.Name + "  " + field.Start + " " + field.Length + Environment.NewLine;
                                    }
                                    if (table.Columns.Count == aList.Count)
                                        table.Rows.Add(aList.ToArray());
                                    else
                                        toterrors++;
                                }

                                line = reader.ReadLine();
                            }
                            else
                                line = reader.ReadLine();
                        }

                    }
                    catch (Exception ex)
                    {
                        var msg = ex.Message;
                    }
                }
            }
            table.Columns.Add("ImportDate", typeof(string)).SetOrdinal(0);
            table.Columns.Add("FileName", typeof(string)).SetOrdinal(0); ;
            if (finfo.Extension == ".xls")
            {
                DataColumnCollection columns = table.Columns;
                if (columns.Contains("zip"))
                {
                    table.Columns["zip"].ColumnName = "zip10";
                }
            }

            table.Columns.Add("Recnum", typeof(string)).SetOrdinal(0);
            if (!isMKIT || finfo.Extension == ".xls")
            {
                table.Columns.Add("ProdFamily", typeof(string));
                table.Columns.Add("KitID", typeof(string));
                table.Columns.Add("LetterCode", typeof(string));
                table.Columns.Add("Count_OLB_CommercialEOC", typeof(int));
                table.Columns.Add("Count_OLBC_Collaterals", typeof(int));
                table.Columns.Add("CountSIN", typeof(int));
                table.Columns.Add("CovCodeMED", typeof(string));
                table.Columns.Add("FilenamesMED", typeof(string));
                table.Columns.Add("DocType_CommercialEOC", typeof(string));
                table.Columns.Add("Med_GRP", typeof(string));
                table.Columns.Add("Med_10D", typeof(string));
                table.Columns.Add("Med_HAG", typeof(string));
                table.Columns.Add("Med_MCC", typeof(string));
                table.Columns.Add("UpdAddr1", typeof(string));
                table.Columns.Add("UpdAddr2", typeof(string));
                table.Columns.Add("UpdAddr3", typeof(string));
                table.Columns.Add("UpdAddr4", typeof(string));
                table.Columns.Add("UpdAddr5", typeof(string));
                table.Columns.Add("UpdCity", typeof(string));
                table.Columns.Add("UpdState", typeof(string));
                table.Columns.Add("UpdZip", typeof(string));
                table.Columns.Add("UpdCounty", typeof(string));
                table.Columns.Add("IMBChar", typeof(string));
                table.Columns.Add("IMBDig", typeof(string));
                table.Columns.Add("DL", typeof(string));
                table.Columns.Add("ReturnCode", typeof(string));
                table.Columns.Add("Ltr_Plan", typeof(string));
            }
            foreach (DataRow dr in table.Rows)
            {
                dr["FileName"] = finfo.Name;
                dr["ImportDate"] = DateTime.Now.ToString("yyyy-MM-dd");
                dr["Recnum"] = GRecnum;
                GRecnum++;
            }
            table.PrimaryKey = new DataColumn[] { table.Columns["Recnum"] };
            table.Columns.Add("Cycle", typeof(string)).SetOrdinal(1);
            //Return all of our records.
            return table;
        }
        private List<Field> GetFields()
        {
            List<Field> fields = new List<Field>();
            XmlDocument map = new XmlDocument();

            //Load the mapping file into the XmlDocument
            map.Load(CodeCallService.ProcessVars.gmappingFile);

            //Load the field nodes.
            XmlNodeList fieldNodes = map.SelectNodes("/File/FileMap/Field");

            //Loop through the nodes and create a field object
            // for each one.
            foreach (XmlNode fieldNode in fieldNodes)
            {
                Field field = new Field();

                //Set the field's name
                field.Name = fieldNode.Attributes["Name"].Value;

                //Set the field's length
                field.Length =
                        Convert.ToInt32(fieldNode.Attributes["Length"].Value);

                //Set the field's starting position
                field.Start =
                        Convert.ToInt32(fieldNode.Attributes["Start"].Value) - 1;

                //Add the field to the Field list.
                fields.Add(field);
            }

            return fields;
        }
        private List<Field> GetFieldsKits()
        {
            List<Field> fields = new List<Field>();
            XmlDocument map = new XmlDocument();

            //Load the mapping file into the XmlDocument
            map.Load(CodeCallService.ProcessVars.gmappingFile);

            //Load the field nodes.
            XmlNodeList fieldNodes = map.SelectNodes("/File/FileMapKIT/Field");

            //Loop through the nodes and create a field object
            // for each one.
            foreach (XmlNode fieldNode in fieldNodes)
            {
                Field field = new Field();

                //Set the field's name
                field.Name = fieldNode.Attributes["Name"].Value;

                //Set the field's length
                field.Length =
                        Convert.ToInt32(fieldNode.Attributes["Length"].Value);

                //Set the field's starting position
                field.Start =
                        Convert.ToInt32(fieldNode.Attributes["Start"].Value) - 1;

                //Add the field to the Field list.
                fields.Add(field);
            }

            return fields;
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

        public string Reprocess_EOC(string filename)
        {
            string errors = "";
            //FileInfo finfo = new FileInfo(filename);
            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";

            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);
            var recnum = dbU.ExecuteScalar("select count(*)  from BCBS_MA_parse_eoc where filename = '" + filename + "'");

            GRecnum = 0;
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;
            if (GRecnum != 1)
            {

                try
                {


                    CreateCSV printcsv = new CreateCSV();



                    SqlParameter[] sqlParams;
                    try
                    {
                        sqlParams = null;
                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };

                        // clean fileds 
                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1", sqlParams);
                    }
                    catch (Exception exx)
                    {
                        errors = errors + exx.Message;
                    }
                    try
                    {
                        sqlParams = null;
                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };

                        // Updates with ~ 
                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_1_MED", sqlParams);
                    }
                    catch (Exception exx)
                    {
                        errors = errors + exx.Message;
                    }

                    DataTable ProdFamiy = dbU.ExecuteDataTable("select distinct E.Prodfamily, M.[Kit_Type Value], M.Expression from BCBS_MA_parse_eoc E join Master_LetterType M on E.Prodfamily = M.Prodfamily where FileName ='" + filename + "'");
                    if (ProdFamiy.Rows.Count > 0)
                    {
                        foreach (DataRow rowf in ProdFamiy.Rows)
                        {
                            try
                            {
                                sqlParams = null;
                                sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename), new SqlParameter("@Kit", rowf[1].ToString()),
                                                                          new SqlParameter("@expression", rowf[2].ToString())};
                                dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_2", sqlParams);
                            }
                            catch (Exception exx)
                            {
                                errors = errors + exx.Message;
                            }

                        }
                        sqlParams = null;
                        sqlParams = new SqlParameter[] { new SqlParameter("@FileName", filename) };
                        dbU.ExecuteScalar("BCBS_MA_upd_EOC_Step_3", sqlParams);



                    }

                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S1', KitID = '' " +
                                "where filename = '" + filename + "' and KitID like 'den%' and PREMAMT1 = '0000000000' and filenamesMED like '%SHPSob%'");

                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S2', KitID = '' " +
                                "where filename = '" + filename + "' and KitID like 'den%' and PREMAMT1 = '0000000000'");

                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S4', KitID = '' " +
                                "where filename = '" + filename + "' and ([group] like '%4063785%' or [group] like '%002360571%' or [group] like '%2360569%' or [group] like '%4065110%' or [group] like '%4065111%')");

                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S4', KitID = '' " +
                                                                   "where filename = '" + filename + "' and ([group] like '%2353891%' and EFF_DATE = '20160826')");
                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set SpecialSupress = 'S4', KitID = '' " +
                                                                  "where filename = '" + filename + "' and ([group] like '%2360572%')");

                    //    sqlParams = null;
                    //sqlParams = new SqlParameter[] { new SqlParameter("@FileName", finfo.Name) };


                    //    dbU.ExecuteScalar("BCBS_MA_upd_EOC", sqlParams);
                    dbU.ExecuteNonQuery("update BCBS_MA_parse_eoc set KitId = 'DEN-PM' where FileName = '" + filename + "' and KitID = 'DEN-P1, DEN-PM'");
                    string strsql = "select recnum, COV_CODE, UACOVCODE1, ProdFamily, KitID from BCBS_MA_parse_eoc where FileName = '" + filename + "' and KitID LIKE '%,%'";
                    DataTable errorsTable = dbU.ExecuteDataTable(strsql);
                    if (errorsTable.Rows.Count > 0)
                    {
                        string errorsUpdating = "Filename : " + filename + Environment.NewLine;
                        errorsUpdating = errorsUpdating + "Recnum\tCOV_CODE\tUACOVCODE1\tProdFamily\tKitID" + Environment.NewLine;
                        foreach (DataRow row in errorsTable.Rows)
                        {
                            errorsUpdating = errorsUpdating + row[0].ToString() + "\t" + row[1].ToString() + "\t" + row[2].ToString() + "\t" + row[3].ToString() + "\t" + row[4].ToString() + Environment.NewLine;
                        }
                        sendMails sendmail = new sendMails();
                        sendmail.SendMailError("BCBS_MA_Processing Updating EOC", "ErrorinProcess", "\n\n" + "Error " + errorsUpdating, "");
                    }
                    else
                    {
                        try
                        {
                            string strsql2 = "select distinct typeassembly, ProdFamily from BCBS_MA_parse_eoc where FileName = '" + filename + "'  and len(kitid) > 0";
                            DataTable versions = dbU.ExecuteDataTable(strsql2);
                            if (versions.Rows.Count > 0)
                            {
                                foreach (DataRow rowV in versions.Rows)
                                {
                                    string Assembly = rowV[0].ToString();
                                    string version = rowV[1].ToString();
                                    SqlParameter[] sqlParams2;
                                    sqlParams2 = null;
                                    sqlParams2 = new SqlParameter[] { new SqlParameter("@Assembly", Assembly),
                                                                        new SqlParameter("@FileName", filename), new SqlParameter("@Version", version) };

                                    dbU.ExecuteScalar("BCBS_MA_rpt_EOC_to_XMPie", sqlParams2);
                                    string strsql3 = "select * from R_XMPie_eoc_" + Assembly + "_" + version + " order by recnum";
                                    DataTable resultXmpie = dbU.ExecuteDataTable(strsql3);
                                    CreateCSV printcsv2 = new CreateCSV();
                                    string printfile = ProcessVars.dataEOC + filename.Substring(0, filename.Length - 4) + "_" + Assembly + "_" + version + "_toXMPie.csv";

                                    if (File.Exists(printfile))
                                        File.Delete(printfile);

                                    printcsv.printCSV_fullProcess(printfile, resultXmpie, "", "Y");
                                }
                            }
                        }
                        catch (Exception exx)
                        {
                            errors = errors + exx.Message;
                        }
                    }


                    printSummary(filename);

                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;
                }

            }
            if (errors == "")
                errors = "Process ok";

            return errors;
        }
    }
}
