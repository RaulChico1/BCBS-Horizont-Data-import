using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class NParse_ICH_SG_SBC_Annual
    {
        DBUtility dbU;
        public string sendtoBCC(string dateReport, string tablename, string selection)
        {
            //string location = @"\\freenas\Internal_Production\Horizon_NJH_Production_Mngmt\SECURE\PROD_INBOUND\" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + @"\CHAMPS";
            string location = ProcessVars.InputDirectory + @"From_FTP\";
            Directory.CreateDirectory(location);
            DBUtility dbU;

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable dataToBCC = dbU.ExecuteDataTable("select distinct filename  from " + tablename + " where importdate = '" + dateReport + "' and  bccProcessed is null");
            string pNameT = ""; string BCCname = "";
            foreach (DataRow rowf in dataToBCC.Rows)
            {
                BCCname = "\\HNJH-PR_" + rowf[0].ToString().Replace(".xlsx", "").Replace(".xls", "").Replace(".csv", "") + "_toBCC.csv";
                pNameT = ProcessVars.InputDirectory + @"From_FTP" + "\\HNJH-PR_" + rowf[0].ToString().Replace(".xlsx", "").Replace(".xls", "").Replace(".csv", "") + "_toBCC.csv";
                if (File.Exists(pNameT))
                    File.Delete(pNameT);

                var fieldnames = new List<string>();
                fieldnames.Add("Recnum");
                fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
                fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
                fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");

                createCSV createcsvT = new createCSV();
                bool resp = createcsvT.addRecordsCSV(pNameT, fieldnames);

                DataTable table_BCC = dbU.ExecuteDataTable(selection + rowf[0].ToString() + "'");
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
                            rowData.Add(""); rowData.Add(""); rowData.Add(row[6].ToString());
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
                    await Task.Delay(1000 * 60 * 2);
                });
                tR.Wait();
               // ProcessBackData(cassFileName);
                HNJH_Champs backdata = new HNJH_Champs();
                //select FileName, TableName, DirectoryTo
               // 1st parameter cassname
                backdata.HNJH_Champion_noCass(BCCname, rowf[0].ToString(), tablename, ProcessVars.InputDirectory + @"From_FTP\");
            }
           
           
           
            return "";
        }
        public void ProcessBackData(string filename)
        {

        }
        public void print_csvs(string localPath, string dateReport, string spName, string datatable)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable filesToProc = dbU.ExecuteDataTable("select distinct filename from " + datatable +" where importdate = '" + dateReport + "'");
            SqlParameter[] sqlParams2;

            //  create report DL = N


            if (filesToProc.Rows.Count > 0)
            {
                foreach (DataRow row in filesToProc.Rows)
                {
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@fname", row[0].ToString()) };
                    DataTable dataReport = dbU.ExecuteDataTable(spName, sqlParams2);
                    createCSV createFilecsv = new createCSV();
                    string fname = row[0].ToString().Substring(0, row[0].ToString().IndexOf(".") ) + ".csv";
                    string pname = localPath + fname;
                    if (File.Exists(pname))
                        File.Delete(pname);
                    createFilecsv.printCSV_fullProcess(pname, dataReport, "", "N");
                }

                foreach (DataRow row in filesToProc.Rows)
                {
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@fname", row[0].ToString()) };
                    DataTable dataReport = dbU.ExecuteDataTable(spName, sqlParams2);
                    createCSV createFilecsv = new createCSV();
                    string fname = row[0].ToString().Substring(0, row[0].ToString().IndexOf(".") - 1) + ".csv";
                    string pname = localPath + "Samples_" + fname;
                    if (File.Exists(pname))
                        File.Delete(pname);
                    createFilecsv.printCSV_fullProcess(pname, dataReport, "", "N");
                }

            }
        }

        public void print_csvsFilename(string filename, string dateP)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable filesToProc = dbU.ExecuteDataTable("select distinct filename from HOR_parse_ICH_SG_SBC_Annual where filename = '" + filename + "'");
            SqlParameter[] sqlParams2;

            //  create report DL = N


            if (filesToProc.Rows.Count > 0)
            {
                foreach (DataRow row in filesToProc.Rows)
                {
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@fname", row[0].ToString()) };
                    DataTable dataReport = dbU.ExecuteDataTable("HOR_rpt_ICH_SG_SBC_Annual_SCI", sqlParams2);
                    createCSV createFilecsv = new createCSV();
                    string fname = "";
                    if (row[0].ToString().IndexOf(".") == -1)
                        fname = row[0].ToString() + ".csv";
                    else
                        fname = row[0].ToString().Substring(0, row[0].ToString().IndexOf(".") - 1) + ".csv";
                    string pname = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + dateP + @"\fromCass\" + fname;
                    if (File.Exists(pname))
                        File.Delete(pname);
                    createFilecsv.printCSV_fullProcess(pname, dataReport, "", "N");
                }

              

            }
        }
       
        public void print_A_csvsFilename(string strsql, string filename)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable dataReport = dbU.ExecuteDataTable(strsql);
                    createCSV createFilecsv = new createCSV();

                    string pname = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\fromCass\" + filename;
                    if (File.Exists(pname))
                        File.Delete(pname);
                    createFilecsv.printCSV_fullProcess(pname, dataReport, "", "N");
               
        }
    }
}
