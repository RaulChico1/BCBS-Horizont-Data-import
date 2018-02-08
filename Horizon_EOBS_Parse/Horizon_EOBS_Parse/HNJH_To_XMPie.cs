using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Configuration;

namespace Horizon_EOBS_Parse
{
    public class HNJH_To_XMPie
    {
        DBUtility dbU;
        public void SplitXMPiePdf(string FFName, string PrintNumber)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteNonQuery("update HOR_parse_HNJH_Panel_Roster_Provider set Xmpie_File = null where filename = '" + FFName + "'");
            //update XMPI file name splitted by 2k records
            string strsql = "select recnum from HOR_parse_HNJH_Panel_Roster_Provider where DL = 'y' and (Xmpie_File is null or Xmpie_File = '')";
            DataTable not_seq_No =  dbU.ExecuteDataTable(strsql);
            if (not_seq_No.Rows.Count > 0)
            {
                SqlParameter[] sqlParamsSplitXmpie;
                sqlParamsSplitXmpie = null;
                sqlParamsSplitXmpie = new SqlParameter[] { new SqlParameter("@fName", FFName), 
                new SqlParameter("@PrintNumber", PrintNumber) };

                dbU.ExecuteScalar("HOR_upd_HNJH_Roster_Split_Pdfs", sqlParamsSplitXmpie);
            }

        }
        public string SentTo_XMpieTables(string PagNoXmpie, string fileName)
        {
            string localdir = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Roster\";
            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            dbU.ExecuteScalar("delete Provider_Panel");
            dbU.ExecuteScalar("delete HNJH_RosterProvider_Xmpie");



            dbU.ExecuteScalar("insert into [Provider_Panel] select Pa.* from HOR_parse_HNJH_Panel_Roster_Panel Pa " +
                                "inner join HOR_parse_HNJH_Panel_Roster_Provider Pv " +
                                "on pv.Recnum = Pa.Recnum " +
                                "where Pv.PagNoXmpie = '" + PagNoXmpie + 
                                "' and Pv.filename = '" + fileName + "' order by  PA.ProvGroupID, ProviderName, MemberName");   //and pv.CountofMembersonPanel > 20 and pv.CountofMembersonPanel < 35

            dbU.ExecuteNonQuery("update Provider_Panel set TPLD = '' where TPLD = 'No'");
            dbU.ExecuteNonQuery("update Provider_Panel set NewMember = 'Yes' where NewMember = 'Y'");
            dbU.ExecuteNonQuery("update Provider_Panel set NewMember = '' where NewMember = 'N'");

            DataTable ProviderPanel = dbU.ExecuteDataTable("select * from Provider_Panel order by ProvGroupID, ProviderName,MemberName");
            createCSV createcsv = new createCSV();
            string Networkfname = ProcessVars.SourceDataRoster + "\\PANEL_" + PagNoXmpie + ".csv";
            string localname = localdir + "PANEL_" + PagNoXmpie + ".csv";
            bool resultCSV = createcsv.printCSV_fullProcess(localname, ProviderPanel, "", "Y");
            if (File.Exists(Networkfname))
                File.Delete(Networkfname);
            File.Copy(localname, Networkfname);

            SqlParameter[] sqlParamsXmpie;
            sqlParamsXmpie = null;
            sqlParamsXmpie = new SqlParameter[] { new SqlParameter("@PagNoXmpie", PagNoXmpie),  
                                                  new SqlParameter("@FileName", fileName)};
            dbU.ExecuteScalar("HOR_upd_Provider_Xmpie", sqlParamsXmpie);

            DataTable Provider = dbU.ExecuteDataTable("select * from HNJH_RosterProvider_Xmpie order by ProvGroupID, ProviderGroupName");
            Networkfname = ProcessVars.SourceDataRoster + "\\PROVIDER_" + PagNoXmpie + ".csv";
            localname = localdir + "PROVIDER_" + PagNoXmpie + ".csv";
            resultCSV = createcsv.printCSV_fullProcess(localname, Provider, "", "Y");
            if (File.Exists(Networkfname))
                File.Delete(Networkfname); 
            File.Copy(localname, Networkfname);
            return result;
        }
        public void ProcessXMpiePdf_ready()
        {


        }
        public void SCI_file_after_XMpiePdf(string XMpieName,string OriginalFileName)
        {
            FileInfo fileinfo = new FileInfo(OriginalFileName);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            SqlParameter[] sqlParams;
            sqlParams = null;
            sqlParams = new SqlParameter[] { new SqlParameter("@PagName", XMpieName), 
                        new SqlParameter("@table", "HOR_parse_HNJH_Panel_Roster_Provider"),
                        new SqlParameter("@FileName", OriginalFileName)};



            DataTable datato_SCI = dbU.ExecuteDataTable("HOR_rpt_PARSE_HNJH_Roster_to_SCI", sqlParams);
            if (datato_SCI.Rows.Count > 0)
            {
                createCSV createcsv = new createCSV();
                //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
                string pName = ProcessVars.SourceDataRoster + "\\" + fileinfo.Name.Substring(0,fileinfo.Name.Length - 4) + "__" +  XMpieName + "_TO_SCI.csv";
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

        }

        public void printNDtoSCI(string filename)
        {
            SqlParameter[] sqlParams3;
            sqlParams3 = null;
            sqlParams3 = new SqlParameter[] { new SqlParameter("@FileName", filename), new SqlParameter("@table", "HOR_parse_HNJH_Panel_Roster_Provider") };

            string spName = "HOR_rpt_PARSE_HNJH_Roster_ND_to_SCI";  // "HOR_rpt_PARSE_cbILLSto_SCI";


            //DataTable datato_SCI_ND = dbU.ExecuteDataTable(spName, sqlParams3);
            //if (datato_SCI_ND.Rows.Count > 0)
            //{
            //    createCSV createcsv = new createCSV();
            //    //string pName = ProcessVars.CBOutputDirectory + FFName.Substring(0, FFName.Length - 4) + "_ToSCI.csv";
            //    string pName = ProcessVars.SourceDataRoster + filename + "_ND_TO_SCI.csv";
            //    if (File.Exists(pName))
            //        File.Delete(pName);
            //    var fieldnames = new List<string>();
            //    for (int index = 0; index < datato_SCI_ND.Columns.Count; index++)
            //    {
            //        fieldnames.Add(datato_SCI_ND.Columns[index].ColumnName);
            //    }
            //    bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            //    foreach (DataRow row in datato_SCI_ND.Rows)
            //    {

            //        var rowData = new List<string>();
            //        for (int index = 0; index < datato_SCI_ND.Columns.Count; index++)
            //        {
            //            rowData.Add(row[index].ToString());
            //        }
            //        resp = false;
            //        resp = createcsv.addRecordsCSV(pName, rowData);
            //    }

            //}

        }
        public void pritnSummary(string reportdate, string StoredProc, string xName)
        {
            //SqlParameter[] sqlParams;
            //sqlParams = null;

            //sqlParams = new SqlParameter[] { new SqlParameter("@Date", reportdate) };
            //DataTable summary = dbU.ExecuteDataTable("HOR_rpt_HNJH_Roster__Summary_Date", sqlParams);
      

            SqlDataReader rdr = null;
            DataTable Summary_File = new DataTable();
            DataTable Detail_DL_N = new DataTable();
            DataTable Summary_County = new DataTable();
            DataTable Summary_NJH = new DataTable();
            //string xName = ProcessVars.SourceDataRosterDir + reportdate + "\\Pnel_Roster\\summary.xls";
            
            if (File.Exists(xName))
                File.Delete(xName);
            using (SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(StoredProc, Connection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    cmd.Parameters.Add("@Date", SqlDbType.VarChar).Value = reportdate.Replace("_", "/");
                    int recType = 1;
                    Connection.Open();

                    rdr = cmd.ExecuteReader();

                    if (rdr.HasRows)
                    {

                        Summary_File.Load(rdr);
                        Summary_NJH.Load(rdr);
                        Detail_DL_N.Load(rdr);
                    }

                    else
                    {
                        Console.WriteLine("No rows found.");
                    }
                    rdr.Close();
                    Export_XLSX createxls = new Export_XLSX();
                    createxls.CreateExcelFileTables(Summary_File, "Summary Files", xName
                                    , Summary_NJH, "Total Records", Detail_DL_N, "Non Deliverables"

                                    );


                }
            }

            
        }
            
    }
}