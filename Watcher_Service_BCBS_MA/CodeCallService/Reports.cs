using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Configuration;

namespace CodeCallService
{
    public class ReportsBCBS
    {
        DBUtility dbU;
        public void pritnSummary()
        {

            string StoredProc = "BCBS_MA_rpt_Mail_Dates_Extract_nulls";
            
            SqlDataReader rdr = null;
            DataTable Summary_BCBSMA = new DataTable();
          
            string xName = @"\\freenas\BCBSMA\PRODUCTION\Reports\Update_BCBS_MA_MailDates_" + DateTime.Now.ToString("MM_dd_yyyy__HH_mm") + ".xls";

            if (File.Exists(xName))
                File.Delete(xName);
            using (SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(StoredProc, Connection))
                {
                    cmd.CommandType = CommandType.StoredProcedure;

                    //cmd.Parameters.Add("@Date", SqlDbType.VarChar).Value = reportdate.Replace("_", "/");
                    int recType = 1;
                    Connection.Open();

                    rdr = cmd.ExecuteReader();

                    if (rdr.HasRows)
                    {

                        Summary_BCBSMA.Load(rdr);
                    }

                    else
                    {
                        Console.WriteLine("No rows found.");
                    }
                    rdr.Close();
                    Export_XLSX createxls = new Export_XLSX();
                    createxls.CreateExcelFileOneTables(Summary_BCBSMA, "BCBS MA Files to Update Mail Date", xName);
                }
            }


        }

    }
}
