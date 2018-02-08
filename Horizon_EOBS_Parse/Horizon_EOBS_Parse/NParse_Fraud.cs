using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;

namespace Horizon_EOBS_Parse
{
    public class NParse_Fraud
    {
        public string create_csv_Fraud(string dateProcess)
        {
            DBUtility dbU;
             GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
             DataTable dataFraud = dbU.ExecuteDataTable("select recnum, filename, sysout, jobname, " +
                        "'' as printdate, '' as archivedate, '' as c_recnum, '' as seq, '' as de_flag, " +
                        "'' as Jobid, '' as field2, '' as field3, '' as field4, '' as fiels5, '' as field6, " +
                        " First_Name + Last_Name as Addr1, Horizon_Street as Addr2, HORIZON_STREET2 as addr3, "+
                        "'' as addr4, '' as addr5, HORIZON_CITY + ' ' + HORIZON_state + ' ' + HORIZON_zip as Addr6 " +
                        "from HOR_Fraud where CONVERT(DATE,ImportDate)='" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'");


             string fileName = ProcessVars.InputDirectory +  dataFraud.Rows[0][1].ToString();
             string sysout = dataFraud.Rows[0][2].ToString();
             string jobID = dataFraud.Rows[0][3].ToString();
             createCSV createcsv = new createCSV();
             //string pName = System.IO.Directory.GetParent(directoryTXT).FullName + @"\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
             string pName = fileName.Substring(0, fileName.Length - 4) + ".csv";
            
             createCAS_CSV createCSV = new createCAS_CSV();
             if (dataFraud.Rows.Count > 0)
             {

                 string resultcsv = createCSV.create_Fraud_CAS_CSV(
                                     fileName, dataFraud, "HOR_Fraud", dataFraud.Rows.Count, dataFraud.Rows.Count.ToString(), sysout, jobID, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"));
                 
             }
             return "";
        }
    }
}
