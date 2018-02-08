using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class NParse_AbilTO
    {
        DBUtility dbU;

        public string printCSV( )
        {

            //NOTE:   upload MANUALLY for now,   Recnum, importdate and filename in 3 first columns
            string directory = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2016-02-17\AbilTo";
            string strsql = "select distinct filename from HOR_parse_AbilTO where convert(date,dateimport) = '2016-02-17'";
            string strsql2 = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            DataTable filenames = dbU.ExecuteDataTable(strsql);
            foreach (DataRow file in filenames.Rows)
            {
                createCSV createcsv = new createCSV();

                strsql2 = "select recnum, First_name, Last_name, Address1, Address2, City, State, Zip from  HOR_parse_AbilTO where filename = '" + file[0].ToString() + "'";
                DataTable datatoPrint = dbU.ExecuteDataTable(strsql2);
                string filename = directory + "\\" + file[0].ToString().Replace(".xls", "") + ".csv";

                if (File.Exists(filename))
                    File.Delete(filename);
                var fieldnames = new List<string>();
                for (int index = 0; index < datatoPrint.Columns.Count; index++)
                {
                    fieldnames.Add(datatoPrint.Columns[index].ColumnName);
                }
                bool resp = createcsv.addRecordsCSV(filename, fieldnames);
                resp = createcsv.addRecordsCSV(filename, fieldnames);
                foreach (DataRow row in datatoPrint.Rows)
                {
                    var rowData = new List<string>();
                    for (int index = 0; index < datatoPrint.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    bool resp2 = false;
                    resp2 = createcsv.addRecordsCSV(filename, rowData);
                }
            }
            return "ok";
        }

    }
}
