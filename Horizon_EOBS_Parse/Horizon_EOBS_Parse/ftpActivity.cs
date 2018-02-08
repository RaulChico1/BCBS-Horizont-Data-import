using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Configuration;
using System.Linq;
using System.Reflection;


namespace Horizon_EOBS_Parse
{
    public class ftpActivity
    {
        int Recnum = 1;
        DBUtility dbU;
        DataTable DataTable = Data_Table();
        DataTable DataTableD = Data_TableDet();
        int currLine = 0;

        private static DataTable Data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Seqnum");
            newt.Columns.Add("Line");
            newt.Columns.Add("SourceFileName");
            newt.Columns.Add("ImportDate");
            newt.Columns.Add("ActivityDate");
            newt.Columns.Add("FileName");
            newt.Columns.Add("ProcessDate");
            newt.Columns.Add("ProcessType");
            newt.Columns.Add("Date_in_Report");
            newt.Columns.Add("In_Report");
            newt.Columns.Add("Comments");
            newt.Columns.Add("Mark");
            
            return newt;
        }
        private static DataTable Data_TableDet()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Seqnum");
            newt.Columns.Add("Line");
            newt.Columns.Add("SourceFileName");
            newt.Columns.Add("ImportDate");
            newt.Columns.Add("ActivityDate");
            newt.Columns.Add("FileName");
            newt.Columns.Add("Directory");
           
            return newt;
        }
        public string evaluate_TXT(string fileName)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            int errorcount = 0;
            string results = "";
            var recnum = dbU.ExecuteScalar("select max(Seqnum) from HOR_ftp_Activity");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                Recnum = 1;
            else
                Recnum = Convert.ToInt32(recnum.ToString()) + 1;


            

            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            currLine = 1;
            int valueOk = 0;
            string line;
            DataTable.Clear();

            FileInfo finfo = new FileInfo(fileName);
        
            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (line.IndexOf(": open ") != -1)
                    {
                        int posc_dum = line.IndexOf("domU");
                        int posc_open = line.IndexOf(": open ") + 8;
                        if (posc_dum > 0)
                        {
                            string dateTime = line.Substring(0, posc_dum - 1);
                            string fname = line.Substring(posc_open, line.Length - posc_open);
                            int poscEndFname = fname.IndexOf(" flags") - 1;

                            string DirFname = fname.Substring(0, poscEndFname).Replace("\"","").Trim();
                            int lastSlash = DirFname.LastIndexOf("/") + 1;
                            string directory = DirFname.Substring(0, lastSlash - 1).Trim();
                            string justName = DirFname.Substring(directory.Length + 1, DirFname.Length - (directory.Length + 1)).Trim().Replace(@"'", @"''");

                            var row = DataTable.NewRow();
                            row["Seqnum"] = Recnum;
                            row["Line"] = currLine;
                            row["SourceFileName"] = finfo.Name;
                            row["ImportDate"] = DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
                            row["ActivityDate"] = dateTime.Trim();
                            row["FileName"] = justName.Trim();
                            
                            DataTable.Rows.Add(row);


                          
                            var row2 = DataTableD.NewRow();
                            row2["Seqnum"] = Recnum;
                            row2["Line"] = currLine;
                            row2["SourceFileName"] = finfo.Name;
                            row2["ImportDate"] = DateTime.Now.AddDays(0).ToString("yyyy-MM-dd");
                            row2["ActivityDate"] = dateTime.Trim();
                            row2["FileName"] = justName.Trim();
                            row2["Directory"] = directory.Trim();
                            DataTableD.Rows.Add(row2);

                            Recnum++;


                            
                        }

                    }

                    currLine++;
                    if (currLine == 4)
                        valueOk++;
                }
                catch (Exception ex)
                {
                    errorcount++;
                }
            }
            file.Close();

            var groups = from r in DataTable.AsEnumerable()
                         group r by new
                         {
                             Col1 = r.Field<String>("FileName")
                         };
            DataTable distinctTable = groups.Select(g => g.First()).CopyToDataTable();


            var groups2 = from r in DataTableD.AsEnumerable()
                         group r by new
                         {
                             Col1 = r.Field<String>("FileName"),
                             Col2 = r.Field<String>("Directory"),
                         };
            DataTable distinctTableD = groups2.Select(g => g.First()).CopyToDataTable();



            foreach (DataRow row in distinctTable.Rows)
            {

                var checkFName = dbU.ExecuteScalar("Select Filename from HOR_ftp_Activity where filename = '" + row["FileName"].ToString() + "'");
                if (checkFName != null)
                {
                        row["Mark"] = "x";
                }

            }
            var rows2 = distinctTable.Select("Mark = 'x'");
            foreach (var row in rows2)
                row.Delete();


            distinctTable.Columns.Remove("Mark");
            if (errorcount == 0)
            {



                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_ftp_Activity_tmp");
                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    bulkCopy.DestinationTableName = "[dbo].[HOR_ftp_Activity_tmp]";

                    try
                    {
                        bulkCopy.BatchSize = DataTable.Rows.Count;
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.WriteToServer(distinctTable);
                    }
                    catch (Exception ex)
                    {
                        errorcount++;
                        //string errorMessage = base.GetBulkCopyColumnException(ex, Connection);
                        //// errorMessage contains "Column: "XYZ" contains data with a length greater than: 20", column, length  
                        //Exception exInvlidColumn = new Exception(errorMessage, ex);
                        //base.LogDataAccessException(exInvlidColumn, System.Reflection.MethodBase.GetCurrentMethod().Name);  

                    }
                }
                Connection.Close();
                if (errorcount == 0)
                {
                    dbU.ExecuteScalar("Insert into HOR_ftp_Activity select * from HOR_ftp_Activity_tmp");
                }


                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    bulkCopy.DestinationTableName = "[dbo].[HOR_ftp_File_Directory_Activity]";

                    try
                    {
                        bulkCopy.BatchSize = DataTable.Rows.Count;
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.WriteToServer(distinctTableD);
                    }
                    catch (Exception ex)
                    {
                        errorcount++;
                    }
                }
                Connection.Close();

            }
            else
                results = "error";

            return results;
        }
    }
}
