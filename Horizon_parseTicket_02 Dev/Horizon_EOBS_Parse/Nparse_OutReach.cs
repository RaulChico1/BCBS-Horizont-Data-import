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
using System.Text.RegularExpressions;
using System.Diagnostics;


namespace Horizon_EOBS_Parse
{
    public class Nparse_OutReach
    {
        DBUtility dbU;
        DataTable dt = new DataTable();
        public string importOutReach(string location)
        {
            string totErrors = "";
            DirectoryInfo xlss = new DirectoryInfo(location);

            FileInfo[] files = xlss.GetFiles("*.txt");

            string errors = "";
            foreach (FileInfo file in files)
            {
                string partFilename = file.Name.Substring(0, 3);
                if (partFilename.IndexOf("_") == -1 && partFilename.IndexOf("._") == -1)
                {
                    errors = processData(file.FullName);
                    if (errors == "")
                    {
                        Directory.Move(file.FullName, file.Directory + "\\__" + file.Name);
                        //errors = printCSV(GlobalVar.DateofProcess.ToShortDateString(), file.FullName);

                    }
                    else
                        totErrors = totErrors + ", " + errors;
                }
            }

            //process return BCC
            //var t0 = Task.Run(async delegate
            //{
            //    await Task.Delay(1000 * 60 * 2);
            //    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
            //});
            //t0.Wait();

            //appSets appsets = new appSets();
            //appsets.setVars();

            //string result = "";
            //GlobalVar.dbaseName = "BCBS_Horizon";
            //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);


            ////string strsql = "select filenamecass from HOR_parse_files_to_CASS where TableName = 'HOR_parse_HNJH_Panel_Roster_Provider' ";
            //string strsql = "select filenamecass, Processed from HOR_parse_files_to_CASS where TableName =  " +
            //                "'HOR_DirectMail' and Processed is null";
            //DataTable table_BCCToProcess = dbU.ExecuteDataTable(strsql);
            //foreach (DataRow row in table_BCCToProcess.Rows)
            //{
            //    ProcessBackData(row[0].ToString(), location);


            //    //Update XMPie file name

            //}



            return errors;
        }

        public string processData(string filename)
        {
            int updErrors = 0;
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            int GRecnum = 1;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;



            string result = "";
            FileInfo fileInfo = new System.IO.FileInfo(filename);





            DataTable fromTAB_tmp = loadCommadata(filename);

            DataTable newTABTable = fromTAB_tmp.Select().Where(x => !x.IsNull(0)).CopyToDataTable();

            //foreach (var column in newTABTable.Columns.Cast<DataColumn>().ToArray())
            //{
            //    if (newTABTable.AsEnumerable().All(dr => dr.IsNull(column)))
            //        newTABTable.Columns.Remove(column);
            //}

            newTABTable.Columns.Add("DateImport").SetOrdinal(0);
            newTABTable.Columns.Add("FileName").SetOrdinal(0);
            newTABTable.Columns.Add("Recnum").SetOrdinal(0);
            //newTABTable.Columns.Add("FullName").SetOrdinal(7);


            foreach (DataRow row in newTABTable.Rows)
            {
                row["FileName"] = fileInfo.Name;
                row["DateImport"] = DateTime.Now;
                row["Recnum"] = GRecnum;

                GRecnum++;
            }


            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            dbU.ExecuteScalar("delete from HOR_Parse_OutReach_TMP");


            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                bulkCopy.DestinationTableName = "[dbo].[HOR_Parse_OutReach_TMP]";

                try
                {
                    bulkCopy.WriteToServer(newTABTable);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
            }
            Connection.Close();
            string BCCname = "";
            if (updErrors == 0)
            {
                try
                {
                    //replace nulls
                    SqlParameter[] sqlParams2;
                    sqlParams2 = null;
                    sqlParams2 = new SqlParameter[] { new SqlParameter("@TableName", "HOR_Parse_OutReach_TMP") };

                    dbU.ExecuteScalar("HOR_upd_NULLS_inTable", sqlParams2);



                    dbU.ExecuteScalar("Insert into HOR_Parse_OutReach select * from HOR_Parse_OutReach_TMP");

                    BCCname = fileInfo.Name.Substring(0, fileInfo.Name.Length - fileInfo.Extension.Length) + " File not to CASS";
                    string directoryAfterCass = fileInfo.DirectoryName;  // ProcessVars.InputDirectory + "FromCASS";


                    dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_Parse_OutReach', GETDATE())");

                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                                    newTABTable.Rows.Count + ",'" + BCCname + "','" + fileInfo.Name + "','" + fileInfo.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss") + "','HOR_Parse_OutReach','" +
                                                    directoryAfterCass + "','','','','Receive','" + GlobalVar.DateofProcess + "')");
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
                //sendBCC(fileInfo.DirectoryName, BCCname, fileInfo.Name);
            }

            return errors;
        }
        static IEnumerable<string> ReadAsLines(string filename)
        {
            using (var reader = new StreamReader(filename))
                while (!reader.EndOfStream)
                    yield return reader.ReadLine();
        }
        public DataTable loadCommadata(string filename)
        {

            //var ch = ',';
            //var n = 4;

            ////var filename = "tabfile.txt";
            //var reader = ReadAsLines(filename);

            //var data = new DataTable();

            ////this assume the first record is filled with the column names
            ////string Line_hearders = "ID,FirstName,MI,LastName,FullName,ADDR1,ADDR2,CITY,ST,ZIP,GRGR,SUBGROUP,CLASS,PRODUCT,CLASSPLAN,EAMDT,GENDT,LetterName,TEMPLATE,FULL_SKU,APPEND_NAME,GEN_ID,BRE,PROV_NAME,PROV_NPI,PROV_TIN,CLCL,OWNER";
            //string Line_hearders = "ID,FirstName,MI,LastName,ADDR1,ADDR2,CITY,ST,ZIP,GRGR,SUBGROUP,CLASS,PRODUCT,CLASSPLAN,EAMDT,GENDT,LetterName,TEMPLATE,FULL_SKU,APPEND_NAME,GEN_ID,BRE,PROV_NAME,PROV_NPI,PROV_TIN,CLCL,OWNER";
            //int Hcommas = Line_hearders.Count(x => x == ',');
            //var headers = Line_hearders.Split(',');
            //foreach (var header in headers)
            //    data.Columns.Add(header);
            //string new_Record = "";
            //var records = reader;
            //foreach (var record in records)
            //{
            //    int commas = record.Count(x => x == ',');
            //    if (commas == 21)
            //    {
            //        var result = record
            //       .Select((c, i) => new { c, i })
            //       .Where(x => x.c == ch)
            //       .Skip(n - 1)
            //       .FirstOrDefault();
            //        new_Record = record.Substring(0, result.i + 1) + "," +  record.Substring(result.i + 1, record.Length - (result.i + 1));
            //    }
            //    else
            //        new_Record = record;
            //    data.Rows.Add(new_Record.Split(','));
            //}
            //return data;
           // ===================================================
            var ch = ',';
            var n = 5;

            //var filename = "tabfile.txt";
            var reader = ReadAsLines(filename);

            var data = new DataTable();

            //this assume the first record is filled with the column names
            string Line_hearders = "ID,FirstName,MI,LastName,FullName,ADDR1,ADDR2,CITY,ST,ZIP,GRGR,SUBGROUP,CLASS,PRODUCT,CLASSPLAN,EAMDT,GENDT,LetterName,TEMPLATE,FULL_SKU,APPEND_NAME,GEN_ID,BRE,PROV_NAME,PROV_NPI,PROV_TIN,CLCL,OWNER";
            //string Line_hearders = "ID,FirstName,MI,LastName,ADDR1,ADDR2,CITY,ST,ZIP,GRGR,SUBGROUP,CLASS,PRODUCT,CLASSPLAN,EAMDT,GENDT,LetterName,TEMPLATE,FULL_SKU,APPEND_NAME,GEN_ID,BRE,PROV_NAME,PROV_NPI,PROV_TIN,CLCL,OWNER";
            int Hcommas = Line_hearders.Count(x => x == ',');
            var headers = Line_hearders.Split(',');
            foreach (var header in headers)
                data.Columns.Add(header);
            string new_Record = "";
            string new_Record2 = "";
            var records = reader;
            foreach (var record in records)
            {
                //n = 5;
                //int commas = record.Count(x => x == ',');
                //if (commas == 22)
                //{
                //    var result = record
                //   .Select((c, i) => new { c, i })
                //   .Where(x => x.c == ch)
                //   .Skip(n - 1)
                //   .FirstOrDefault();
                //    new_Record = record.Substring(0, result.i + 1) + "," + record.Substring(result.i + 1, record.Length - (result.i + 1));
                //}
                //else
                //    new_Record = record;

                int commas = record.Count(x => x == ',');
                n = 4;
                if (record.IndexOf("700 LONGFELLOW ST") != -1)
                    n = 4;
                var result = record
                   .Select((c, i) => new { c, i })
                   .Where(x => x.c == ch)
                   .Skip(n - 1)
                   .FirstOrDefault();
                long number1 = 0;
                bool canConvert = long.TryParse(record.Substring(result.i + 1, 1), out number1);
                if (canConvert == true)
                    new_Record = record.Substring(0, result.i + 1) + "," + record.Substring(result.i + 1, record.Length - (result.i + 1));
                else
                    new_Record = record;

                n = 8;

                var result2 = new_Record
                   .Select((c, i) => new { c, i })
                   .Where(x => x.c == ch)
                   .Skip(n - 1)
                   .FirstOrDefault();
                number1 = 0;
                canConvert = long.TryParse(new_Record.Substring(result2.i + 1, 1), out number1);
                if (canConvert == true)
                {
                    n = 6;
                    var result3 = new_Record
                   .Select((c, i) => new { c, i })
                   .Where(x => x.c == ch)
                   .Skip(n - 1)
                   .FirstOrDefault();
                    new_Record = new_Record.Substring(0, result3.i + 1) + "," + new_Record.Substring(result3.i + 1, new_Record.Length - (result3.i + 1));
                }
                //else
                //    new_Record = record;




                data.Rows.Add(new_Record.Split(','));
            }
            return data;
        }

        private void closeOutReach()
        {

            string erros = "";
            DBUtility dbU;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
          

            DataTable resultstoInterim = dbU.ExecuteDataTable("HOR_upd_DailyUpload_toInterim_OutReach");
           
            if (resultstoInterim.Rows.Count > 0)
            {
                string colnames = "";
                for (int index = 0; index < resultstoInterim.Columns.Count; index++)
                {
                    string colname = resultstoInterim.Columns[index].ColumnName;
                    colnames = colnames + ", [" + colname + "]";
                }
                string recnumError = "";
                string insertCommand1 = "Insert into CIE_Interim_JobReceipt_Manual (" + colnames.Substring(1, colnames.Length - 1) + ") VALUES ('";
                foreach (DataRow row in resultstoInterim.Rows)
                {
                    DateTime cycleDate = DateTime.Parse(row[0].ToString());
                    string cDate = cycleDate.Year + "-" + cycleDate.Month.ToString("00") + "-" + cycleDate.Day.ToString("00");

                    var resultUpd = dbU.ExecuteScalar("select filename from CIE_Interim_JobReceipt_Manual where filename = '" + row[1].ToString() + "' and Cycledate = '" + cDate + "'");
                    if (resultUpd == null)
                    {
                        string insertCommand2 = "";
                        for (int index = 0; index < resultstoInterim.Columns.Count; index++)
                        {
                            insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''").Trim() + "','";
                        }
                        try
                        {
                            recnumError = row[0].ToString();
                            var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                        }
                        catch (Exception ex)
                        {

                            erros = erros + ex.Message + "\n\n";
                        }
                    }
                    else
                    {
                        var here = "";
                    }
                }
                Process.Start("http://businessintel.cierant.com/njhorizon/sys_cnb.aspx?task=receive&Date=" + GlobalVar.DateofProcess.ToString("yyyyMMdd"));
            }
            //Ticket createTicket = new Ticket();
            //createTicket.createTicket(resultsTicket01, ProcessVars.InputDirectory + @"From_FTP\", "02");
            //Results.Text = "Ticket 02 Closed" + erros;


        }
    }
}
