using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;


namespace Horizon_EOBS_Parse
{
    public class Nparse_UPPR
    {
        DataTable DataTable = Data_Table();
        List<string> addrs = new List<string>();
        DBUtility dbU;
        private static DataTable Data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Sheet_Count");
            newt.Columns.Add("MemberID");
            newt.Columns.Add("ProviderID");
            newt.Columns.Add("Name");
            newt.Columns.Add("zip");
            newt.Columns.Add("bkcode");
            newt.Columns.Add("paymentNbr");
            newt.Columns.Add("amt");
            newt.Columns.Add("Seq");
            
            newt.Columns.Add("filename");
            return newt;
        }
        public string processUPPRs()
        {
            string Results = "";
            appSets appsets = new appSets();
            appsets.setVars();

            string locationLocal = ProcessVars.InputDirectory + "Decrypted";  // @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\Upprs\2016-11-03\";

            DirectoryInfo txts = new DirectoryInfo(locationLocal);

            FileInfo[] files = txts.GetFiles("UPPR*.txt");

            string errors = "";
            foreach (FileInfo file in files)
            {
                if (file.Name.IndexOf("__") == -1)
                {
                    GlobalVar.dbaseName = "BCBS_Horizon";
                    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                    dbU.ExecuteScalar("delete from HOR_parse_UPPR where filename = '" + file.Name + "'");


                    errors = evaluate_TXT(file.FullName);
                    if (errors == "")
                    {


                        string nfilename = file.Directory + "\\__" + file.Name;
                        if (File.Exists(nfilename))
                            File.Delete(nfilename);
                        File.Move(file.FullName, nfilename);
                    }
                    else
                    {
                        Results = Results.ToString() + " errors:  " + file.Name  + "  " + errors + Environment.NewLine;
                    }
                }
            }
            
            return Results;
        }


        public string evaluate_TXT(string fileName)
        {
            int updErrors = 0;

            int prevline = 0;
            int oline = 0;
            int currLine = 0;
            bool fsys = false;
            string line;
            string errors = "";
            DataTable.Clear();
            string add1 = "PG1 OUTPUT-SEQUENCE-NBR";
            string final = "VBPRORPT";
            string final2 = "TOTAL PAGES";
            string final3 = "TOTAL NUMBER";
            string final4 = "TOTAL AMOUNT";
            string final5 = "TOTAL BANK";
            FileInfo fileInfo = new System.IO.FileInfo(fileName);

            System.IO.StreamReader file =
              new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    oline++;
                    if (oline == 60)
                        errors = "";
                    if (line.IndexOf(add1) != -1 && !fsys)
                    {
                        prevline = currLine;
                        fsys = true;
                    }
                    if (line.IndexOf(final) != -1 || line.IndexOf(final2) != -1
                        || line.IndexOf(final3) != -1 || line.IndexOf(final4) != -1
                        || line.IndexOf(final5) != -1)
                    {
                        prevline = currLine;
                        fsys = false;
                    }
                    if (currLine > prevline && fsys && line.Length > 1)
                    {
                        addrs.Add(line.Substring(6, 19));
                        if (line.Substring(27, 22).IndexOf("3HZ") == 0)
                        {
                            addrs.Add(line.Substring(27, 22));
                            addrs.Add("");
                        }
                        else
                        {
                            addrs.Add("");
                            addrs.Add(line.Substring(27, 22));
                        }

                        addrs.Add(line.Substring(49, 27));
                        addrs.Add(line.Substring(76, 10));
                        addrs.Add(line.Substring(93, 2));
                        addrs.Add(line.Substring(101, 10));

                        if (line.Length < 112)
                            addrs.Add("");
                        else
                            addrs.Add(line.Substring(114, 13));
                        addToTable(oline, fileInfo.Name);


                    }

                    currLine++;


                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;
                    updErrors++;
                }


            }
            file.Close();

            if (updErrors == 0)
            {
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_UPPR_tmp");


                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    //bulkCopy.DestinationTableName =
                    //    "[dbo].[Tempo_fsaData]";
                    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_UPPR_tmp]";

                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(DataTable);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;
                        updErrors++;
                    }
                }
                Connection.Close();
                if (updErrors == 0)
                {
                    try
                    {
                        dbU.ExecuteScalar("Insert into HOR_parse_UPPR select * from HOR_parse_UPPR_tmp");
                        string strsql = "update HOR_parse_UCDS set memberid = U.memberid, ProviderID = u.providerid, paymentNbr = u.paymentNbr from HOR_parse_UPPR U join HOR_parse_UCDS D on u.sheet_count = d.Sheet_count " +
                                        "where U.FileName = '" + fileInfo.Name + "'";
                        dbU.ExecuteNonQuery(strsql);
                        dbU.ExecuteNonQuery("update HOR_parse_UPPR set importdate = getdate() where filename = '" + fileInfo.Name + "'");
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;
                        updErrors++;
                    }
                }
            }
            return errors;
        }
        public void addToTable(int online, string fname)
        {
            if (addrs[0].ToString().ToUpper().IndexOf("UCDSIM") == -1)
            {
                var row = DataTable.NewRow();
                row["Sheet_Count"] = addrs[0];
                row["MemberID"] = addrs[1];
                row["ProviderID"] = addrs[2];
                row["Name"] = addrs[3];
                row["zip"] = addrs[4];
                row["bkcode"] = addrs[5];
                row["paymentNbr"] = addrs[6];
                row["amt"] = addrs[7];
                row["seq"] = online;
                row["filename"] = fname;

                DataTable.Rows.Add(row);
            }
            else
            {
                var noproc = "Here";
            }
            addrs.Clear();
           
        }
    }
}
