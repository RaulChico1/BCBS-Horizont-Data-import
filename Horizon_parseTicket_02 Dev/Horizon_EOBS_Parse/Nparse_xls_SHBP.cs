using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Xml;
using System.Xml.Linq;
using System.Configuration;

namespace Horizon_EOBS_Parse
{
    public class Nparse_xls_SHBP
    {
        DBUtility dbU;

        public void parse_all_SHBP_EOC()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string results = "";
            string DirLocal = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\SHBP_test data\";
            //DirectoryInfo originalZIPs = new DirectoryInfo(DirLocal + @"from_FTP");
            DirectoryInfo originalXLs = new DirectoryInfo(DirLocal);
            FileInfo[] FilesXLS = originalXLs.GetFiles("EOC*.xls");
            if (FilesXLS.Count() > 0)
            {
                foreach (FileInfo file in FilesXLS)
                {
                    if (file.Name.IndexOf("__EOC") == 0)
                    { }
                    else
                    {
                        results = parse_SHBP_EOC(file.FullName.ToString(), DirLocal, file.Name);
                        if (results == "")
                        {
                            File.Move(file.FullName, ProcessVars.OtherProcessed + file.Name);
                            File.Copy(file.FullName.Replace(".zip", ".csv"), ProcessVars.OtherProcessed + file.Name.Replace(".zip", ".csv"));
                        }
                    }

                }
            }
        }
        public string parse_SHBP_EOC(string filename, string DirLocal, string JustFname)
        {
            string result = "";
            DataTable fromXLSTmp = loadXLSX(filename);
            fromXLSTmp.Columns[0].ColumnName = "Service";
            fromXLSTmp.Columns[1].ColumnName = "GovTcode";
            fromXLSTmp.Columns.Add("ImportDate", typeof(DateTime)).SetOrdinal(0);
            fromXLSTmp.Columns.Add("FileName", typeof(String)).SetOrdinal(0);
            fromXLSTmp.Columns.Add("O_Seq", typeof(Int64)).SetOrdinal(0);
            fromXLSTmp.Columns.Add("Recnum", typeof(Int64)).SetOrdinal(0);
            fromXLSTmp.Columns.Add("OriginalAddress", typeof(String));
            fromXLSTmp.Columns.Add("OriginalCSZ", typeof(String));
            fromXLSTmp.Columns.Add("UpdAddr1");
            fromXLSTmp.Columns.Add("UpdAddr2");
            fromXLSTmp.Columns.Add("UpdAddr3");
            fromXLSTmp.Columns.Add("UpdAddr4");
            fromXLSTmp.Columns.Add("UpdAddr5");
            fromXLSTmp.Columns.Add("City");
            fromXLSTmp.Columns.Add("State");
            fromXLSTmp.Columns.Add("Zip");
            fromXLSTmp.Columns.Add("DL");
            int O_seq = 1;

            foreach (DataRow row in fromXLSTmp.Rows) // Loop over the rows.
            {
                row["Recnum"] = O_seq;
                row["O_Seq"] = O_seq;
                O_seq++;
                row["ImportDate"] = DateTime.Now;
                //row["Xmpie_File"] = fileInfo.Name.Replace(".txt", "_") + stringResult + FileSeq.ToString("00");

                //row["Xmpie_Date"] = DateTime.Now;
                //row["Flag_Xmpie"] = "1";
                row["FileName"] = JustFname;
                row["DL"] = "";
                string[] words = row["Address"].ToString().Replace(",", " ").Replace("  "," ").Split(' ');
                if(words[words.Length-1].ToString() == "")
                    Array.Resize(ref words, words.Length - 1);
                int lenW = words.Length-3;
                //if(words[lenW].Length > 2)
                //{
                    string new_Name = "";
                    for (int x = 0; x < lenW; x ++)
                    {
                        new_Name = new_Name + " " + words[x].ToString();
                    }
                    row["OriginalAddress"] = new_Name;

                    string new_CSZ = "";
                    for (int x = lenW; x < words.Length ; x++)
                    {
                        new_CSZ = new_CSZ + " " + words[x].ToString();
                    }
                    row["OriginalCSZ"] = new_CSZ;
                //}
                //else
                //{
                //    string new_Name = "";
                //    for (int x = 0; x < lenW; x++)
                //    {
                //        new_Name = new_Name + " " + words[x].ToString();
                //    }
                //    row["OriginalAddress"] = new_Name;

                //    string new_CSZ = "";
                //    for (int x = lenW; x < words.Length; x++)
                //    {
                //        new_CSZ = new_CSZ + " " + words[x].ToString();
                //    }
                //    row["OriginalCSZ"] = new_CSZ;
                //}
            }


            return result;
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
                    if (row["TABLE_NAME"].ToString().Contains("Sheet1"))
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
    }
}
