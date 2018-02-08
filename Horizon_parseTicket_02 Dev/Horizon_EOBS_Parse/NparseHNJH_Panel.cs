using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Xml;
using System.Xml.Linq;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Text.RegularExpressions;


namespace Horizon_EOBS_Parse
{
    public class NparseHNJH_Panel
    {
        string errors = "";
        DBUtility dbU;
       
        int errorcount = 0;
        int Recnum = 1;
        int GRecnum = 1;
        int currLine = 0;
        int seqBundle = 0;
        int FileSeq = 0;


        public string ProcessFilesinDir(string dateProcess, string LocalDirectory, string step)
        {
            string result = "";
            if (Directory.Exists(LocalDirectory))
            {

                result = result + " " + FilestoProcess_HNJH_Provider(dateProcess, LocalDirectory, step);

            }
            return result;
        }
        public string FilestoProcess_HNJH_Provider(string dateProcess, string subdirectory, string step)        //not maintenance
        {
            string InsertName = "";

            //if (Directory.Exists(ProcessVars.InputDirectory))
            if (Directory.Exists(subdirectory))
            {
                //string dirIDCards = ProcessVars.InputDirectory  + @"from_FTP\Con2\";
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.txt");

                //if (FilesDAT.Count() == 1)
                //{
                    foreach (FileInfo file in FilesDAT)
                    {
                        if (file.Name.IndexOf("_") != 0)
                        {
                            InsertName = "";
                            try
                            {
                                if (step == "1")
                                {
                                    //string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString());
                                    string error = load_Roster(file.FullName, InsertName, subdirectory, false, "", false, file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
                                    if (error != "Roster load Done!!")
                                        errors = errors + error + "\n\n";
                                    else
                                    {
                                        FileSeq++;
                                        errors = error;
                                    }
                                }
                                else
                                {
                                    string error = splitRoster(file.Name);
                                     error = evaluate_Roster(file.FullName, InsertName, subdirectory, false, "", false, file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
                                     if (error != "Evaluate Roster Done")
                                         errors = errors + error + "\n\n";
                                     else
                                     {
                                         FileSeq++;
                                         errors = error;
                                     }
                                }
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                //}


            }
            return errors;
        }
        public string load_Roster(string fileName, string insertName, string directoryTXT, bool nozip, string insertBBB, bool testProcess, string LastWriteTime)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            createCSV createcsvT = new createCSV();
            int O_seq = 1;
           

            var fileExist = dbU.ExecuteScalar("select count(recnum) from HOR_parse_HNJH_Panel_Roster_Provider where filename = '" + fileInfo.Name + "'");
            if (fileExist != null && fileExist.ToString() != "0")
            {
                dbU.ExecuteScalar("delete from HOR_parse_HNJH_Panel_Roster_Panel where filename = '" + fileInfo.Name + "'");
                dbU.ExecuteScalar("delete from HOR_parse_HNJH_Panel_Roster_Provider where filename = '" + fileInfo.Name + "'");
            }
            if (!File.Exists(ProcessVars.gmappingFile))
                throw new Exception("Mapping file not found.");


            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();

            List<Field> fields = GetFields();
            foreach (Field field in fields)
            {
                dt1.Columns.Add(field.Name);
                dt2.Columns.Add(field.Name);
            }
            dt1 = ParseFiletoTable(fileName, dt1, "1");
            dt2 = ParseFiletoTable(fileName, dt2, "2");

        

            DataTable table = dt1.Copy();
            table.Merge(dt2);
            table.Rows[0].Delete();
            int recPosc = 0;

            table.Columns.Add("Xmpie_File", typeof(String)).SetOrdinal(0);
            table.Columns.Add("Xmpie_Date", typeof(DateTime)).SetOrdinal(0);
            table.Columns.Add("Flag_Xmpie", typeof(String)).SetOrdinal(0);
            table.Columns.Add("ImportDate", typeof(DateTime)).SetOrdinal(0);
            table.Columns.Add("FileName", typeof(String)).SetOrdinal(0);
            table.Columns.Add("O_Seq", typeof(Int64)).SetOrdinal(0);
            table.Columns.Add("Recnum", typeof(Int64)).SetOrdinal(0);
            table.Columns.Add("DL");
            table.Columns.Add("Med_Flag");
            table.Columns.Add("Type");

            DateTime dt = DateTime.Now;
            string s = dt.ToString("yyyy-MM-ddHHmmss");

            var dateToConvert = DateTime.Today;
            var stringResult = string.Format("{0}{1}", dateToConvert.ToString("yy"), dateToConvert.DayOfYear);

            foreach (DataRow row in table.Rows) // Loop over the rows.
            {
                row["Recnum"] = "0";
                row["O_Seq"] = O_seq;
                O_seq++;
                row["ImportDate"] = DateTime.Now;
                //row["Xmpie_File"] = fileInfo.Name.Replace(".txt", "_") + stringResult + FileSeq.ToString("00");

                row["Xmpie_Date"] = DateTime.Now;
                row["Flag_Xmpie"] = "1";
                row["FileName"] = fileInfo.Name;
                row["DL"] = "";
                GRecnum++;
            }
            //createCSV printfile = new createCSV();
            //printfile.printCSV_fullProcess(fileInfo +  "allData.csv", table, "", "");
                    GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            int updErrors = 0;
            try
            {
                dbU.ExecuteScalar("delete from HOR_parse_HNJH_Panel_Roster_Panel_Row_Data");
            }
            catch (Exception ex)
            {
                errors = errors + ex.Message;    //colid 27   Member Gender
                updErrors++;
            }
            
            SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);
            Connection.Open();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
            {
                bulkCopy.DestinationTableName = "[dbo].[HOR_parse_HNJH_Panel_Roster_Panel_Row_Data]";

                try
                {
                    bulkCopy.BulkCopyTimeout = 2000;
                    bulkCopy.WriteToServer(table);
                }
                catch (Exception ex)
                {
                    errors = errors + ex.Message;    //colid 27   Member Gender
                    updErrors++;
                }
            }
            Connection.Close();

           
            return "Roster load Done!!";


        }

        public string splitRoster(string filename)
        {
            string result = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            dbU.ExecuteNonQuery("delete HOR_parse_HNJH_Panel_Roster_Panel_Row_Data  where provgroupid = 'Provider Gro'");
            dbU.ExecuteNonQuery("delete HOR_parse_HNJH_Panel_Roster_Panel where FileName ='" + filename + "'");
            dbU.ExecuteNonQuery("delete HOR_parse_HNJH_Panel_Roster_Provider where FileName ='" + filename + "'");
            dbU.ExecuteNonQuery("delete HOR_parse_files_to_cass where FileName ='" + filename + "'");
            dbU.ExecuteNonQuery("delete HOR_parse_Seq where tablename = 'HOR_parse_HNJH_Panel_Roster_Provider and convert(date,DateTime) = '" + GlobalVar.DateofProcess.AddDays(0).ToString("yyyy-MM-dd") + "'");
            DataTable tableW = dbU.ExecuteDataTable("select distinct ProvGroupID from HOR_parse_HNJH_Panel_Roster_Panel_Row_Data");
            foreach (DataRow rowx in tableW.Rows)
            {
                DataTable table = dbU.ExecuteDataTable("select * from HOR_parse_HNJH_Panel_Roster_Panel_Row_Data where ProvGroupID = '" + rowx[0].ToString() + "' order by recnum");

                DataTable tblProviders = table.Clone();
                for (int i = 39; i > 36; i--)           //38; i > 35; i--)
                    tblProviders.Columns.RemoveAt(i);
                for (int i = 35; i > 16; i--)           // int i = 34; i > 15; i--)
                    tblProviders.Columns.RemoveAt(i);




                DataTable tblPanel = table.Clone();
                for (int i = 16; i > 3; i--)                //(int i = 15; i > 3; i--)
                    if (i != 11)                            //if (i != 10)
                        tblPanel.Columns.RemoveAt(i);
                tblPanel.Columns.Add("Network");
                tblPanel.Columns.Add("NetworkDisplay");
                try
                {
                    DataView view = new DataView(table);
                    table.DefaultView.Sort = "[ProvGroupID] asc";
                    int SeqPdf = 1;
                    int seqIN_Pdf = 1;
                    DataTable newTable = table.DefaultView.ToTable();
                    string prevRec = "";
                    foreach (DataRow rowv in table.DefaultView.ToTable().Rows) // Loop over the rows.
                    {
                        if (prevRec != rowv["ProvGroupID"].ToString())
                        {
                            GRecnum++;
                            prevRec = rowv["ProvGroupID"].ToString();
                            var rowP = tblProviders.NewRow();
                            for (int j = 1; j < tblProviders.Columns.Count - 1; j++)
                            {
                                rowP[j] = rowv[j].ToString().TrimStart().TrimEnd();
                            }
                            rowP[0] = GRecnum;
                            rowP[17] = rowv[36].ToString().TrimStart().TrimEnd();       //rowP[16] = rowv[35].ToString().TrimStart().TrimEnd();

                            //rowP[6] = fileInfo.Name.Replace(".txt", "_") + SeqPdf.ToString("00") + "_" + stringResult + FileSeq.ToString("00");
                            tblProviders.Rows.Add(rowP);

                            if (seqIN_Pdf == 2000)
                            {
                                seqIN_Pdf = 1;
                                SeqPdf++;
                            }
                            else
                                seqIN_Pdf++;
                        }
                        //rowv["Recnum"] = GRecnum;
                        var rowM = tblPanel.NewRow();
                        rowM[0] = GRecnum;
                        rowM[1] = rowv[1];
                        rowM[2] = rowv[2];
                        rowM[3] = rowv[3];
                        rowM[4] = rowv[11];         // rowM[4] = rowv[10]
                        rowM[5] = rowv[17];         //rowM[5] = rowv[16];
                        rowM[6] = rowv[18];         //rowM[6] = rowv[17];
                        //rowM[28] = fileInfo.Name.Replace(".txt", "_") + SeqPdf.ToString("00") + "_" + stringResult + FileSeq.ToString("00");
                        int yy = 7;
                        for (int j = 19; j < 37; j++)           //int j = 18; j < 36; j++)
                        {
                            rowM[yy] = rowv[j];
                            yy++;
                        }
                        rowM[28] = rowv[9];
                        rowM[29] = (rowv[9].ToString() == "0700") ? "Medicaid" : "DSNP"; 
                        tblPanel.Rows.Add(rowM);
                    }
                }
                catch (Exception ex)
                {
                    var excep = ex.Message;
                }

                string errors = "";
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_HNJH_Panel_Roster_Provider_TMP");

                int updErrors = 0;
                SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {

                    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_HNJH_Panel_Roster_Provider_TMP]";

                    try
                    {
                        bulkCopy.WriteToServer(tblProviders);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;    //colid 27   Member Gender
                        updErrors++;
                    }
                }
                Connection.Close();



                if (updErrors == 0)
                {
                    dbU.ExecuteScalar("Insert into HOR_parse_HNJH_Panel_Roster_Provider select * from HOR_parse_HNJH_Panel_Roster_Provider_TMP");
                }

                errors = "";
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_HNJH_Panel_Roster_Panel_TMP");

                Connection.Open();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
                {
                    bulkCopy.DestinationTableName = "[dbo].[HOR_parse_HNJH_Panel_Roster_Panel_TMP]";

                    try
                    {
                        bulkCopy.WriteToServer(tblPanel);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + ex.Message;    //colid 27   Member Gender
                        updErrors++;
                    }
                }
                Connection.Close();

                if (updErrors == 0)
                {

                    dbU.ExecuteScalar("Insert into HOR_parse_HNJH_Panel_Roster_Panel select * from HOR_parse_HNJH_Panel_Roster_Panel_TMP");
                }

                //=======================




            }
            dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_HNJH_Panel_Roster_Provider', GETDATE())");

            return result;
        }
        public string evaluate_Roster(string fileName, string insertName, string directoryTXT, bool nozip, string insertBBB, bool testProcess, string LastWriteTime)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            createCSV createcsvT = new createCSV();
            int TotalRecs = 0;
            var records = dbU.ExecuteScalar("select count(recnum) from HOR_parse_HNJH_Panel_Roster_Provider where filename = '" + fileInfo.Name + "'");

            if (records.ToString() == "")
                TotalRecs = 0;
            else
                TotalRecs = Convert.ToInt32(records.ToString()) + 1;

            string pNameT = fileInfo.DirectoryName + "\\HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
            string BCCname = "HNJH-PR_" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + "_toBCC.csv";
            string directoryAfterCass = ProcessVars.InputDirectory + "FromCASS";



            dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,LettersProduced,sysout,jobid,Work_Task,DateProcess ) values(" +
                                             TotalRecs + ",'" + BCCname + "','" + fileInfo.Name + "','" + LastWriteTime + "','HOR_parse_HNJH_Panel_Roster_Provider','" +
                                             directoryAfterCass + "','','','','Receive','" + GlobalVar.DateofProcess + "')");


            //=======================
            DataTable table_BCC = dbU.ExecuteDataTable("SELECT Recnum, [ProviderGroupName],[ProviderGroupAddress1],[ProviderGroupAddress2], [ProviderGroupCity] + ', ' + [ProviderGroupState] + ' ' + [ProviderGroupZip] as CSZ FROM [BCBS_Horizon].[dbo].[HOR_parse_HNJH_Panel_Roster_Provider] " +
                                                        " where filename = '" + fileInfo.Name + "'");
            //CSV  data===================================================================
    
            if (File.Exists(pNameT))
                File.Delete(pNameT);

            var fieldnames = new List<string>();
            fieldnames.Add("Recnum");
            fieldnames.Add("F2"); fieldnames.Add("F3"); fieldnames.Add("F4"); fieldnames.Add("F5"); fieldnames.Add("F6"); fieldnames.Add("F7");
            fieldnames.Add("F8"); fieldnames.Add("F9"); fieldnames.Add("F10"); fieldnames.Add("F11"); fieldnames.Add("F12"); fieldnames.Add("F13");
            fieldnames.Add("F14"); fieldnames.Add("Addr1"); fieldnames.Add("Addr2"); fieldnames.Add("Addr3"); fieldnames.Add("Addr4"); fieldnames.Add("Addr5"); fieldnames.Add("Addr6");

            bool resp = createcsvT.addRecordsCSV(pNameT, fieldnames);
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
                        rowData.Add(""); rowData.Add(""); rowData.Add(row[index].ToString());
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

           
            
            return  "Evaluate Roster Done";
        }

        private List<List<Field>> ParseFile(string inputFile)
        {
            //Get the field mapping.
            List<Field> fields = GetFields();
            //Create a List<List<Field>> collection of collections.
            // The main collection contains our records, and the
            // sub collection contains the fields each one of our
            // records contains.
            List<List<Field>> records = new List<List<Field>>();

            //Open the flat file using a StreamReader.
            using (StreamReader reader = new StreamReader(inputFile))
            {
                //Load the first line of the file.
                string line = reader.ReadLine();

                //Loop through the file until there are no lines
                // left.
                try
                {
                    while (line != null)
                    {
                        //Create out record (field collection)
                        List<Field> record = new List<Field>();
                        if (line.Length < 533)
                        {
                            //Loop through the mapped fields
                            foreach (Field field in fields)
                            {
                                Field fileField = new Field();

                                if (field.Start.ToString() == "529" && line.Length > 530)
                                {
                                    int lenght = line.Length - 529;
                                    fileField.Value =
                                        line.Substring(field.Start, lenght);
                                }
                                else
                                {
                                    fileField.Value =
                                        line.Substring(field.Start, field.Length);
                                }
                                //Set the name of the field.
                                fileField.Name = field.Name;

                                //Add the field to our record.
                                record.Add(fileField);
                            }

                            //Add the record to our record collection
                            records.Add(record);
                        }
                        //Read the next line.
                        line = reader.ReadLine();
                    }
                }
                catch (Exception ex)
                {
                    var msg = ex.Message;
                }
            }

            //Return all of our records.
            return records;
        }
        private DataTable ParseFiletoTable(string inputFile, DataTable table, string part)
        {
            int toterrors = 0;
            List<Field> fields = GetFields();
            int Limit = 484082;
            ArrayList aList;
            int linenumber = 0;
            if (part == "2")
                Limit = 484082;
            using (StreamReader reader = new StreamReader(inputFile))
            {
                string line = reader.ReadLine();
                try
                {
                    while (line != null)
                    {
                        linenumber++;
                        if (part == "1")
                        {
                                aList = new ArrayList();
                                foreach (Field field in fields)
                                {
                                    if (field.Start.ToString() == "529" && line.Length > 530)
                                    {
                                        int lenght = line.Length - 529;
                                        aList.Add(line.Substring(field.Start, lenght));
                                    }
                                    else
                                    {
                                        if (field.Name == "MemberPhone")
                                        {
                                            string Pvalue = line.Substring(field.Start, field.Length);
                                            aList.Add(Regex.Replace(Pvalue.Trim(), @"(\d{3})(\d{3})(\d{4})", "($1) $2-$3"));
                                        }
                                        else
                                            aList.Add(line.Substring(field.Start, field.Length).Trim());
                                    }
                                }
                                if (table.Columns.Count == aList.Count)
                                    table.Rows.Add(aList.ToArray());
                                else
                                    toterrors++;
                        }
                        else
                        {
                            if (linenumber > Limit)
                            {
                                aList = new ArrayList();
                                foreach (Field field in fields)
                                {
                                    if (field.Start.ToString() == "529" && line.Length > 530)
                                    {
                                        int lenght = line.Length - 529;
                                        aList.Add(line.Substring(field.Start, lenght));
                                    }
                                    else
                                    {
                                        if (field.Name == "MemberPhone")
                                        {
                                            string Pvalue = line.Substring(field.Start, field.Length);
                                            aList.Add(Regex.Replace(Pvalue.Trim(), @"(\d{3})(\d{3})(\d{4})", "($1) $2-$3"));
                                        }
                                        else
                                            aList.Add(line.Substring(field.Start, field.Length).Trim());
                                    }
                                }
                                if (table.Columns.Count == aList.Count)
                                    table.Rows.Add(aList.ToArray());
                                else
                                    toterrors++;
                            }
                        }
                        //Read the next line.
                        line = reader.ReadLine();
                        if (part == "1" && linenumber == Limit)
                            break;
                    }
                }
                catch (Exception ex)
                {
                    var msg = ex.Message;
                }
            }

            //Return all of our records.
            return table;
        }
        private List<Field> GetFields()
        {
            List<Field> fields = new List<Field>();
            XmlDocument map = new XmlDocument();

            //Load the mapping file into the XmlDocument
            map.Load(ProcessVars.gmappingFileHNJH_Panel);

            //Load the field nodes.
            XmlNodeList fieldNodes = map.SelectNodes("/FileMap/Field");

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

    }
}
