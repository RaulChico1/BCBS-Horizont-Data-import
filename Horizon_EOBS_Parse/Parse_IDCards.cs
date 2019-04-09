using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Xml;
using System.Xml.Linq;
using System.Configuration;
using System.Data.SqlClient;
using ExcelLibrary;
using System.Text.RegularExpressions;





namespace Horizon_EOBS_Parse
{
    public class Parse_IDCards
    {
        DBUtility dbU;
        DBUtility dbu_169;
        string errors = "";
        int errorcount = 0;
        int Recnum = 1;
        long GRecnum = 1;
        int currLine = 0;
        int seqBundle = 0;
          DataTable DataTable = Data_Table();
          List<string> Codes = new List<string>();
          List<string> CovCodes = new List<string>();

          string strGroup, GroupBundle, Medicare, Source_M;


        public string ProcessFiles(string dateProcess)
        {
            ProcessVars.serviceIsrunning = true;
            //autoEvent.WaitOne(1000 * 60 * 3, false);
            string result = FilestoProcess_CON2(dateProcess, "");
             result = result + " " + FilestoProcess_GRP2(dateProcess, "");
            ProcessVars.serviceIsrunning = false;

            return result;
        }
        public string ProcessFilesinDir(string dateProcess, string LocalDirectory)
        {
            string result = "";
            if (Directory.Exists(LocalDirectory))
            {
                string[] subdirectoryEntries = Directory.GetDirectories(LocalDirectory);

                foreach (string subdirectory in subdirectoryEntries)
                {
                    if (subdirectory.IndexOf("\\_") == -1)
                    {
                        //check file name not saubdirectory
                        if (subdirectory.IndexOf("CON2") != -1 && subdirectory.IndexOf("_CON2") == -1)
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_CON2(dateProcess, subdirectory);

                        if (subdirectory.IndexOf("GRP2") != -1 && subdirectory.IndexOf("_GRP2") == -1)
                            if (Directory.Exists(subdirectory))
                                result = result + " " + FilestoProcess_GRP2(dateProcess, subdirectory);
                        if (subdirectory.IndexOf("Bed Bath and Beyond") != -1 && subdirectory.IndexOf("_Bed Bath and Beyond_") == -1)
                            if (Directory.Exists(subdirectory))
                                result = result + " " + FilestoProcess_BBB(dateProcess, subdirectory, true);
                        if (subdirectory.IndexOf("Heavy and General Laborers") != -1 && subdirectory.IndexOf("_Heavy and General Laborers_") == -1)
                            if (Directory.Exists(subdirectory))
                                result = result + " " + FilestoProcess_BBB(dateProcess, subdirectory, false);
                        //TEST AREA

                        if ((subdirectory.IndexOf("OMNIA_") != -1 && subdirectory.IndexOf("_") != 0 && subdirectory.IndexOf("GRP2") == -1) ||
                            (subdirectory.IndexOf("OMNIA_") != -1 && subdirectory.IndexOf("_") != 0 && subdirectory.IndexOf("CON2") == -1))
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_OMNIA(dateProcess, subdirectory);
                        //HOSHBP_
                        if (subdirectory.IndexOf("SHBP_") != -1 && subdirectory.IndexOf("_") != 0)
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_OMNIA(dateProcess, subdirectory);
                        if (subdirectory.IndexOf("Elaine_") != -1 && subdirectory.IndexOf("_") != 0)
                            if (Directory.Exists(subdirectory))
                                result = FilestoProcess_OMNIA(dateProcess, subdirectory);

                        //HNJHID

                        //if (subdirectory.IndexOf("HNJHID_") != -1 && subdirectory.IndexOf("_") != 0)
                        //    if (Directory.Exists(subdirectory))
                        //        result = FilestoProcess_HNJHID(dateProcess, subdirectory);




                    }
                }

                //ProcessVars.serviceIsrunning = true;
                ////autoEvent.WaitOne(1000 * 60 * 3, false);
                // result = FilestoProcess_CON2(dateProcess);
                //result = result + " " + FilestoProcess_GRP2(dateProcess);
                //ProcessVars.serviceIsrunning = false;
            }
            return result;
        }
        public string FilestoProcess(string dateProcess)        //not maintenance
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.NLDirectory))
            {
                string[] subdirectoryEntries = Directory.GetDirectories(ProcessVars.IDCardsDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\");
                string[] subdirectoryEntries2 = Directory.GetDirectories(subdirectoryEntries[0].ToString());
                foreach (string subdirectory in subdirectoryEntries2)
                {
                    DirectoryInfo originalDATs = new DirectoryInfo(subdirectory);
                    FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                    foreach (FileInfo file in FilesDAT)
                    {
                        DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        if (FilesPDF[0].ToString().ToUpper().IndexOf("PDF") != -1)
                        {

                            InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));

                        }
                        //FileInfo[] getPDF = 
                        if (file.Name.IndexOf("IM") == -1)
                        {
                            try
                            {
                                string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString(), false, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }

                    }
                }

            }
            return errors;
        }

       
		


        public string FilestoProcess_HNJHID(string dateProcess, string NJHIDCardsdirectory)        //not maintenance
        {
           // string InsertName = "";
            DataSet ds=null;
           if (Directory.Exists(NJHIDCardsdirectory))
            {
               
                string dirIDCards = NJHIDCardsdirectory;
               
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                //dELETE FROM TMP TABLE AND XMPIE TABLE And Temp TO KEEP ONLY THAT DAYS DATA .
                dbU.ExecuteScalar("Delete from HNJH_IDCards_Temp");
                dbU.ExecuteScalar("Delete from HNJH_IDCards_Xmpie");
                dbU.ExecuteScalar("Delete from HNJH_IDCards_Xmpie_MLTSS");
                
                DirectoryInfo NetworkNjhidCardDirectory = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesTxtNetwork = NetworkNjhidCardDirectory.GetFiles("*.Txt");

                foreach (FileInfo file in FilesTxtNetwork)
                {
                    try
                    {


                     //  string errors = evaluate_HNJHIDCards(file.FullName, NJHIDCardsdirectory);
                        string errors = evaluate_HNJHIDCards(file.FullName, NJHIDCardsdirectory);
                        
                    }
                    catch (Exception ez)
                    {
                        errors = errors + file + "  " + ez.Message + "\n\n";
                    }

                }

                
              


              // //Need to move  the incoming files in the incoming folder to processedfolder  
                
                String ProcessedDirectory = NJHIDCardsdirectory + "Processed";
                DirectoryInfo dirInfo = new DirectoryInfo(ProcessedDirectory);
                if (dirInfo.Exists == false)
                    Directory.CreateDirectory(ProcessedDirectory);

                List<String> IncomingTxtFiles = Directory
                                   .GetFiles(NJHIDCardsdirectory, "*.txt").ToList();

                foreach (string file in IncomingTxtFiles)
                {
                    FileInfo mFile = new FileInfo(file);
                    // to remove name collission
                    if (new FileInfo(dirInfo + "\\" + mFile.Name).Exists == false)
                        mFile.MoveTo(dirInfo + "\\" + mFile.Name);
                }

               
              }
              return "";
        }

   



               




          

        public string FilestoProcess_OMNIA(string dateProcess, string subdirectory)        //not maintenance
        {
            string InsertName = "";

            //if (Directory.Exists(ProcessVars.InputDirectory))
            if (Directory.Exists(subdirectory))
            {
                //string dirIDCards = ProcessVars.InputDirectory  + @"from_FTP\Con2\";
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");

                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {
                        //DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        DirectoryInfo originalPDFs = new DirectoryInfo(subdirectory + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");

                        foreach (FileInfo file2 in FilesPDF)
                        {
                            if (file2.ToString().ToUpper().Replace(".PDF", "").IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (file2.ToString().IndexOf("_") > 0 && file2.ToString().IndexOf("_") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf("_"));

                                else if (file2.ToString().IndexOf(" ") > 0 && file2.ToString().IndexOf(" ") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf(" "));

                                //if (FilesPDF[0].ToString().IndexOf(" ") > 0 && FilesPDF[0].ToString().IndexOf(" ") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));
                                //if (FilesPDF[0].ToString().IndexOf("_") > 0 && FilesPDF[0].ToString().IndexOf("_") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf("_"));
                            }
                        }
                        //FileInfo[] getPDF = 

                        try
                        {
                            //string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString());
                            string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, "", false);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                }

                else
                {
                    var errorDAT = "More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();

                }

            }
            return errors;
        }








        public string FilestoProcess_CON2(string dateProcess, string subdirectory)        //not maintenance
        {
            string InsertName = "";

            //if (Directory.Exists(ProcessVars.InputDirectory))
            if (Directory.Exists(subdirectory))
            {
                //string dirIDCards = ProcessVars.InputDirectory  + @"from_FTP\Con2\";
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {
                        //DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        //=====================================================================================

                        DirectoryInfo originalPDFs = new DirectoryInfo(subdirectory + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        foreach (FileInfo file2 in FilesPDF)
                        {
                            if (file2.ToString().ToUpper().Replace(".PDF", "").IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (file2.ToString().IndexOf("_") > 0 && file2.ToString().IndexOf("_") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf("_"));

                                else if (file2.ToString().IndexOf(" ") > 0 && file2.ToString().IndexOf(" ") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf(" "));
                            }

                        }
                        //FileInfo[] getPDF = 
                        if (file.Name.IndexOf("CON2") == 0)
                        {
                            try
                            {
                                //string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString());
                                string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                    }

                }
                else
                {
                    var errorDAT = "More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();

                }

            }
            return errors;
        }
        public string FilestoProcess_GRP2(string dateProcess, string subdirectory)        //not maintenance
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.InputDirectory))
            {
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {


                        DirectoryInfo originalPDFs = new DirectoryInfo(subdirectory + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        foreach (FileInfo file2 in FilesPDF)
                        {

                            if (file2.ToString().ToUpper().Replace(".PDF", "").IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (file2.ToString().IndexOf("_") > 0 && file2.ToString().IndexOf("_") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf("_"));
                                else if (file2.ToString().IndexOf(" ") > 0 && file2.ToString().IndexOf(" ") < (file2.ToString().Length - 3))
                                    InsertName = file2.ToString().Substring(0, file2.ToString().IndexOf(" "));
                                //if (FilesPDF[0].ToString().IndexOf("_") > 0 && FilesPDF[0].ToString().IndexOf("_") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf("_"));
                                //else if (FilesPDF[0].ToString().IndexOf(" ") > 0 && FilesPDF[0].ToString().IndexOf(" ") < (FilesPDF[0].ToString().Length - 3))
                                //    InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));
                            }
                        }
                        if (file.Name.IndexOf("GRP2") == 0)
                        {
                            try
                            {
                                seqBundle = 0;
                                string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }
                    }
                }
                else
                {
                    var errorDAT = "More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();
                }
            }
            return errors;
        }

        public string FilestoProcess_BBB(string dateProcess, string subdirectory, bool isBBB)        //Bed Bath and Beyond
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.InputDirectory))
            {
                string dirIDCards = subdirectory;

                DirectoryInfo originalDATs = new DirectoryInfo(dirIDCards);
                FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");

                if (FilesDAT.Count() == 1)
                {
                    foreach (FileInfo file in FilesDAT)
                    {
                        string insertBBBLocation = "";
                        if (isBBB)
                        {
                            DirectoryInfo originalPDFs = new DirectoryInfo(ProcessVars.oInsertBBB);
                            FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                            foreach (FileInfo file2 in FilesPDF)
                            {
                                InsertName = file2.Name.ToString().Substring(0, file2.Name.ToString().IndexOf("_"));
                                insertBBBLocation = file2.FullName;
                            }
                        }
                        try
                        {
                            seqBundle = 0;
                            string error = evaluate_IDCards(file.FullName, InsertName, subdirectory, false, insertBBBLocation, false);
                            if (error != "")
                                errors = errors + error + "\n\n";
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                }

                else
                {
                    var errorDAT = "More that 1 DAT file in " + subdirectory + "  count:" + FilesDAT.Count().ToString();

                }

            }
            return errors;
        }

        public string MaintenanceFilestoProcess(string dateProcess)   //maintenance
        {
            string InsertName = "";

            if (Directory.Exists(ProcessVars.NLDirectory))
            {
                string dirIDCards = ProcessVars.IDCardsMDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                string[] subdirectoryEntries = Directory.GetDirectories(dirIDCards);
                //string[] subdirectoryEntries2 = Directory.GetDirectories(subdirectoryEntries[0].ToString());
                foreach (string subdirectory in subdirectoryEntries)
                {
                    DirectoryInfo originalDATs = new DirectoryInfo(subdirectory);
                    FileInfo[] FilesDAT = originalDATs.GetFiles("*.DAT");
                    foreach (FileInfo file in FilesDAT)
                    {
                        DirectoryInfo originalPDFs = new DirectoryInfo(file.Directory.ToString() + @"\");
                        FileInfo[] FilesPDF = originalPDFs.GetFiles("*.PDF");
                        foreach (FileInfo file2 in FilesPDF)
                        {
                            if (file2.ToString().ToUpper().IndexOf(file.Name.ToString().ToUpper().Replace(".DAT", "")) == -1)  // was -1 ???
                            {
                                if (FilesPDF[0].ToString().ToUpper().IndexOf("PDF") != -1)
                                {
                                    if (FilesPDF[0].ToString().IndexOf(" ") > 0 && FilesPDF[0].ToString().IndexOf(" ") < (FilesPDF[0].ToString().Length - 3))
                                        InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf(" "));
                                    if (FilesPDF[0].ToString().IndexOf("_") > 0 && FilesPDF[0].ToString().IndexOf("_") < (FilesPDF[0].ToString().Length - 3))
                                        InsertName = FilesPDF[0].ToString().Substring(0, FilesPDF[0].ToString().IndexOf("_"));
                                }
                            }
                        }
                        //FileInfo[] getPDF = 
                        if (file.Name.IndexOf("IM") == -1)
                        {
                            try
                            {
                                string error = evaluate_IDCards(file.FullName, InsertName, file.Directory.ToString(), true, "", false);
                                if (error != "")
                                    errors = errors + error + "\n\n";

                                InsertName = "";
                            }
                            catch (Exception ez)
                            {
                                errors = errors + file + "  " + ez.Message + "\n\n";
                            }
                        }

                    }
                }

            }
            return errors;
        }

        public string evaluate_HNJHIDCards(string fileName, string directoryTXT)
        {
            
            DataSet ds = null;
            dbU = ProcessVars.oDBUtility();
         //   dbu_169 = ProcessVars.oDBUtility_169();
            Recnum = 1;
            string x = Path.GetFileName(fileName);
            string y = Path.GetDirectoryName(fileName);
            string z = Path.GetFileNameWithoutExtension(fileName);
            DateTime dt = DateTime.Now;
            string s = dt.ToString("yyyyMMddHHmmss");
            DataTable NJHMemoryTable;
            int errors = 0;


            
            


                string filename = Path.GetFileName(fileName);
                DataTable = new System.Data.DataTable("NJHMemoryTable");
          

              ds = TextToDataSet.Convert(fileName, "NJHMemoryTable", "\t");
          
           
            if (ds!=null && ds.Tables[0].Rows.Count>0)
            {
               int countforrows= ds.Tables[0].Rows.Count;
                var recnum = dbU.ExecuteScalar("SELECT MAX(RECNUM) FROM [BCBS_Horizon].[dbo].[HOR_parse_SEQ]");
             //  var recnum = dbu_169.ExecuteScalar("SELECT MAX(RECNUM) FROM [BCBS_Horizon].[dbo].[HOR_parse_SEQ]");

                int recordnumber = 0;
                if (recnum.ToString() == "")

                    GRecnum = 1;
                else
                    GRecnum = Convert.ToInt64(recnum.ToString()) + 1;

                NJHMemoryTable = ds.Tables[0];
                NJHMemoryTable.Columns.Add("RECNUM");
                NJHMemoryTable.Columns.Add("TIMESTAMP");
                NJHMemoryTable.Columns.Add("FILEDATE");
                NJHMemoryTable.Columns.Add("INSERT_PREV");
                NJHMemoryTable.Columns.Add("INSERT");
                NJHMemoryTable.Columns.Add("MEM_LASTNAME");
                NJHMemoryTable.Columns.Add("MEM_FIRSTNAME");
                NJHMemoryTable.Columns.Add("FORM_ID");
                NJHMemoryTable.Columns.Add("FILENAME");
                NJHMemoryTable.Columns.Add("GROUPID");
                NJHMemoryTable.Columns.Add("VARCODE");
                NJHMemoryTable.Columns.Add("GROUP NAME SHORT");
                NJHMemoryTable.Columns.Add("INSURED FNAME");
                NJHMemoryTable.Columns.Add("INSURED LNAME");
                NJHMemoryTable.Columns.Add("INSURED SUFFIX");
                NJHMemoryTable.Columns.Add("INSURED INITIALS");
                NJHMemoryTable.Columns.Add("ADMIN NAME");
                NJHMemoryTable.Columns.Add("ADMIN PRE");
                NJHMemoryTable.Columns.Add("STREET ADDRESS");
                NJHMemoryTable.Columns.Add("CITY");
                NJHMemoryTable.Columns.Add("STATE");
                NJHMemoryTable.Columns.Add("ZIP+4");
                NJHMemoryTable.Columns.Add("GROUP#_PRE");
                NJHMemoryTable.Columns.Add("GROUP#_POST");

                NJHMemoryTable.Columns.Add("GROUP NAME LONG");
                NJHMemoryTable.Columns.Add("MEMBER GRP PREFIX");
                NJHMemoryTable.Columns.Add("PLAN TYPE");
                NJHMemoryTable.Columns.Add("EFFECTIVE DATE");
                NJHMemoryTable.Columns.Add("PLAN CODES");
                NJHMemoryTable.Columns.Add("MEMBER ID");
                NJHMemoryTable.Columns.Add("GRP BUNDLE");
                NJHMemoryTable.Columns.Add("DL");
                NJHMemoryTable.Columns.Add("MED_FLAG");
                NJHMemoryTable.Columns.Add("TYPE");
                NJHMemoryTable.Columns.Add("IMB");
                NJHMemoryTable.Columns.Add("RECIEVESTATUS");
                NJHMemoryTable.Columns.Add("Supress");



                FileInfo fileInfo = new System.IO.FileInfo(fileName);
                DateTime lastWriteTime = File.GetLastWriteTime(fileName);
                int SeqNoMLTSS = 1;
                int SeqNoNjFamily = 1;
                string FileDate = lastWriteTime.ToString("MM/dd/yyyy");
                string Filex = z + "_ID_" + s;

                //Data Cleansing in inmemory Data Table
                Regex rgx = new Regex(@"^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$");
                foreach (DataRow row in NJHMemoryTable.Rows) // Loop over the rows.
                {

                    try
                    {

                        
                        if (row["PCP_PHONE_NO"].ToString() != "" && row["PCP_PHONE_NO"] != null && !rgx.IsMatch(row["PCP_PHONE_NO"].ToString()))
                            
                        {
                            string fieldval = row["PCP_PHONE_NO"].ToString().Replace("-","");
                            string PhoneNoWithDash = GetResultsWithHyphen(fieldval.Trim());
                            row["PCP_PHONE_NO"] = PhoneNoWithDash;
                        }

                        row["RECNUM"] = GRecnum;
                        GRecnum++;

                        row["FILENAME"] = Filex.Trim();
                        row["TIMESTAMP"] = s.Trim();
                        row["FILEDATE"] = FileDate.Trim();
                        if (row["MEMBER_NAME"].ToString().Contains("\"") && row["MEMBER_NAME"].ToString() != null && row["MEMBER_NAME"].ToString()!="")
                        {
                            string memberName = row["MEMBER_NAME"].ToString().Replace("\"", "");
                            row["MEMBER_NAME"] = memberName.Trim();

                        }

                        if (row["PCP_NAME"].ToString() != null && row["PCP_NAME"].ToString() != "" && row["PCP_NAME"].ToString().Contains("\""))
                        {
                            string pcpname = row["PCP_NAME"].ToString().Replace("\"", "");
                            row["PCP_NAME"] = pcpname.Trim();


                        }

                       


                        if (row["MEMBER_NAME"].ToString() != null && row["MEMBER_NAME"].ToString().IndexOf(',') >= 0 )
                        {

                            string[] MemberNameSplit = row["MEMBER_NAME"].ToString().Split(',');
                            string MemberLastName = MemberNameSplit[0].ToString().Replace("\"", "").Trim();
                            string MemberFirstName = MemberNameSplit[1].ToString().Replace("\"", "").Trim();
                            row["MEM_LASTNAME"] = MemberLastName;
                            row["MEM_FIRSTNAME"] = MemberFirstName;
                        }

                        //if (row["EMERGENCY_AMT"].ToString() != "" && row["EMERGENCY_AMT"].ToString() != null)
                        //{
                        //    string stemergencyamt = row["EMERGENCY_AMT"].ToString().Trim();
                        //    decimal emergencyamtdouble = Math.Round(Convert.ToDecimal(stemergencyamt));
                        //    row["EMERGENCY_AMT"] = emergencyamtdouble.ToString().Trim();

                        //}

                        //if (row["PCP_COPAY"].ToString() != "" && row["PCP_COPAY"].ToString() != null)
                        //{
                        //    string pcpcopayamt = row["PCP_COPAY"].ToString().Trim();
                        //    decimal pcpcopayamtdouble = Math.Round(Convert.ToDecimal(pcpcopayamt));
                        //    row["PCP_COPAY"] = pcpcopayamtdouble.ToString().Trim();


                        //}
                        if (row["DENTAL_BENFIT"].ToString() != "" && row["DENTAL_COPAY"].ToString() != null)
                        {
                            string dentalbenfit = row["DENTAL_BENFIT"].ToString().Trim();
                            if (dentalbenfit.ToUpper()=="NO")
                            {
                                row["DENTAL_COPAY"] = "N/A";

                            }
                           
                        }

                        //if (row["DENTAL_COPAY"].ToString() != "" && row["DENTAL_COPAY"].ToString() != null && row["DENTAL_COPAY"].ToString() != "N/A")
                        //{
                        //    string dentalcopayamt = row["DENTAL_COPAY"].ToString().Trim();
                        //    decimal dentalcopayamtdouble = Math.Round(Convert.ToDecimal(dentalcopayamt));
                        //    row["DENTAL_COPAY"] = dentalcopayamtdouble.ToString().Trim();
                        //}

                        //if (row["SPECIALIST_COPAY"].ToString() != "" && row["SPECIALIST_COPAY"].ToString() != null)
                        //{
                        //    string specialistcopayamt = row["SPECIALIST_COPAY"].ToString().Trim();
                        //    decimal specialistcopayamtdouble = Math.Round(Convert.ToDecimal(specialistcopayamt));
                        //    row["SPECIALIST_COPAY"] = specialistcopayamtdouble.ToString().Trim();

                        //}

                        //if (row["RX_GENERIC"].ToString() != "" && row["RX_GENERIC"].ToString() != null)
                        //{

                        //    string rxgenericamt = row["RX_GENERIC"].ToString().Trim();
                        //    decimal rxgenericamtdouble = Math.Round(Convert.ToDecimal(rxgenericamt));
                        //    row["RX_GENERIC"] = rxgenericamtdouble.ToString().Trim();
                        //}

                        //if (row["RX_BRAND"].ToString() != "" && row["RX_BRAND"].ToString() != null)
                        //{

                        //    string rxbrandamt = row["RX_BRAND"].ToString().Trim();
                        //    decimal rxbrandamtdouble = Math.Round(Convert.ToDecimal(rxbrandamt));
                        //    row["RX_BRAND"] = rxbrandamtdouble.ToString().Trim();

                        //}
                        try
                        {
                            if (row["MEME_PLAN"].ToString() != null && row["MEME_PLAN"].ToString() != "")
                            {
                                string MemberPlan = row["MEME_PLAN"].ToString().Trim();
                                if (MemberPlan.IndexOf('A') != -1 || MemberPlan.IndexOf('B') != -1 || MemberPlan.IndexOf('D') != -1 || MemberPlan.IndexOf('C') != -1 || MemberPlan.IndexOf("ABP") != -1)//that is MLTSS
                                {
                                    row["FORM_ID"] = "FC100";

                                }
                                else
                                {
                                    row["FORM_ID"] = "ML100";

                                }
                            }



                        }


                        catch (Exception EX)
                        {
                            errors = errors + 1;

                        }


                        if (row["CARD_IND"].ToString() != null && row["CARD_IND"].ToString() != "" && row["MEME_PLAN"] != null && row["MEME_PLAN"] != "")
                        {
                            string CardIndicator = (row["CARD_IND"].ToString()).Trim();
                            string Meme_plan = (row["MEME_PLAN"].ToString()).Trim();
                            if ((CardIndicator == "N" || CardIndicator == "C") && (Meme_plan == "MLTSS"))

                                row["INSERT"] = "HNJH_MLTSS_N";

                            else if ((CardIndicator == "R" || CardIndicator == "D") && (Meme_plan == "MLTSS"))

                                row["INSERT"] = "HNJH_MLTSS_R";



                            else if ((CardIndicator == "N" || CardIndicator == "C") && (Meme_plan != "MLTSS"))

                                row["INSERT"] = "HNJH_FC_N";


                            else if ((CardIndicator == "R" || CardIndicator == "D") && (Meme_plan != "MLTSS"))

                                row["INSERT"] = "HNJH_FC_R";


                        }


                        if (row["MEME_MEDCD_NO"].ToString() != null && row["MEME_MEDCD_NO"].ToString() != "")
                        {


                            row["INSERT_PREV"] = "";
                            //GlobalVar.dbaseName = "BCBS_Horizon";
                            //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

                            //string medcdno = row["MEME_MEDCD_NO"].ToString();
                            //// SqlParameter[] sqlmedcdno = new SqlParameter[]
                            //// {                   
                            ////   DBUtility.GetInParameter("@MemMdCdNo",medcdno),                                                     
                            ////};

                            ////  string Sql1 = "SELECT count(*) FROM HNJH_IDCards WHERE Meme_Medcd_No=substring(@MemMdCdNo,0,10) ";

                            //int exists1 = Convert.ToInt32(dbU.ExecuteScalar("SELECT count(*) FROM [HNJH_IDCards_Temp] WHERE Meme_Medcd_No='" + medcdno + "'"));
                            //if (exists1 == 1)
                            //{
                            //    row["INSERT_PREV"] = "Y";
                            //}

                            //else
                            //{
                            //    row["INSERT_PREV"] = "";
                            //}



                        }


                        /////When supression file is there uncomment this part////
                        //try
                        //{
                        //    if (row["MEME_ID"].ToString() != "" && row["MEME_ID"].ToString() != null)
                        //    {
                        //        GlobalVar.dbaseName = "BCBS_Horizon";
                        //        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                        //        string memeid = row["MEME_ID"].ToString().Trim();


                        //        int FileCount = Convert.ToInt32(dbU.ExecuteScalar("SELECT count(*) FROM [HJNH_ID_ISSUETemp] WHERE MEME_ID='" + memeid + "'"));

                        //        if (FileCount == 1)
                        //        {
                        //            row["Supress"] = "Y";
                        //        }



                        //    }
                        //}
                        //catch (Exception EX)
                        //{
                        //    errors = errors + 1;

                        //}







                      
                       

                        try
                        {
                            if (row["FORM_ID"].ToString() != "" && row["FORM_ID"].ToString() != null)
                            {
                                row["GROUPID"] = row["FORM_ID"];

                            }
                        }
                        catch (Exception EX)
                        {
                            errors = errors + 1;

                        }
                        row["VARCODE"] = "";
                        row["GROUP NAME SHORT"] = "";

                        if (row["MEM_FIRSTNAME"].ToString() != "" && row["MEM_FIRSTNAME"] != null)
                        {
                            row["INSURED FNAME"] = row["MEM_FIRSTNAME"];


                        }

                        if (row["MEM_LASTNAME"].ToString() != "" && row["MEM_LASTNAME"] != null)
                        {
                            row["INSURED LNAME"] = row["MEM_LASTNAME"];

                        }

                        row["INSURED SUFFIX"] = DBNull.Value;
                        row["INSURED INITIALS"] = DBNull.Value;
                        if (row["MEME_ADDR1"].ToString() != "" && row["MEME_ADDR1"] != null)
                        {
                            row["ADMIN NAME"] = row["MEME_ADDR1"];

                        }

                        if (row["MEME_ADDR2"].ToString() != "" && row["MEME_ADDR2"] != null)
                        {
                            row["ADMIN PRE"] = row["MEME_ADDR2"];

                        }
                        else if (row["MEME_ADDR3"].ToString() != "" && row["MEME_ADDR3"] != null)
                        {
                            row["ADMIN PRE"] = row["MEME_ADDR3"];
                        }


                        if (row["MEME_ADDR1"].ToString() != "" && row["MEME_ADDR1"] != null)
                        {
                            row["STREET ADDRESS"] = row["MEME_ADDR1"];
                        }
                        else if (row["MEME_ADDR2"].ToString() != "" && row["MEME_ADDR2"] != null)
                        {
                            row["STREET ADDRESS"] = row["MEME_ADDR2"];
                        }

                        if (row["MEME_CITY"].ToString() != "" && row["MEME_CITY"] != null)
                        {
                            row["CITY"] = row["MEME_CITY"];
                        }

                        if (row["MEME_STATE"].ToString() != "" && row["MEME_STATE"] != null)
                        {
                            row["STATE"] = row["MEME_STATE"];
                        }

                        if (row["MEME_ZIP"].ToString() != "" && row["MEME_ZIP"] != null)
                        {
                            row["ZIP+4"] = row["MEME_ZIP"];
                        }


                        row["GROUP#_PRE"] = DBNull.Value;
                        row["GROUP#_POST"] = DBNull.Value;
                        row["GROUP NAME LONG"] = DBNull.Value;
                        row["MEMBER GRP PREFIX"] = DBNull.Value;


                        if (row["MEME_PLAN"].ToString() != "" && row["MEME_PLAN"] != null)
                        {
                            row["PLAN TYPE"] = row["MEME_PLAN"];
                        }


                        if (row["MEME_PLAN_EFF_DT"].ToString() != "" && row["MEME_PLAN_EFF_DT"] != null)
                        {
                            row["EFFECTIVE DATE"] = row["MEME_PLAN_EFF_DT"];
                        }

                        row["PLAN CODES"] = "";

                        if (row["MEME_MEDCD_NO"].ToString() != "" && row["MEME_MEDCD_NO"] != null)
                        {
                            row["MEMBER ID"] = row["MEME_MEDCD_NO"];
                        }
                        row["GRP BUNDLE"] = DBNull.Value;
                        row["DL"] = 'Y';
                        row["MED_FLAG"] = "";
                        row["type"] = DBNull.Value;
                        row["Supress"] = DBNull.Value;
                        row["RECIEVESTATUS"] = "Recieve";
                    }
                    catch (Exception ex)
                    {
                        var msg = ex.InnerException;
                        errors = errors + 1;


                    }

                }

           

                var lastRow = NJHMemoryTable.Rows[NJHMemoryTable.Rows.Count - 1];
                NJHMemoryTable.Rows.Remove(lastRow);
               NJHMemoryTable.AcceptChanges();



                dbU = ProcessVars.oDBUtility();
                using (SqlConnection cn = new SqlConnection(ProcessVars.ConnectionString))
                {

                    cn.Open();
                    using (SqlBulkCopy copy = new SqlBulkCopy(cn))
                    {
                        try
                        {
                            copy.DestinationTableName = "HNJH_IDCards_Temp";

                            copy.ColumnMappings.Add("RECNUM", "Recnum");
                            copy.ColumnMappings.Add("FILENAME", "FileName");
                            copy.ColumnMappings.Add("FILEDATE", "FileDate");
                            copy.ColumnMappings.Add("TIMESTAMP", "Timestamp");
                            // copy.ColumnMappings.Add("SEQ#", "Seq#");
                            copy.ColumnMappings.Add("MEME_ID", "Meme_ID");
                            copy.ColumnMappings.Add("MEMBER_NAME", "Member_Name");
                            copy.ColumnMappings.Add("MEM_LASTNAME", "Meme_LastName");
                            copy.ColumnMappings.Add("MEM_FIRSTNAME", "Meme_FirstName");
                            copy.ColumnMappings.Add("PCP_NAME", "PCP_Name");
                            copy.ColumnMappings.Add("PCP_PHONE_NO", "PCP_Phone_No");
                            copy.ColumnMappings.Add("MEME_MEDCD_NO", "Meme_Medcd_No");
                            copy.ColumnMappings.Add("MEME_ADDR1", "Meme_Addr1");
                            copy.ColumnMappings.Add("MEME_ADDR2", "Meme_Addr2");
                            copy.ColumnMappings.Add("MEME_ADDR3", "Meme_Addr3");
                            copy.ColumnMappings.Add("MEME_CITY", "Meme_City");
                            copy.ColumnMappings.Add("MEME_STATE", "Meme_State");
                            copy.ColumnMappings.Add("MEME_ZIP", "Meme_Zip");
                            copy.ColumnMappings.Add("MEME_PLAN", "Meme_Plan");
                            copy.ColumnMappings.Add("MEME_PLAN_EFF_DT", "Meme_Plan_Eff_Dt");
                            copy.ColumnMappings.Add("DENTAL_BENFIT", "Dental_Benefit");
                            copy.ColumnMappings.Add("EMERGENCY_AMT", "Emergency_Amt");
                            copy.ColumnMappings.Add("PCP_COPAY", "Pcp_CoPay");
                            copy.ColumnMappings.Add("DENTAL_COPAY", "Dental_CoPay");
                            copy.ColumnMappings.Add("SPECIALIST_COPAY", "Specialist_CoPay");
                            copy.ColumnMappings.Add("RX_GENERIC", "Rx_Generic");
                            copy.ColumnMappings.Add("RX_BRAND", "Rx_Brand");
                            copy.ColumnMappings.Add("Source_ID_CARD_REQ", "Source_Id_Card_Req");
                            copy.ColumnMappings.Add("CARD_IND", "Card_Ind");
                            copy.ColumnMappings.Add("INSERT_PREV", "Insert_Prev");
                            copy.ColumnMappings.Add("INSERT", "Insert");
                            copy.ColumnMappings.Add("FORM_ID", "Form_Id");

                            copy.ColumnMappings.Add("VARCODE", "varcode");
                            copy.ColumnMappings.Add("GROUP NAME SHORT", "group name short");
                            copy.ColumnMappings.Add("INSURED FNAME", "insured fname");
                            copy.ColumnMappings.Add("INSURED LNAME", "insured lname");
                            copy.ColumnMappings.Add("INSURED SUFFIX", "insured suffix");
                            copy.ColumnMappings.Add("INSURED INITIALS", "insured initials");
                            copy.ColumnMappings.Add("ADMIN NAME", "admin name");
                            copy.ColumnMappings.Add("ADMIN PRE", "admin pre");
                            copy.ColumnMappings.Add("STREET ADDRESS", "street address");
                            copy.ColumnMappings.Add("CITY", "city");
                            copy.ColumnMappings.Add("STATE", "state");
                            copy.ColumnMappings.Add("ZIP+4", "zip+4");
                            copy.ColumnMappings.Add("GROUP#_PRE", "group#_pre");
                            copy.ColumnMappings.Add("GROUP#_POST", "group#_post");
                            copy.ColumnMappings.Add("GROUP NAME LONG", "group name long");
                            copy.ColumnMappings.Add("MEMBER GRP PREFIX", "member grp prefix");
                            copy.ColumnMappings.Add("PLAN TYPE", "plan type");
                            copy.ColumnMappings.Add("EFFECTIVE DATE", "effective date");
                            copy.ColumnMappings.Add("PLAN CODES", "plan codes");
                            copy.ColumnMappings.Add("GRP BUNDLE", "grp bundle");
                            copy.ColumnMappings.Add("DL", "dl");
                            copy.ColumnMappings.Add("MED_FLAG", "med_flag");
                            copy.ColumnMappings.Add("TYPE", "type");
                            copy.ColumnMappings.Add("RECIEVESTATUS", "RecieveStatus");
                            copy.ColumnMappings.Add("Supress", "Supress");
                            copy.WriteToServer(NJHMemoryTable);





                        }
                        catch (Exception ex)
                        {
                            errors = errors + 1;


                        }
                    }


                }



                SqlParameter[] sqlParamsLoadedFileName = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };

                try
                {

                    DataSet hnj_idcardsDs = dbU.ExecuteDataSet1("CleanHNJH_IDCards_Temp", sqlParamsLoadedFileName);

                }

                catch (Exception ex)
                { errors = errors + 1; }


                //// //create csv file to bcc machine

                SqlParameter[] sqlParamsToBCC = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };

                string BCCname = "HNJH-ID_" + z + "_toBCC.csv";
                string pNameT = fileInfo.DirectoryName + "\\" + BCCname;
                if (File.Exists(pNameT))
                    File.Delete(pNameT);
                DataSet hnj_ToBcc = dbU.ExecuteDataSet("HNJH_CreateTableForBcc", sqlParamsToBCC);
                if (hnj_ToBcc.Tables[0].Rows.Count > 0)
                {
                    createCSV createcsvT = new createCSV();

                    var fieldnamesT = new List<string>();
                    for (int index = 0; index < hnj_ToBcc.Tables[0].Columns.Count; index++)
                    {
                        fieldnamesT.Add(hnj_ToBcc.Tables[0].Columns[index].ColumnName);
                    }

                    bool respT2 = createcsvT.addRecordsCSV(pNameT, fieldnamesT);
                    foreach (DataRow row in hnj_ToBcc.Tables[0].Rows)
                    {

                        var rowData = new List<string>();
                        for (int index = 0; index < hnj_ToBcc.Tables[0].Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());

                        }
                        respT2 = false;
                        respT2 = createcsvT.addRecordsCSV(pNameT, rowData);


                    }

                }

                //// copy to CASS

                string cassFileName = ProcessVars.gDMPs + BCCname;
                File.Copy(pNameT, cassFileName);

                // // // wait foR 3 min
                var t = Task.Run(async delegate
               {
                   await Task.Delay(1000 * 60 * 2);
                   return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
               });
                t.Wait();

                string filebcc = ProcessVars.gHNJHODMPs + BCCname.Replace("_toBCC.csv", "_toBCC-OUTPUT.csv").Trim();
                if (!File.Exists(filebcc))
                    {
                    string result2 = "Not BCC File check with Steve";
                    }
                BackCASS processReturns = new BackCASS();
                string result = processReturns.HNJHProcessFiles(BCCname);
              
                
               // string result = "";
               
                //After bcc check is done 

                if (result == "")
                {
                    ///create seq# now


                    try
                    {   string sqlcommand="HNJH_CreateSeqNo";
                    dbU.ExecuteScalar(sqlcommand);
                    }
                    catch (Exception ex)
                      { errors = errors + 1; }




                    


                  //  move data from temptable to master table.

                    try
                    {

                        SqlParameter[] sqlParamsLoadedFileName5 = new SqlParameter[]
                       {                   
                      DBUtility.GetInParameter("@FileName",Filex),
                        };
                     DataSet hnj_MasterDSet = dbU.ExecuteDataSet1("HNJH_MoveDataToMaster", sqlParamsLoadedFileName5);
                      

                    }
                    catch (Exception ex)
                    {
                        errors = errors + 1;
                    }

                    //MOVE ONLY DELEVERABLE TO XMPIE TABLEs-(hnjhid_xmpie and hnjh_mltss)


                    try
                    {
                        SqlParameter[] sqlParamsLoadedFileName2 = new SqlParameter[]
                    {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                    };


                        
                       DataSet hnj_XmpieDSet = dbU.ExecuteDataSet("HNJH_MoveDataToXmpie", sqlParamsLoadedFileName2);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + 1;
                    }




                    //creating csv file for njFamilyPlan 


                    try
                    {

                        SqlParameter[] sqlParamsLoadedFileName3 = new SqlParameter[]
                    {                   

                        DBUtility.GetInParameter("@FileName",Filex), 


                    };


                        DataSet hnj_NJFamily = dbU.ExecuteDataSet("HNJH_GetNJFamily", sqlParamsLoadedFileName3);

                        DataTable dtable1 = hnj_NJFamily.Tables[0];
                        int tablecount = hnj_NJFamily.Tables[0].Rows.Count;

                        //HNJHID031016_ID-CD_DSNP_DateTimeStamp.csv
                        string pNameT1 = fileInfo.DirectoryName + "\\";
                        if (hnj_NJFamily.Tables[0].Rows.Count > 0)
                        {



                            List<DataTable> dttableaftersplit = new List<DataTable>();
                            dttableaftersplit = SplitTableNj(dtable1, 10000);
                            DateTime begintime = DateTime.Now;
                            int k = 1;
                            int yz = 1;
                            createCSV createcsv = new createCSV();
                            foreach (var dtable in dttableaftersplit)
                            {
                                //HNJHID121616_ID-CD_DSNP_20161216174704 change to HNJHID121616_ID_DSNP_20161216174704_1.csv 
                                string stringfilepath = pNameT1 + z + "_ID_" + begintime.ToString("yyyyMMddHHmmss")+yz.ToString()+ "_" + k.ToString() + ".csv";
                                if (File.Exists(stringfilepath))
                                    File.Delete(stringfilepath);

                                createcsv.CreateCSVFile(dtable, stringfilepath);
                                begintime.AddMinutes(2);
                                k = k + 10000;
                                yz = yz + 1;
                            }

                        }









                    }
                    catch (Exception ex)
                    {
                        errors=errors+1;

                    }





















                    ////CREATE .CSV FILES  NJFAMILY PLAN

                    //try
                    //{

                    //    SqlParameter[] sqlParamsLoadedFileName3 = new SqlParameter[]
                    //{                   
                    
                    //    DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                    //};


                    //    DataSet hnj_NJFamily = dbU.ExecuteDataSet("HNJH_GetNJFamily", sqlParamsLoadedFileName3);


                    //    int tablecount = hnj_NJFamily.Tables[0].Rows.Count;
                    //    if (hnj_NJFamily.Tables[0].Rows.Count > 0)
                    //    {

                    //        int start = 0;

                    //        bool respT1;
                    //        bool respT2;
                    //        bool respT3;
                    //        bool respT4;
                    //        bool respT5;
                    //        bool respT6;
                    //        bool respT7;
                    //        bool respT8;
                    //        bool respT9;
                    //        bool respT10;
                    //        bool respT11;
                    //        bool respT12;
                    //        bool respT13;
                    //        bool respT14;
                    //        bool respT15;
                    //        bool respT16;
                    //        bool respT17;
                    //        //bool respT18;
                    //        //bool respT19;
                    //        //bool respT20;








                    //        createCSV createcsvT = new createCSV();



                    //        string CSVFilenameNJ1 = z + "_ID_" + DateTime.Now.ToString("yyyyMMddHHmmss") + "_1";
                    //        string CSVFilenameNJ2 = z + "_ID_" + DateTime.Now.AddMinutes(2).ToString("yyyyMMddHHmmss") + "_10001";
                    //        string CSVFilenameNJ3 = z + "_ID_" + DateTime.Now.AddMinutes(3).ToString("yyyyMMddHHmmss") + "_20001";
                    //        string CSVFilenameNJ4 = z + "_ID_" + DateTime.Now.AddMinutes(4).ToString("yyyyMMddHHmmss") + "_30001";
                    //        string CSVFilenameNJ5 = z + "_ID_" + DateTime.Now.AddMinutes(5).ToString("yyyyMMddHHmmss") + "_40001";
                    //        string CSVFilenameNJ6 = z + "_ID_" + DateTime.Now.AddMinutes(6).ToString("yyyyMMddHHmmss") + "_50001";
                    //        string CSVFilenameNJ7 = z + "_ID_" + DateTime.Now.AddMinutes(7).ToString("yyyyMMddHHmmss") + "_60001";
                    //        string CSVFilenameNJ8 = z + "_ID_" + DateTime.Now.AddMinutes(8).ToString("yyyyMMddHHmmss") + "_70001";
                    //        string CSVFilenameNJ9 = z + "_ID_" + DateTime.Now.AddMinutes(9).ToString("yyyyMMddHHmmss") + "_80001";
                    //        string CSVFilenameNJ10 = z + "_ID_" + DateTime.Now.AddMinutes(10).ToString("yyyyMMddHHmmss") + "_90001";
                    //        string CSVFilenameNJ11 = z + "_ID_" + DateTime.Now.AddMinutes(11).ToString("yyyyMMddHHmmss") + "_100001";
                    //        string CSVFilenameNJ12 = z + "_ID_" + DateTime.Now.AddMinutes(12).ToString("yyyyMMddHHmmss") + "_110001";
                    //        string CSVFilenameNJ13 = z + "_ID_" + DateTime.Now.AddMinutes(13).ToString("yyyyMMddHHmmss") + "_120001";
                    //        string CSVFilenameNJ14 = z + "_ID_" + DateTime.Now.AddMinutes(14).ToString("yyyyMMddHHmmss") + "_130001";
                    //        string CSVFilenameNJ15 = z + "_ID_" + DateTime.Now.AddMinutes(15).ToString("yyyyMMddHHmmss") + "_140001";
                    //        string CSVFilenameNJ16 = z + "_ID_" + DateTime.Now.AddMinutes(16).ToString("yyyyMMddHHmmss") + "_150001";
                    //        string CSVFilenameNJ17 = z + "_ID_" + DateTime.Now.AddMinutes(17).ToString("yyyyMMddHHmmss") + "_160001";
                    //        string CSVFilenameNJ18 = z + "_ID_" + DateTime.Now.AddMinutes(18).ToString("yyyyMMddHHmmss") + "_170001";
                    //        string CSVFilenameNJ19 = z + "_ID_" + DateTime.Now.AddMinutes(19).ToString("yyyyMMddHHmmss") + "_180001";
                    //        string CSVFilenameNJ20 = z + "_ID_" + DateTime.Now.AddMinutes(10).ToString("yyyyMMddHHmmss") + "_190001";





                    //        string pNameT1 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ1 + ".csv";
                    //        string pNameT2 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ2 + ".csv";
                    //        string pNameT3 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ3 + ".csv";
                    //        string pNameT4 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ4 + ".csv";
                    //        string pNameT5 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ5 + ".csv";
                    //        string pNameT6 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ6 + ".csv";
                    //        string pNameT7 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ7 + ".csv";
                    //        string pNameT8 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ8 + ".csv";
                    //        string pNameT9 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ9 + ".csv";
                    //        string pNameT10 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ10 + ".csv";
                    //        string pNameT11 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ11 + ".csv";
                    //        string pNameT12 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ12 + ".csv";
                    //        string pNameT13 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ13 + ".csv";
                    //        string pNameT14 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ14 + ".csv";
                    //        string pNameT15 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ15 + ".csv";
                    //        string pNameT16 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ16 + ".csv";
                    //        string pNameT17 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ17 + ".csv";
                    //        string pNameT18 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ18 + ".csv";
                    //        string pNameT19 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ19 + ".csv";
                    //        string pNameT20 = fileInfo.DirectoryName + "\\" + CSVFilenameNJ20 + ".csv";


                    //        if (File.Exists(pNameT1))
                    //            File.Delete(pNameT1);


                    //        if (File.Exists(pNameT2))
                    //            File.Delete(pNameT2);

                    //        if (File.Exists(pNameT3))
                    //            File.Delete(pNameT3);

                    //        if (File.Exists(pNameT4))
                    //            File.Delete(pNameT4);

                    //        if (File.Exists(pNameT5))
                    //            File.Delete(pNameT5);

                    //        if (File.Exists(pNameT6))
                    //            File.Delete(pNameT6);

                    //        if (File.Exists(pNameT7))
                    //            File.Delete(pNameT7);


                    //        if (File.Exists(pNameT8))
                    //            File.Delete(pNameT8);


                    //        if (File.Exists(pNameT9))
                    //            File.Delete(pNameT9);

                    //        if (File.Exists(pNameT10))
                    //            File.Delete(pNameT10);

                    //        if (File.Exists(pNameT11))
                    //            File.Delete(pNameT11);

                    //        if (File.Exists(pNameT12))
                    //            File.Delete(pNameT12);

                    //        if (File.Exists(pNameT13))
                    //            File.Delete(pNameT13);

                    //        if (File.Exists(pNameT14))
                    //            File.Delete(pNameT14);

                    //        if (File.Exists(pNameT15))
                    //            File.Delete(pNameT15);

                    //        if (File.Exists(pNameT16))
                    //            File.Delete(pNameT16);

                    //        if (File.Exists(pNameT17))
                    //            File.Delete(pNameT17);

                    //        if (File.Exists(pNameT18))
                    //            File.Delete(pNameT18);

                    //        if (File.Exists(pNameT19))
                    //            File.Delete(pNameT19);

                    //        if (File.Exists(pNameT20))
                    //            File.Delete(pNameT20);




                    //        var fieldnamesT = new List<string>();

                    //        for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //        {
                    //            fieldnamesT.Add(hnj_NJFamily.Tables[0].Columns[index].ColumnName);
                    //        }





                    //        respT1 = createcsvT.addRecordsPipe_CSV(pNameT1, fieldnamesT);
                    //      //  respT2 = createcsvT.addRecordsPipe_CSV(pNameT2, fieldnamesT);
                    //        //respT3 = createcsvT.addRecordsPipe_CSV(pNameT3, fieldnamesT);
                    //        //respT4 = createcsvT.addRecordsPipe_CSV(pNameT4, fieldnamesT);
                    //        //respT5 = createcsvT.addRecordsPipe_CSV(pNameT5, fieldnamesT);
                    //        //respT6 = createcsvT.addRecordsPipe_CSV(pNameT6, fieldnamesT);
                    //        //respT7 = createcsvT.addRecordsPipe_CSV(pNameT7, fieldnamesT);
                    //        //respT8 = createcsvT.addRecordsPipe_CSV(pNameT8, fieldnamesT);
                    //        //respT9 = createcsvT.addRecordsPipe_CSV(pNameT9, fieldnamesT);
                    //        //respT10 = createcsvT.addRecordsPipe_CSV(pNameT10, fieldnamesT);
                    //        //respT11 = createcsvT.addRecordsPipe_CSV(pNameT11, fieldnamesT);
                    //        //respT12 = createcsvT.addRecordsPipe_CSV(pNameT12, fieldnamesT);
                    //        //respT13 = createcsvT.addRecordsPipe_CSV(pNameT13, fieldnamesT);
                    //        //respT14 = createcsvT.addRecordsPipe_CSV(pNameT14, fieldnamesT);
                    //        //respT15 = createcsvT.addRecordsPipe_CSV(pNameT15, fieldnamesT);

                    //        foreach (DataRow row in hnj_NJFamily.Tables[0].Rows)
                    //        {
                    //            if (start <= 9999)
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());


                    //                }


                    //                respT1 = false;
                    //                respT1 = createcsvT.addRecordsPipe_CSV(pNameT1, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 10000) && (start <= 19999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());


                    //                }


                    //                respT2 = false;
                    //                respT2 = createcsvT.addRecordsPipe_CSV(pNameT2, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 20000) && (start <= 29999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT3 = false;
                    //                respT3 = createcsvT.addRecordsPipe_CSV(pNameT3, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 30000) && (start <= 39999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT4 = false;
                    //                respT4 = createcsvT.addRecordsPipe_CSV(pNameT4, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 40000) && (start <= 49999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT5 = false;
                    //                respT5 = createcsvT.addRecordsPipe_CSV(pNameT5, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 50000) && (start <= 59999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT6 = false;
                    //                respT6 = createcsvT.addRecordsPipe_CSV(pNameT6, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 60000) && (start <= 69999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT7 = false;
                    //                respT7 = createcsvT.addRecordsPipe_CSV(pNameT7, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 70000) && (start <= 79999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT8 = false;
                    //                respT8 = createcsvT.addRecordsPipe_CSV(pNameT8, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }

                    //            else if ((start >= 80000) && (start <= 89999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT9 = false;
                    //                respT9 = createcsvT.addRecordsPipe_CSV(pNameT9, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 90000) && (start <= 99999))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT10 = false;
                    //                respT10 = createcsvT.addRecordsPipe_CSV(pNameT10, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }

                    //            else if ((start >= 100000) && (start <= 109000))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT11 = false;
                    //                respT11 = createcsvT.addRecordsPipe_CSV(pNameT11, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }

                    //            else if ((start >= 110000) && (start <= 119000))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT11 = false;
                    //                respT11 = createcsvT.addRecordsPipe_CSV(pNameT11, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 120000) && (start <= 129000))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT11 = false;
                    //                respT11 = createcsvT.addRecordsPipe_CSV(pNameT11, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 130000) && (start <= 139000))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT11 = false;
                    //                respT11 = createcsvT.addRecordsPipe_CSV(pNameT11, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }
                    //            else if ((start >= 140000) && (start <= 149000))
                    //            {

                    //                var rowData = new List<string>();

                    //                for (int index = 0; index < hnj_NJFamily.Tables[0].Columns.Count; index++)
                    //                {
                    //                    rowData.Add(row[index].ToString());

                    //                }


                    //                respT11 = false;
                    //                respT11 = createcsvT.addRecordsPipe_CSV(pNameT11, rowData);
                    //                start = start + 1;
                    //                if (start == tablecount)
                    //                    break;

                    //            }

                    //        }
                    //    }
                    //}
                    //catch (Exception ex)
                    //{
                    //    errors = errors + 1;
                    //}



                //CREATE .CSV FILES  MLTSS PLAN

                    try
                    {

                        SqlParameter[] sqlParamsLoadedFileName4 = new SqlParameter[]
                   {                   
                       DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                   };


                        DataSet hnj_MLTSS = dbU.ExecuteDataSet("HNJH_GetMLTSS", sqlParamsLoadedFileName4);
                        DataTable dtable2 = hnj_MLTSS.Tables[0];
                        int tablecount = hnj_MLTSS.Tables[0].Rows.Count;

                        //HNJHID031016_ID-CD_DSNP_DateTimeStamp.csv
                        string pNameT1 = fileInfo.DirectoryName + "\\";
                        if (hnj_MLTSS.Tables[0].Rows.Count > 0)
                        {



                            List<DataTable> dttableaftersplit = new List<DataTable>();
                            dttableaftersplit = SplitTableNj(dtable2, 10000);
                            DateTime begintime = DateTime.Now;
                            int k = 1;
                            int xc = 8;
                            createCSV createcsv = new createCSV();
                            foreach (var dtable in dttableaftersplit)
                            {
                                //HNJHID121616_ID-CD_DSNP_20161216174704 change to HNJHID121616_ID_DSNP_20161216174704_1.csv 
                                string stringfilepath = pNameT1 + z + "_ID_" + begintime.ToString("yyyyMMddHHmmss") + xc.ToString() + "_" + k.ToString() + "m" + ".csv";
                                if (File.Exists(stringfilepath))
                                    File.Delete(stringfilepath);

                                createcsv.CreateCSVFile(dtable, stringfilepath);
                                begintime.AddMinutes(2);
                                k = k + 1;
                                xc = xc + 1;
                            }

                        }







                    }
                  catch (Exception EX)
                    {
                        errors = errors + 1;
                    }














                   // //CREATE .CSV FILES  MLTSS PLAN
                   // try
                   // {

                   //     SqlParameter[] sqlParamsLoadedFileName4 = new SqlParameter[]
                   //{                   
                   //    DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                   //};


                   //     DataSet hnj_MLTSS = dbU.ExecuteDataSet("HNJH_GetMLTSS", sqlParamsLoadedFileName4);
                   //     int tablecount = hnj_MLTSS.Tables[0].Rows.Count;
                   //     if (hnj_MLTSS.Tables[0].Rows.Count > 0)
                   //     {

                   //         int start = 0;
                   //         bool respT1;
                   //         bool respT2;
                   //         bool respT3;
                   //         bool respT4;
                   //         bool respT5;
                   //         bool respT6;
                   //         bool respT7;
                   //         bool respT8;
                   //         bool respT9;
                   //         bool respT10;
                   //         bool respT11;
                   //         bool respT12;
                   //         bool respT13;
                   //         bool respT14;
                   //         bool respT15;
                   //         bool respT16;
                   //         bool respT17;
                   //         bool respT18;
                   //         bool respT19;



                   //         createCSV createcsvT = new createCSV();
                   //         string CSVFilenameMLTSS1 = z + "_ID_" + DateTime.Now.ToString("yyyyMMddHHmmss")+"_1m";
                   //         string CSVFilenameMLTSS2 = z + "_ID_" + DateTime.Now.AddMinutes(2).ToString("yyyyMMddHHmmss")+"_2m";
                   //         string CSVFilenameMLTSS3 = z + "_ID_" + DateTime.Now.AddMinutes(3).ToString("yyyyMMddHHmmss")+"_3m";
                   //         string CSVFilenameMLTSS4 = z + "_ID_" + DateTime.Now.AddMinutes(4).ToString("yyyyMMddHHmmss")+"_4m";
                   //         string CSVFilenameMLTSS5 = z + "_ID_" + DateTime.Now.AddMinutes(5).ToString("yyyyMMddHHmmss")+"_5m";
                   //         string CSVFilenameMLTSS6 = z + "_ID_" + DateTime.Now.AddMinutes(6).ToString("yyyyMMddHHmmss")+"_6m";
                   //         string CSVFilenameMLTSS7 = z + "_ID_" + DateTime.Now.AddMinutes(7).ToString("yyyyMMddHHmmss")+"_7m";
                   //         string CSVFilenameMLTSS8 = z + "_ID_" + DateTime.Now.AddMinutes(8).ToString("yyyyMMddHHmmss")+"_8m";
                   //         string CSVFilenameMLTSS9 = z + "_ID_" + DateTime.Now.AddMinutes(9).ToString("yyyyMMddHHmmss")+"_9m";
                   //         string CSVFilenameMLTSS10 = z + "_ID_" + DateTime.Now.AddMinutes(10).ToString("yyyyMMddHHmmss")+"_10m";
                   //         string CSVFilenameMLTSS11 = z + "_ID_" + DateTime.Now.AddMinutes(11).ToString("yyyyMMddHHmmss")+"_11m";
                   //         string CSVFilenameMLTSS12 = z + "_ID_" + DateTime.Now.AddMinutes(12).ToString("yyyyMMddHHmmss")+"_12m";
                   //         string CSVFilenameMLTSS13 = z + "_ID_" + DateTime.Now.AddMinutes(13).ToString("yyyyMMddHHmmss")+"_13m";
                   //         string CSVFilenameMLTSS14 = z + "_ID_" + DateTime.Now.AddMinutes(14).ToString("yyyyMMddHHmmss")+"_14m";
                   //         string CSVFilenameMLTSS15 = z + "_ID_" + DateTime.Now.AddMinutes(15).ToString("yyyyMMddHHmmss")+"_15m";
                   //         string CSVFilenameMLTSS16 = z + "_ID_" + DateTime.Now.AddMinutes(16).ToString("yyyyMMddHHmmss")+"_16m";
                   //         string CSVFilenameMLTSS17 = z + "_ID_" + DateTime.Now.AddMinutes(17).ToString("yyyyMMddHHmmss");
                   //         string CSVFilenameMLTSS18 = z + "_ID_" + DateTime.Now.AddMinutes(18).ToString("yyyyMMddHHmmss");
                   //         string CSVFilenameMLTSS19 = z + "_ID_" + DateTime.Now.AddMinutes(19).ToString("yyyyMMddHHmmss");

                   //         string pNameT1 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS1 + ".csv";
                   //         string pNameT2 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS2 + ".csv";
                   //         string pNameT3 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS3 + ".csv";
                   //         string pNameT4 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS4 + ".csv";
                   //         string pNameT5 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS5 + ".csv";
                   //         string pNameT6 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS6 + ".csv";
                   //         string pNameT7 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS7 + ".csv";
                   //         string pNameT8 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS8 + ".csv";
                   //         string pNameT9 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS9 + ".csv";
                   //         string pNameT10 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS10 + ".csv";
                   //         string pNameT11 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS11 + ".csv";
                   //         string pNameT12 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS12 + ".csv";
                   //         string pNameT13 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS13 + ".csv";
                   //         string pNameT14 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS14 + ".csv";
                   //         string pNameT15 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS15 + ".csv";
                   //         string pNameT16 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS16 + ".csv";
                   //         string pNameT17 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS17 + ".csv";
                   //         string pNameT18 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS18 + ".csv";
                   //         string pNameT19 = fileInfo.DirectoryName + "\\" + CSVFilenameMLTSS19 + ".csv";


                   //         if (File.Exists(pNameT1))
                   //             File.Delete(pNameT1);

                   //         if (File.Exists(pNameT2))
                   //             File.Delete(pNameT2);

                   //         if (File.Exists(pNameT3))
                   //             File.Delete(pNameT3);

                   //         if (File.Exists(pNameT4))
                   //             File.Delete(pNameT4);

                   //         if (File.Exists(pNameT5))
                   //             File.Delete(pNameT5);

                   //         if (File.Exists(pNameT6))
                   //             File.Delete(pNameT6);


                   //         if (File.Exists(pNameT7))
                   //             File.Delete(pNameT7);


                   //         if (File.Exists(pNameT8))
                   //             File.Delete(pNameT8);

                   //         if (File.Exists(pNameT9))
                   //             File.Delete(pNameT9);

                   //         if (File.Exists(pNameT10))
                   //             File.Delete(pNameT10);

                   //         if (File.Exists(pNameT11))
                   //             File.Delete(pNameT11);

                   //         if (File.Exists(pNameT12))
                   //             File.Delete(pNameT12);

                   //         if (File.Exists(pNameT13))
                   //             File.Delete(pNameT13);

                   //         if (File.Exists(pNameT14))
                   //             File.Delete(pNameT14);

                   //         if (File.Exists(pNameT15))
                   //             File.Delete(pNameT15);

                   //         if (File.Exists(pNameT16))
                   //             File.Delete(pNameT16);

                   //         if (File.Exists(pNameT17))
                   //             File.Delete(pNameT17);

                   //         if (File.Exists(pNameT18))
                   //             File.Delete(pNameT18);

                   //         if (File.Exists(pNameT19))
                   //             File.Delete(pNameT19);



                   //         var fieldnamesT = new List<string>();

                   //         for (int index = 0; index < hnj_MLTSS.Tables[0].Columns.Count; index++)
                   //         {
                   //             fieldnamesT.Add(hnj_MLTSS.Tables[0].Columns[index].ColumnName);
                   //         }
                   //         respT1 = createcsvT.addRecordsPipe_CSV(pNameT1, fieldnamesT);
                   //      //   respT2 = createcsvT.addRecordsPipe_CSV(pNameT2, fieldnamesT);
                   //       //  respT3 = createcsvT.addRecordsPipe_CSV(pNameT3, fieldnamesT);
                   //         //respT4 = createcsvT.addRecordsPipe_CSV(pNameT4, fieldnamesT);
                   //         // respT5 = createcsvT.addRecordsPipe_CSV(pNameT5, fieldnamesT);
                   //         // respT6 = createcsvT.addRecordsPipe_CSV(pNameT6, fieldnamesT);
                   //         // respT7 = createcsvT.addRecordsPipe_CSV(pNameT7, fieldnamesT);
                   //         // respT8 = createcsvT.addRecordsPipe_CSV(pNameT8, fieldnamesT);
                   //         // respT9 = createcsvT.addRecordsPipe_CSV(pNameT9, fieldnamesT);
                   //         // respT10 = createcsvT.addRecordsPipe_CSV(pNameT10, fieldnamesT);
                   //         //  respT11 = createcsvT.addRecordsPipe_CSV(pNameT11, fieldnamesT);
                              
                   //         foreach (DataRow row in hnj_MLTSS.Tables[0].Rows)
                   //         {
                   //             if (start <= 9999)
                   //             {
                   //                 var rowData = new List<string>();

                   //                 for (int index = 0; index < hnj_MLTSS.Tables[0].Columns.Count; index++)
                   //                 {
                   //                     rowData.Add(row[index].ToString());

                   //                 }


                   //                 respT1 = false;
                   //                 respT1 = createcsvT.addRecordsPipe_CSV(pNameT1, rowData);
                   //                 start = start + 1;
                   //                 if (start == tablecount)
                   //                     break;

                   //             }
                   //             else if ((start >= 10000) && (start <= 19999))
                   //             {
                   //                 var rowData = new List<string>();

                   //                 for (int index = 0; index < hnj_MLTSS.Tables[0].Columns.Count; index++)
                   //                 {
                   //                     rowData.Add(row[index].ToString());

                   //                 }

                   //                 respT2 = false;
                   //                 respT2 = createcsvT.addRecordsPipe_CSV(pNameT2, rowData);
                   //                 start = start + 1;
                   //                 if (start == tablecount)
                   //                     break;

                   //             }
                   //             else if ((start >= 20000) && (start <= 29999))
                   //             {
                   //                 var rowData = new List<string>();

                   //                 for (int index = 0; index < hnj_MLTSS.Tables[0].Columns.Count; index++)
                   //                 {
                   //                     rowData.Add(row[index].ToString());

                   //                 }

                   //                 respT3 = false;
                   //                 respT3 = createcsvT.addRecordsPipe_CSV(pNameT3, rowData);
                   //                 start = start + 1;
                   //                 if (start == tablecount)
                   //                     break;

                   //             }
                   //             else if ((start >= 30000) && (start <= 39999))
                   //             {
                   //                 var rowData = new List<string>();

                   //                 for (int index = 0; index < hnj_MLTSS.Tables[0].Columns.Count; index++)
                   //                 {
                   //                     rowData.Add(row[index].ToString());

                   //                 }

                   //                 respT4 = false;
                   //                 respT4 = createcsvT.addRecordsPipe_CSV(pNameT4, rowData);
                   //                 start = start + 1;
                   //                 if (start == tablecount)
                   //                     break;

                   //             }
                   //             else if ((start >= 40000) && (start <= 49999))
                   //             {
                   //                 var rowData = new List<string>();

                   //                 for (int index = 0; index < hnj_MLTSS.Tables[0].Columns.Count; index++)
                   //                 {
                   //                     rowData.Add(row[index].ToString());

                   //                 }


                   //                 respT5 = false;
                   //                 respT5 = createcsvT.addRecordsPipe_CSV(pNameT5, rowData);
                   //                 start = start + 1;
                   //                 if (start == tablecount)
                   //                     break;

                   //             }
                   //             else if ((start >= 50000) && (start <= 59999))
                   //             {
                   //                 var rowData = new List<string>();

                   //                 for (int index = 0; index < hnj_MLTSS.Tables[0].Columns.Count; index++)
                   //                 {
                   //                     rowData.Add(row[index].ToString());

                   //                 }


                   //                 respT6 = false;
                   //                 respT6 = createcsvT.addRecordsPipe_CSV(pNameT6, rowData);
                   //                 start = start + 1;
                   //                 if (start == tablecount)
                   //                     break;

                   //             }
                               
                               

                   //         }
                           

                   //     }


                   // }
                   // catch (Exception EX)
                   // {
                   //     errors = errors + 1;
                   // }

               

                  
                
                
                ///Create HNJHID031016_RCV_20160311172130.csv for horizon

                SqlParameter[] sqlParamsRcv = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };

                DateTime dt1 = DateTime.Now;
                string s1 = dt1.ToString("yyyyMMddHHmmss");
                string OutputFolder = fileInfo.DirectoryName + "\\Output\\";
                if (!Directory.Exists(OutputFolder))
                    Directory.CreateDirectory(OutputFolder);

                string RCVname = z+"_RCV_" +s1+ ".csv";
                string FileName = OutputFolder + "\\" + RCVname;
                if (File.Exists(FileName))
                    File.Delete(FileName);
                DataSet hnj_Rcv = dbU.ExecuteDataSet("HNJH_CreateRcvTableForNJ", sqlParamsRcv);
                if (hnj_Rcv.Tables[0].Rows.Count > 0)
                {  
                    createCSV createcsvT = new createCSV();
                   
                    var fieldnamesT = new List<string>();
                    for (int index = 0; index < hnj_Rcv.Tables[0].Columns.Count; index++)
                    {
                        fieldnamesT.Add(hnj_Rcv.Tables[0].Columns[index].ColumnName);
                    }

                    bool respT2 = createcsvT.addRecordsCSV(FileName, fieldnamesT);
                    foreach (DataRow row in hnj_Rcv.Tables[0].Rows)
                    {

                        var rowData = new List<string>();
                        for (int index = 0; index < hnj_Rcv.Tables[0].Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());

                        }
                        respT2 = false;
                        respT2 = createcsvT.addRecordsCSV(FileName, rowData);


                    }

                }

                ///Create HNJHID031016_AV_20160311182514.xlsx for horizon

                SqlParameter[] sqlParamsNonDeliverable = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                                      
                  };

                DateTime dtToday = DateTime.Now;
                string s2 = dtToday.ToString("yyyyMMddHHmmss");
               
                               
                string AVName = z + "_AV_" + s2 + ".xlsx";
                if (!Directory.Exists(OutputFolder))
                    Directory.CreateDirectory(OutputFolder);
                string AVFileName = OutputFolder + "\\" + AVName;

                if (File.Exists(AVFileName))
                    File.Delete(AVFileName);
                DataSet hnj_ACV = dbU.ExecuteDataSet("HNJH_CreateAcvTableForNJ", sqlParamsNonDeliverable);
                if (hnj_ACV.Tables[0].Rows.Count > 0)
                {

                    ClassExcel excelcreate = new ClassExcel();
                   excelcreate.createExcelFile(hnj_ACV, OutputFolder + "\\" + AVName);
                   // ExcelLibrary.DataSetHelper.CreateWorkbook(AVFileName, hnj_ACV);
                  

                }
              }
              
           
                
                
              else
                {

                    errors=errors+1;
                }

            }
            else
            {
              errors=errors+1;
              }
             if (errors == 0)
                {

                    try
                    {

                        var recnumFromTemp = dbU.ExecuteScalar("select max(recnum) from HNJH_IDCards_Temp");
                       
                     
                       dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + ( Convert.ToInt64(recnumFromTemp.ToString())) + ",'HNJH_IDCards', GETDATE())");

                     
                    }
                    catch (Exception EX)
                    {
                        errors = errors + 1;
                    }

                }
            
               
                if (errors == 0)
                { return ""; }
                else
                { return "Invalid"; }

        }



        public string evaluate_HNJHIDCards_Reprocess(string fileName, string directoryTXT)
        {

            DataSet ds = null;
            dbU = ProcessVars.oDBUtility();
            Recnum = 1;
            string z = fileName;
            DateTime dt = DateTime.Now;
            string s = dt.ToString("yyyyMMddHHmmss");
            DataTable NJHMemoryTable;
            DataSet hnj_NJFamily = null;
            int errors = 0;

            string Filex = z + "_ID_" + s;


            try
            {




                DataSet hnj_XmpieDSet = dbU.ExecuteDataSet("HNJH_IDCardsReprocess");




            }
            catch (Exception ex)
            {
                errors = errors + 1;
            }




            try
            {



                SqlParameter[] sqlParamsRenameFile = new SqlParameter[]
                    {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                    };



                DataSet hnj_XmpieDSet = dbU.ExecuteDataSet("HNJH_RenameFileName_Reprocess", sqlParamsRenameFile);






            }
            catch (Exception ex)
            {
                errors = errors + 1;
            }






            try
            {
                SqlParameter[] sqlParamsLoadedFileName2 = new SqlParameter[]
                    {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                    };



                DataSet hnj_XmpieDSet = dbU.ExecuteDataSet("HNJH_MoveDataToXmpie_Reprocess", sqlParamsLoadedFileName2);
            }
            catch (Exception ex)
            {
                errors = errors + 1;
            }




            //creating csv file for njFamilyPlan 

            try
            {
                SqlParameter[] sqlParamsLoadedFileName3 = new SqlParameter[]
                    {                   
                     DBUtility.GetInParameter("@FileName",Filex),
                      
                       
                    };



                hnj_NJFamily = dbU.ExecuteDataSet("HNJH_GetNJFamily_Reprocess", sqlParamsLoadedFileName3);




                DataTable dtable1 = hnj_NJFamily.Tables[0];
                int tablecount = hnj_NJFamily.Tables[0].Rows.Count;

                //HNJHID031016_ID-CD_DSNP_DateTimeStamp.csv
                string pNameT1 = directoryTXT + "\\";
                if (hnj_NJFamily.Tables[0].Rows.Count > 0)
                {



                    List<DataTable> dttableaftersplit = new List<DataTable>();
                    dttableaftersplit = SplitTableNj(dtable1, 10000);
                    DateTime begintime = DateTime.Now;
                    int k = 1;
                    int yz = 1;
                    createCSV createcsv = new createCSV();
                    foreach (var dtable in dttableaftersplit)
                    {
                        //HNJHID121616_ID-CD_DSNP_20161216174704 change to HNJHID121616_ID_DSNP_20161216174704_1.csv 
                        string stringfilepath = pNameT1 + z + "_ID_" + begintime.ToString("yyyyMMddHHmmss") + yz.ToString() + "_" + k.ToString() + ".csv";
                        if (File.Exists(stringfilepath))
                            File.Delete(stringfilepath);

                        createcsv.CreateCSVFile(dtable, stringfilepath);
                        begintime.AddMinutes(2);
                        k = k + 10000;
                        yz = yz + 1;
                    }

                }

            }
            catch (Exception ex)
            {
                errors = errors + 1;
            }




            //CREATE .CSV FILES  MLTSS PLAN

            try
            {

                SqlParameter[] sqlParamsLoadedFileName4 = new SqlParameter[]
                   {                   
                       DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                   };


                DataSet hnj_MLTSS = dbU.ExecuteDataSet("HNJH_GetMLTSS_Reprocess", sqlParamsLoadedFileName4);
                DataTable dtable2 = hnj_MLTSS.Tables[0];
                int tablecount = hnj_MLTSS.Tables[0].Rows.Count;

                //HNJHID031016_ID-CD_DSNP_DateTimeStamp.csv
                string pNameT1 = directoryTXT + "\\";
                if (hnj_MLTSS.Tables[0].Rows.Count > 0)
                {



                    List<DataTable> dttableaftersplit = new List<DataTable>();
                    dttableaftersplit = SplitTableNj(dtable2, 10000);
                    DateTime begintime = DateTime.Now;
                    int k = 1;
                    int xc = 8;
                    createCSV createcsv = new createCSV();
                    foreach (var dtable in dttableaftersplit)
                    {
                        //HNJHID121616_ID-CD_DSNP_20161216174704 change to HNJHID121616_ID_DSNP_20161216174704_1.csv 
                        string stringfilepath = pNameT1 + z + "_ID_" + begintime.ToString("yyyyMMddHHmmss") + xc.ToString() + "_" + k.ToString() + "m" + ".csv";
                        if (File.Exists(stringfilepath))
                            File.Delete(stringfilepath);

                        createcsv.CreateCSVFile(dtable, stringfilepath);
                        begintime.AddMinutes(2);
                        k = k + 1;
                        xc = xc + 1;
                    }

                }







            }
            catch (Exception EX)
            {
                errors = errors + 1;
            }



            if (errors == 0)
            {
                try
                {
                    DataSet dstemp = dbU.ExecuteDataSet("HNJH_IDCardsReprocessedSentOut");
                }

                catch (Exception ex)
                {


                }

                return "";
            }
            else
            { return "Invalid"; }




        }


























                public string evaluate_SAPD(string fileName, string directoryTXT)
        {
            //SGSAPD60DAYRENW_20160401001328.TXT -Inputfilename with path
            DataSet ds = null;
            dbU = ProcessVars.oDBUtility();
            Recnum = 1;
            string x = Path.GetFileName(fileName);
            string y = Path.GetDirectoryName(fileName);
            string z = Path.GetFileNameWithoutExtension(fileName);
            int firstpos = z.IndexOf("_");
            string DateRecieved = z.Substring(firstpos + 1, 8);
            DateTime dt = DateTime.Now;
            string s = dt.ToString("yyyyMMddHHmmss");
            //  DataTable SAPDMemoryTable;
            int errors = 0;


            SqlParameter[] sqlParamsLoaded = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",z),                                                     
                  };



            string Sql = "SELECT count(*) FROM  HNJH_SAPD_Master WHERE FileName like '" + z + "%'";

            object exists = dbU.ExecuteScalar(Sql, sqlParamsLoaded);

            //CHECK IF FILE EXISTS-IF EXISTS THEN DONT DO THE REST
            if (exists != null && Convert.ToInt32(exists).Equals(0))
            {


                string filename = Path.GetFileName(fileName);
                DataTable SAPDMemoryTable = new System.Data.DataTable("SAPDMemoryTable");


               
               // GRP_ID|SGRP_ID|PRD_ID|EFDT|TRMDT|RNWL_DT|GRP NAME|STREET|CITY|STATE|ZIP|COUNTY|RATE|NEW PRD|NEW PRD NAME|CNTY CODE|BRKR ID
              //  ds = TextToDataSet.Convertwithoutheader(fileName, "SAPDMemoryTable", "|", new string[] { "MAIN GROUP", "SUBGROUP", "Date 1", "Date 2", "Anniversary Date", "Group Name", "Group Address", "Add2", "Add3", "City", "State", "Zip", "County", "Rate", "Plan", "PlanName", "BROKER ID" });
                ds = TextToDataSet.Convert(fileName, "SAPDMemoryTable", "|");
              //  ds = TextToDataSet.Convertwithoutheader(fileName, "SAPDMemoryTable", "|", new string[] { "MAIN GROUP", "SUBGROUP", "Date 1", "Date 2", "Anniversary Date", "Group Name", "Group Address", "City", "State", "Zip", "County", "Rate", "Plan", "PlanName", "BROKER ID" });
              
                
                var recnum = dbU.ExecuteScalar("SELECT MAX(RECNUM) FROM [BCBS_Horizon].[dbo].[HOR_parse_SEQ]");
                if (recnum.ToString() == "")

                    GRecnum = 1;
                else
                    GRecnum = Convert.ToInt64(recnum.ToString()) + 1;

                SAPDMemoryTable = ds.Tables[0];
                SAPDMemoryTable.Columns.Add("DateRecieved");
                SAPDMemoryTable.Columns.Add("RECNUM");
                SAPDMemoryTable.Columns.Add("FileName");
                SAPDMemoryTable.Columns.Add("FileDate");
                SAPDMemoryTable.Columns.Add("RecieveStatus");
                SAPDMemoryTable.Columns.Add("Dl");
                SAPDMemoryTable.Columns.Add("Imb");
                SAPDMemoryTable.Columns.Add("Broker_FileName");
                SAPDMemoryTable.Columns.Add("Add2");
                SAPDMemoryTable.Columns.Add("Add3");
                FileInfo fileInfo = new System.IO.FileInfo(fileName);
                DateTime lastWriteTime = File.GetLastWriteTime(fileName);
                string FileDate = lastWriteTime.ToString("MM/dd/yyyy");
                string Filex = z;


                foreach (DataRow row in SAPDMemoryTable.Rows) // Loop over the rows.
                {

                    try
                    {

                        row["DateRecieved"] = DateRecieved.Trim();
                        row["RECNUM"] = GRecnum;
                        GRecnum++;
                        row["Add2"] = "";
                        row["Add3"] = "";
                        row["FileName"] = Filex.Trim();//SGSAPD60DAYRENW_20160401001328 
                        row["FileDate"] = FileDate.Trim();
                        row["RecieveStatus"] = "Recieve";
                        row["Dl"] = 'Y';
                       // GRP_ID|SGRP_ID|PRD_ID|EFDT|TRMDT|RNWL_DT|GRP NAME|STREET|CITY|STATE|ZIP|COUNTY|RATE|NEW PRD|NEW PRD NAME|CNTY CODE|BRKR ID
                      // if (row["BROKER ID"].ToString() != null && row["BROKER ID"].ToString() != "" && row["Anniversary Date"].ToString() != null && row["Anniversary Date"].ToString() != "")
                        if (row["BRKR ID"].ToString() != null && row["BRKR ID"].ToString() != "" && row["RNWL_DT"].ToString() != null && row["RNWL_DT"].ToString() != "")
                        
                        {



                            string Brokerid = row["BRKR ID"].ToString().Trim();

                            // string PhoneNoWithDash = GetResultsWithHyphen(fieldval.Trim());

                            string anniversarydatemonth = row["RNWL_DT"].ToString();

                            string AnniversaryShortMonthName = GetMonthName(anniversarydatemonth);

                            row["Broker_FileName"] = Brokerid + "_" + "SAPDRENWAL_" + AnniversaryShortMonthName + "_" + row["SGRP_ID"].ToString() + "-" + row["GRP_ID"].ToString() + "-" + s;
                            //011329_SAPDRENWAL_JUN_002-000209-20170321150443
                           // 011812_SAPDRENWAL_JUN_001-0014K2-20170321150443

                        }






                    }
                    catch (Exception ex)
                    {
                        var msg = ex.InnerException;
                        errors = errors + 1;


                    }

                }

                //Removing last ONE  row

                var lastRow = SAPDMemoryTable.Rows[SAPDMemoryTable.Rows.Count - 1];
                SAPDMemoryTable.Rows.Remove(lastRow);
                SAPDMemoryTable.AcceptChanges();
                
                dbU = ProcessVars.oDBUtility();
                using (SqlConnection cn = new SqlConnection(ProcessVars.ConnectionString))
                {

                    cn.Open();
                    using (SqlBulkCopy copy = new SqlBulkCopy(cn))
                    {
                        try
                        {
                            // GRP_ID|SGRP_ID|PRD_ID|EFDT|TRMDT|RNWL_DT|GRP NAME|STREET|CITY|STATE|ZIP|COUNTY|RATE|NEW PRD|NEW PRD NAME|CNTY CODE|BRKR ID
                            //copy.DestinationTableName = "HNJH_SAPD_Temp";

                            //copy.ColumnMappings.Add("MAIN GROUP", "MAIN GROUP");
                            //copy.ColumnMappings.Add("SUBGROUP", "SUBGROUP");
                            //copy.ColumnMappings.Add("Date 1", "Date 1");
                            //copy.ColumnMappings.Add("Date 2", "Date 2");
                            //copy.ColumnMappings.Add("Anniversary Date", "Anniversary Date");
                            //copy.ColumnMappings.Add("Group Name", "Group Name");



                            //copy.ColumnMappings.Add("Group Address", "Group Address");
                            //copy.ColumnMappings.Add("Add2", "Add2");
                            //copy.ColumnMappings.Add("Add3", "Add3");
                            //copy.ColumnMappings.Add("City", "City");
                            //copy.ColumnMappings.Add("State", "State");
                            //copy.ColumnMappings.Add("Zip", "Zip");

                            //copy.ColumnMappings.Add("County", "County");
                            //copy.ColumnMappings.Add("Rate", "Rate");
                            //copy.ColumnMappings.Add("Plan", "Plan");
                            //copy.ColumnMappings.Add("PlanName", "PlanName");
                            //copy.ColumnMappings.Add("BROKER ID", "BROKER ID");


                            //copy.ColumnMappings.Add("DateRecieved", "DateRecieved");
                            //copy.ColumnMappings.Add("RECNUM", "RECNUM");
                            //copy.ColumnMappings.Add("FileName", "FileName");
                            //copy.ColumnMappings.Add("FileDate", "FileDate");
                            //copy.ColumnMappings.Add("Dl", "Dl");
                            //copy.ColumnMappings.Add("RecieveStatus", "RecieveStatus");
                            //copy.ColumnMappings.Add("Broker_FileName", "Broker_FileName");
                           
                            //copy.WriteToServer(SAPDMemoryTable);

                            // GRP_ID|SGRP_ID|PRD_ID|EFDT|TRMDT|RNWL_DT|GRP NAME|STREET|CITY|STATE|ZIP|COUNTY|RATE|NEW PRD|NEW PRD NAME|CNTY CODE|BRKR ID

                            
                            copy.DestinationTableName = "HNJH_SAPD_Temp";

                            copy.ColumnMappings.Add("GRP_ID","MAIN GROUP");
                            copy.ColumnMappings.Add("SGRP_ID","SUBGROUP" );
                            copy.ColumnMappings.Add("PRD_ID", "PRD_ID");
                            copy.ColumnMappings.Add("EFDT","Date 1" );
                            copy.ColumnMappings.Add("TRMDT","Date 2" );
                            copy.ColumnMappings.Add("RNWL_DT","Anniversary Date");
                            copy.ColumnMappings.Add("GRP NAME","Group Name");
                            copy.ColumnMappings.Add( "STREET","Group Address");
                            copy.ColumnMappings.Add("Add2", "Add2");
                            copy.ColumnMappings.Add("Add3", "Add3");
                            copy.ColumnMappings.Add( "CITY","City");
                            copy.ColumnMappings.Add("STATE","State");
                            copy.ColumnMappings.Add("ZIP","Zip");

                            copy.ColumnMappings.Add("COUNTY","County");
                            copy.ColumnMappings.Add("RATE","Rate");
                            copy.ColumnMappings.Add( "NEW PRD","Plan");
                            copy.ColumnMappings.Add("NEW PRD NAME","PlanName" );
                            copy.ColumnMappings.Add("BRKR ID","BROKER ID" );

                            copy.ColumnMappings.Add("CNTY CODE","CNTY_CODE" );




                            copy.ColumnMappings.Add("DateRecieved", "DateRecieved");
                            copy.ColumnMappings.Add("RECNUM", "RECNUM");
                            copy.ColumnMappings.Add("FileName", "FileName");
                            copy.ColumnMappings.Add("FileDate", "FileDate");
                            copy.ColumnMappings.Add("Dl", "Dl");
                            copy.ColumnMappings.Add("RecieveStatus", "RecieveStatus");
                            copy.ColumnMappings.Add("Broker_FileName", "Broker_FileName");

                            copy.WriteToServer(SAPDMemoryTable);

                 
                        }
                        catch (Exception ex)
                        {
                            errors = errors + 1;


                        }
                    }


                }
                //CLEANING TO REMOVE NEWLINE CHARACTER IN [MAIN GROUP]
                SqlParameter[] sqlParamsLoadedFileName = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };


                DataSet hnj_idcardsDs = dbU.ExecuteDataSet("CleanHNJH_SAPD_Temp", sqlParamsLoadedFileName);





                ////// //create csv file to bcc machine

                SqlParameter[] sqlParamsToBCC = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 


                  };

                string BCCname = "HNJH-SAPD_" + z + "_toBCC.csv";
                string pNameT = fileInfo.DirectoryName + "\\" + BCCname;
                if (File.Exists(pNameT))
                    File.Delete(pNameT);
                DataSet hnj_ToBcc = dbU.ExecuteDataSet("HNJH_CreateTableForBccForSAPD", sqlParamsToBCC);

                 if (hnj_ToBcc.Tables[0].Rows.Count > 0)
                {
                    createCSV createcsvT = new createCSV();


                    var fieldnamesT = new List<string>();
                    for (int index = 0; index < hnj_ToBcc.Tables[0].Columns.Count; index++)
                    {
                        fieldnamesT.Add(hnj_ToBcc.Tables[0].Columns[index].ColumnName);
                    }

                    bool respT2 = createcsvT.addRecordsCSV(pNameT, fieldnamesT);
                    foreach (DataRow row in hnj_ToBcc.Tables[0].Rows)
                    {

                        var rowData = new List<string>();
                        for (int index = 0; index < hnj_ToBcc.Tables[0].Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());

                        }
                        respT2 = false;
                        respT2 = createcsvT.addRecordsCSV(pNameT, rowData);


                    }

                }

                //// copy to CASS
               
                string cassFileName = ProcessVars.gDMPs + BCCname;
                File.Copy(pNameT, cassFileName);

                // // // wait foR 3 min
                var t = Task.Run(async delegate
                {
                    await Task.Delay(1000 * 60 * 1);
                    return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                });
                t.Wait();


                BackCASS processReturns = new BackCASS();
                string result = processReturns.HNJHSAPDProcessFiles(BCCname);


               // string result = "";

                //After bcc check is done 

                if (result == "")
                {

                    //  move data from temptable to master table.

                    try
                    {

                        SqlParameter[] sqlParamsLoadedFileName5 = new SqlParameter[]
                       {                   
                      DBUtility.GetInParameter("@FileName",Filex.Trim()),
                        };

                        DataSet hnj_MasterDSet = dbU.ExecuteDataSet("HNJH_SAPD_MoveDataToMaster", sqlParamsLoadedFileName5);

                    }
                    catch (Exception ex)
                    {
                        errors = errors + 1;
                    }

                    //MOVE ONLY DELEVERABLE TO XMPIE TABLEs-(HNJH_SAPD_Xmpie)


                    try
                    {
                        SqlParameter[] sqlParamsLoadedFileName2 = new SqlParameter[]
                    {                   
                      DBUtility.GetInParameter("@FileName",Filex.Trim()), 
                      
                       
                    };



                        DataSet hnj_XmpieDSet = dbU.ExecuteDataSet("HNJH_SAPD_MoveDataToXmpie", sqlParamsLoadedFileName2);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + 1;
                    }





                    ///Create NoNdELIVERABLE Excel file for horizon

                    SqlParameter[] sqlParamsNonDeliverable = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                                      
                  };

                    DateTime dtToday = DateTime.Now;
                    string s2 = dtToday.ToString("yyyyMMddHHmmss");


                    string AVName = z + "_AV_" + s2 + ".xlsx";

                    string Horizondirectory = @ProcessVars.NJHSAPDDirectory + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\" + "Horizon";
                    if (!Directory.Exists(Horizondirectory))
                        Directory.CreateDirectory(Horizondirectory);




                    string AVFileName = Horizondirectory + "\\" + AVName;

                    if (File.Exists(AVFileName))
                        File.Delete(AVFileName);
                    DataSet hnj_ACV = dbU.ExecuteDataSet("HNJH_SAPD_CreateAcvTableForNJ", sqlParamsNonDeliverable);
                    if (hnj_ACV.Tables[0].Rows.Count > 0)
                    {

                        ClassExcel excelcreate = new ClassExcel();
                        excelcreate.createExcelFile(hnj_ACV, Horizondirectory + "\\" + AVName);
                        // ExcelLibrary.DataSetHelper.CreateWorkbook(AVFileName, hnj_ACV);


                    }






                    if (errors == 0)
                    {

                        try
                        {

                            var recnumFromTemp = dbU.ExecuteScalar("select max(recnum) from HNJH_SAPD_Temp");

                            dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Convert.ToInt64(recnumFromTemp.ToString())) + ",'HNJH_SAPD', GETDATE())");



                        }
                        catch (Exception EX)
                        {
                            errors = errors + 1;
                        }

                    }
                }



            }
            return "";
        }

                public string GetMonthName(string anniversarydatemonth)
                {
                    string MonthVal = anniversarydatemonth.Substring(0, 2);

                    string result = "";

                    switch (MonthVal)
                    {
                        case "01":
                            result = "JAN";
                            break;
                        case "02":
                            result = "FEB";
                            break;
                        case "03":
                            result = "MAR";
                            break;

                        case "04":
                            result = "APR";
                            break;
                        case "05":
                            result = "MAY";
                            break;
                        case "06":
                            result = "JUN";
                            break;
                        case "07":
                            result = "JULY";
                            break;
                        case "08":
                            result = "AUG";
                            break;
                        case "09":
                            result = "SEP";
                            break;
                        case "10":
                            result = "OCT";
                            break;
                        case "11":
                            result = "NOV";
                            break;
                        case "12":
                            result = "DEC";
                            break;

                        default:
                            result = "default";
                            break;
                    };

                    return result;

                }

      


            
         

        public string evaluate_IDCards(string fileName, string insertName, string directoryTXT, bool nozip, string insertBBB, bool testProcess)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            Recnum = 1;
            //DataTable pullOuts = dbU.ExecuteDataTable("select code from HOR_parse_ID_Cards_Pull where CONVERT(DATE,InputDate)='" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'");
            DataTable pullOuts = dbU.ExecuteDataTable("select code from HOR_parse_ID_Cards_Pull where CONVERT(DATE,InputDate) > '2016-02-28' and processDate is null");  

            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;


            if (!File.Exists(ProcessVars.gmappingFile))
                throw new Exception("Mapping file not found.");

            List<List<Field>> records =
                ParseFile(fileName);

            DataTable table = new DataTable();

            List<Field> fields = GetFields();
            foreach (Field field in fields)
            {

                table.Columns.Add(field.Name);
            }

            foreach (List<Field> record in records)
            {
                var row = table.NewRow();

                foreach (Field field in record)
                {
                    row[field.Name] = field.Value;

                }
                table.Rows.Add(row);
            }
            table.Columns.Add("Recnum");
            table.Columns.Add("Timestamp");
            table.Columns.Add("Insert_Prev");
            table.Columns.Add("Insert");
            table.Columns.Add("Grp_Bundle");
            table.Columns.Add("DL");
            table.Columns.Add("Med_Flag");
            table.Columns.Add("GroupChain");
            table.Columns.Add("Type");

            DateTime dt = DateTime.Now;
            string s = dt.ToString("yyyyMMddHHmmss");
            FileInfo fileInfo = new System.IO.FileInfo(fileName);

            bool isOmnia = fileInfo.Name.ToUpper().Contains("OMNIA") ? true : false;
            bool isTMED = fileInfo.Name.ToUpper().Contains("TMED") ? true : false;
            foreach (DataRow row in table.Rows) // Loop over the rows.
            {

                row["Recnum"] = GRecnum;
                GRecnum++;
                row["Timestamp"] = s;
                if (row["Admin Name"].ToString().IndexOf("SEND WITH PREVIOUS CARD") == 0)
                    row["Insert_Prev"] = "Y";
                else
                    row["Insert_Prev"] = "N";
                row["Insert"] = insertName;   // "CMC0007154(0415)";   external parameter
                //row["Grp_Bundle"] = "N";
                row["DL"] = "";
                //row["Med_Flag"] = "N";
                row["GroupChain"] = row["CodeGrp1"].ToString().Trim() + "-" + row["CodeGrp2"].ToString().Trim();
                string type = row["GroupID"].ToString().Substring(2, 4);
                string ResultType = "";
                if (type == "2633")
                    ResultType = "S";
                if (isTMED)
                {
                    ResultType = "T";
                }
                if (isOmnia)
                {
                    if (type != "2633")
                    {
                        ResultType = "O";
                        row["Insert"] = "CMC0007287d (1215)";
                    }
                }
                row["Type"] = ResultType;

                //else if (type == "2243")
                //    ResultType = "O";

                //row["Type"] = ResultType;
                //if (isOmnia && ResultType == "O")
                //    row["Insert"] = "CMC0007287d (1215)";
                //if (fileName.IndexOf("_DLR_") != -1)
                //    row["Med_Flag"] = "Y";
            }
            //==    print temp data===================================================================
            createCSV createcsvT = new createCSV();
            string pNameT = fileName.Substring(0, fileName.Length - 4) + "_data.csv";
            if (File.Exists(pNameT))
                File.Delete(pNameT);
            var fieldnamesT = new List<string>();
            for (int index = 0; index < table.Columns.Count; index++)
            {
                fieldnamesT.Add(table.Columns[index].ColumnName);
            }
            bool respT = createcsvT.addRecordsCSV(pNameT, fieldnamesT);
            foreach (DataRow row in table.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < table.Columns.Count; index++)
                {
                    rowData.Add(row[index].ToString());
                }
                respT = false;
                respT = createcsvT.addRecordsCSV(pNameT, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            //========================================================================



            getfilesTXT(directoryTXT);


            string test = "";
            if (DataTable.Rows.Count > 0)
            {
               
                foreach (DataRow row in table.Rows)
                {
                    if (row["GroupChain"].ToString().Trim() != "")
                    {
                        if (row["Member ID"].ToString().Trim() == "3HZN37974380")
                        {

                            test = "";
                        }
                        DataRow[] result = DataTable.Select("Code= '" + row["Member ID"].ToString().Trim() + "' AND Group = '" + row["GroupChain"].ToString().Trim() + "'");

                        if (result.Count() > 0)
                        {
                            if (result.Count() == 1)
                            {
                                foreach (DataRow rowR in result)
                                {
                                    if (rowR["MarkUnique"].ToString() == "")
                                    {
                                        row["Grp_Bundle"] = rowR[3];
                                        row["Med_Flag"] = rowR[4];
                                    }
                                    else
                                    {
                                        test = "";
                                    }
                                }

                            }
                            else
                            {
                                test = "";
                                foreach (DataRow rowR in result)
                                {
                                    test = test + "  " + rowR[0];

                                }
                                test = test + "____" + row["Member ID"].ToString().Trim() + " " + row["GroupChain"].ToString().Trim() + "\n\n";
                            }

                        }

                    }
                }


                //===================================

                DataTable tableNew = table.Clone();

                //foreach (DataRow r in table.Rows)  //loop through the columns. 
                //{
                //    //string val = row.ColumnName;
                //    DataRow[] result = DataTable.Select("Code= '" + r["Member ID"].ToString().Trim() + "' AND CovCode = '" + r["Code2"].ToString().Trim() + "'");
                //    foreach (DataRow row in result)
                //    {
                //        Console.WriteLine("{0}, {1}", row[0], row[1]);
                //    }


                //}
                foreach (DataRow drtableOld in table.Rows)
                {
                    if (drtableOld["Member ID"].ToString() == "3HZN59939670")
                    {
                        tableNew.ImportRow(drtableOld);
                    }
                }
                DataTable tableNew2 = DataTable.Clone();
                foreach (DataRow drtableOld in DataTable.Rows)
                {
                    if (drtableOld["Code"].ToString() == "3HZN59939670") //&& drtableOld["CovCode"].ToString() == "G978")
                    {
                        tableNew2.ImportRow(drtableOld);
                    }
                }

                //==================
            }
            table.Columns.Remove("CodeGrp2");
            table.Columns.Remove("CodeGrp1");
            table.Columns.Remove("GroupChain");
            
             //FileInfo fileInfo = new System.IO.FileInfo(fileName);


            //mark pull outs
            foreach (DataRow dr in table.Rows)
            {
                string valuetosearch = dr["Member ID"].ToString();
                foreach (DataRow drS in pullOuts.Rows)
                {
                    if(drS["code"].ToString() == valuetosearch)
                    {
                        dr["DL"] = "N";
                           //                     dbU.ExecuteScalar("update HOR_parse_ID_Cards_Pull set ProcessDate = GETDATE(), Recordnum = " +
                           //dr["Recnum"] + ", filename = '" + fileInfo.Name + "' where CONVERT(DATE,InputDate)='" + DateTime.Now.ToString("yyyy-MM-dd") + "' and code = '" + drS["code"].ToString() + "'");


                        dbU.ExecuteScalar("update HOR_parse_ID_Cards_Pull set ProcessDate = GETDATE(), Recordnum = " +
                           dr["Recnum"] + ", filename = '" + fileInfo.Name + "' where code = '" + drS["code"].ToString() + "'");
                    }
                }
            }


            List<int> primes = new List<int>(new int[] { 23, 24, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 25, 26, 27, 28, 29,30 });

           
            createCSV createcsv = new createCSV();
            //string pName = System.IO.Directory.GetParent(directoryTXT).FullName + @"\" + fileInfo.Name.Substring(0, fileInfo.Name.Length - 4) + ".csv";
            string pName = fileName.Substring(0, fileName.Length - 4) + ".csv";
            if (File.Exists(pName))
                File.Delete(pName);
            var fieldnames = new List<string>();
            foreach (int number in primes)
            {
                fieldnames.Add(table.Columns[number].ColumnName);
            }
            bool resp = createcsv.addRecordsPipe_CSV(pName, fieldnames);
            //bool resp = createcsv.addRecordsCSV(pName, fieldnames);
            DataTable dataToSql = table.Copy();
            foreach (DataRow oRow in table.Rows)            // check datatoSQL......................................
            {
                var rowData = new List<string>();
                foreach (int number in primes)
                {
                    rowData.Add(oRow[number].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsPipe_CSV(pName, rowData);
                
                //resp = createcsv.addRecordsCSV(pName, rowData);
            }
            string colnames = "";
            for (int index = 0; index < dataToSql.Columns.Count; index++)
            {
                string colname = dataToSql.Columns[index].ColumnName;
                colnames = colnames + ", [" + colname + "]";
            }
            if (!testProcess)
            {
                string zipName = "";
                DirectoryInfo originalZIP = new DirectoryInfo(directoryTXT);
                if (!nozip)
                {
                    //include CSV in Zip
                    //DirectoryInfo originalZIP = new DirectoryInfo(directoryTXT);
                    FileInfo[] FileZIP = originalZIP.GetFiles("*.zip");
                    zipName = "";
                    if (FileZIP.Count() == 1)
                        zipName = FileZIP.First().ToString();
                    ZipFiles zipfiles = new ZipFiles();
                    string resultZ = zipfiles.AddFilestoZip(directoryTXT + "\\" + zipName, pName);
                    if (insertBBB != "")
                        resultZ = zipfiles.AddFilestoZip(directoryTXT + "\\" + zipName, insertBBB);
                    // copy zip to network
                    string NDirectory = @"\\freenas\Clients\Horizon BCBS\ID Cards\SECURE DATA\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd");
                    string Network_pName = NDirectory + "\\" + zipName;
                    if (!Directory.Exists(NDirectory))
                        Directory.CreateDirectory(NDirectory);

                    if (File.Exists(Network_pName))
                        File.Delete(Network_pName);
                    File.Copy(directoryTXT + "\\" + zipName, Network_pName);
                    // upload zip
                    N_loadFromFTP uploadZip = new N_loadFromFTP();

                    //check file name was blank.................
                    string resultUpload = uploadZip.uploadftp(zipName, directoryTXT + "\\" + zipName, 2, "/ID_Cards/", 1, 1);
                    LogWriter logEndProcess = new LogWriter();
                    logEndProcess.WriteLogToTable("end of upload", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "upload return: " + resultUpload, "Files" + zipName);
                }
                int TotRecnum = dataToSql.Rows.Count;
                GlobalVar.dbaseName = "BCBS_Horizon";
                dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                dbU.ExecuteScalar("delete from HOR_parse_TMP_Maintenance_ID_Cards");

                int errors = 0;
                string recnumError = "";
                string insertCommand1 = "Insert into HOR_parse_TMP_Maintenance_ID_Cards([FileName],[ImportDate]" + colnames + ") VALUES ('";
                foreach (DataRow row in dataToSql.Rows)
                {
                    string insertCommand2 = fileInfo.Name + "', GETDATE(),'";
                    for (int index = 0; index < dataToSql.Columns.Count; index++)
                    {
                        insertCommand2 = insertCommand2 + row[index].ToString().Replace("'", "''") + "','";
                    }
                    try
                    {
                        recnumError = row[0].ToString();
                        var resultSql = dbU.ExecuteScalar(insertCommand1 + insertCommand2.Substring(0, insertCommand2.Length - 2) + ")");
                    }
                    catch (Exception ex)
                    {
                        errors++;
                    }
                }
                if (errors == 0)
                {
                    try
                    {
                        dbU.ExecuteScalar("Insert into HOR_parse_Maintenance_ID_Cards select * from HOR_parse_TMP_Maintenance_ID_Cards");
                        dbU.ExecuteScalar("delete from HOR_parse_TMP_Maintenance_ID_Cards");
                        dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum - 1) + ",'HOR_parse_Maintenance_ID_Cards', GETDATE())");
                        // rename directory
                        string dirName = Path.GetFileName(directoryTXT);
                        string pathDir = Directory.GetParent(directoryTXT).ToString();  //"\\\\CIERANT-TAPER\\Clients\\Horizon BCBS\\ID Cards\\SECURE DATA\\2015-10-15"
                        Directory.Move(pathDir + "\\" + dirName, pathDir + "\\_" + dirName);
                        //File.Move(directoryTXT, directoryTXT);
                    }
                    catch (Exception ex)
                    {
                        var msg = ex.Message;
                    }
                }
                try
                {
                    //check if intesert

                    dbU.ExecuteScalar("Insert into HOR_parse_files_to_CASS(RecordsNum, LettersProduced, FileNameCASS, FileName, ImportDate, TableName,DirectoryTo,sysout,jobid,Work_Task,Processed,DateProcess,ZipName ) values(" +
                                               TotRecnum + ",'" + TotRecnum.ToString() + "','" + fileInfo.Name + "_File not to CASS_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + "','" + fileInfo.Name + "','" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "','HOR_parse_Maintenance_ID_Cards" + "','No CASS','No Sysout','No Jobname','Receive','Y','" + GlobalVar.DateofProcess + "','" + zipName + "')");
                }
                catch (Exception ex)
                {
                    string strsqlDup = "select filename , importdate where filename like '%" + fileInfo.Name + "%'";
                    dbU.ExecuteDataTable(strsqlDup);
                    var errorParse = "Dup File???" + ex.Message;
                    errors++;
                }
            }
            return "";
        }



        public void getfilesTXT(string directoryTXT)
        {
            DataTable.Clear();
            strGroup = GroupBundle =  string.Empty;
            DirectoryInfo originalTXTs = new DirectoryInfo(directoryTXT);
            FileInfo[] FilesTXT = originalTXTs.GetFiles("*.TXT");
            foreach (FileInfo file in FilesTXT)
            {
                if (file.Name.IndexOf("IM") == -1)
                {
                    try
                    {
                        string error = getFileTXT(file.FullName);
                        if (error != "")
                            errors = errors + error + "\n\n";
                    }
                    catch (Exception ez)
                    {
                        errors = errors + file + "  " + ez.Message + "\n\n";
                    }
                }

            }

        }
        public string getFileTXT(string fileName)
        {
            string[] namesM = new string[] { "MEDICARE", "HMO", "PPD" , "PDP"};
            //DataTable DataTable = Data_Table();
            bool valueOk = false;
            string line;
            string prevValLine = "";
            int prevline = 0;
            currLine = 0;
            int lineSys = 0;
            Medicare = "";
            Source_M = "";
            bool isIndv = false;

            string sys = "21      ";
            string sysNG = "21      XX";
            string IdentNumber = "3HZN";

            bool inRecord = false;
            FileInfo fileInfo = new System.IO.FileInfo(fileName);

            string pNameTXT = fileName.Substring(0, fileName.Length - 4) + "_Bundle_TXT.csv";

            System.IO.StreamReader file =
               new System.IO.StreamReader(fileName);
            while ((line = file.ReadLine()) != null)
            {
               
                try
                {
                    if (line.IndexOf(sys) == 0 && line.IndexOf(sysNG) == -1)
                    {
                        if (Codes.Count > 0)
                        {
                            //while (Codes.Count < 25)
                            //{
                            //    Codes.Add("");
                            //}
                            addToTable();
                        }

                        
                        inRecord = true;
                        prevline = currLine;
                        lineSys = currLine + 2;
                        strGroup = prevValLine.Substring(prevValLine.IndexOf("GROUP NO: ", 1) + 10, prevValLine.Length - (prevValLine.IndexOf("GROUP NO: ", 1) + 10));
                        //if(Array.IndexOf(namesM, line) != -1)
                        if (namesM.Any(line.Contains))
                        {
                            Medicare = "IMPORTANT PLAN INFORMATION ENCLOSED";
                            Source_M = line.ToString();
                        }
                        else
                        {
                            Medicare = "";
                            Source_M = line.ToString();
                        }
                    }
                    else
                    {
                        prevline = currLine;
                    }
                    if (line.IndexOf(" 1       END OF SECTION") == 0)
                    {
                        if (Codes.Count > 0)
                        {
                            //while (Codes.Count < 25)
                            //{
                            //    Codes.Add("");
                            //}
                            addToTable();
                        }
                        inRecord = false;
                    }
                    if(currLine == lineSys && inRecord)
                    {
                        if (line.IndexOf("MAIL TO INDIVIDUAL") != -1)
                            GroupBundle = "Individual";
                        else
                        {
                            GroupBundle = "Group";
                            seqBundle++;
                        }
                    }
                    if (inRecord && line.IndexOf(IdentNumber) != -1)
                    {
                        Codes.Add(line.Substring(line.IndexOf(IdentNumber, 1),
                            (line.IndexOf(" ", line.IndexOf(IdentNumber, 1) + 1)) - line.IndexOf(IdentNumber, 1)));
                        if (line.Length == 65 || line.Length > 65)
                            CovCodes.Add(line.Substring(60, 5));
                        else
                            CovCodes.Add(line.Substring(60, 4));
                    }

                }
                catch (Exception ex)
                { 
                    CovCodes.Add("error line " + currLine);
                }
                prevValLine = line;

                currLine++;
                if (currLine == 10525)
                    valueOk = true;

            }
            file.Close();

            DataView dv = DataTable.DefaultView;
            dv.Sort = "SeqBundle";
            DataTable sortedDT = dv.ToTable();
            //sortedDT.Columns.Add("MarkUnique", typeof(string));
            string prevValue = "";
            int count = 0;
            int recordnum = 0;
            foreach (DataRow row in sortedDT.Rows)
            {

                if (row["SeqBundle"].ToString() == prevValue)
                { count++; }
                else
                {
                    if (count == 1)
                    {
                        //mark
                        sortedDT.Rows[recordnum]["MarkUnique"] = "X";
                    }
                    count = 0;
                    prevValue = row["SeqBundle"].ToString();
                }
                recordnum++;
            }



            createCSV createcsv = new createCSV();
            if (File.Exists(pNameTXT))
                File.Delete(pNameTXT);
            var fieldnames = new List<string>();
            for (int index = 0; index < sortedDT.Columns.Count; index++)
            {
                fieldnames.Add(sortedDT.Columns[index].ColumnName);
            }
            bool resp = createcsv.addRecordsCSV(pNameTXT, fieldnames);
            foreach (DataRow row in sortedDT.Rows)
            {

                var rowData = new List<string>();
                for (int index = 0; index < sortedDT.Columns.Count; index++)
                {
                    rowData.Add(row[index].ToString());
                }
                resp = false;
                resp = createcsv.addRecordsCSV(pNameTXT, rowData);
                //if (UpdSQL != "")
                //    dbU.ExecuteScalar(UpdSQL + row[0]);
            }
            foreach (DataRow row in sortedDT.Rows)
            {
                if (row["MarkUnique"].ToString() == "X")
                {
                    foreach (DataRow rowDT in DataTable.Rows)
                    {
                        if (row["Line"].ToString() == rowDT["Line"].ToString())
                        {
                            rowDT["MarkUnique"] = "X";
                        }
                    }
                }
            }

            return "";
        }
        public void addToTable()
        {
            for (int i = 1; i <= Codes.Count; i++)
            {
                if (Codes[i - 1].Length > 1)
                {
                    var row = DataTable.NewRow();
                    row["Line"] = currLine;
                    row["Group"] = strGroup;
                    if (GroupBundle == "Group")
                    {
                        row["Bundle"] = GroupBundle;
                        row["SeqBundle"] = seqBundle.ToString("D6") + "_" + DateTime.Now.ToString("yyyyMMdd");
                    }
                    row["Medicare"] = Medicare;
                    row["Code" ] = Codes[i - 1];
                    row["CovCode"] = CovCodes[i - 1];
                    row["Source_M"] = Source_M;
                    DataTable.Rows.Add(row);
                }
            }
             Codes.Clear();
             CovCodes.Clear();
             Source_M = "";

            //var row = DataTable.NewRow();
            //row["Line"] = currLine;
            //row["Group"] = strGroup;
            //row["Boundle"] = GroupBoundle;
            //for (int i = 1; i <= 25; i++)
            //{
            //    row["Code" + i] = Codes[i - 1];
            //}

            //DataTable.Rows.Add(row);
            //Codes.Clear();

        }

        public string searchText2(string valuetosearch, string line, int currline)
        {
            try
            {
                int poscBlank = line.IndexOf(" ", line.IndexOf(valuetosearch) + valuetosearch.Length);
                string seg1 = line.Substring(line.IndexOf(valuetosearch));
                string seg2 = seg1.Substring(0, seg1.IndexOf("DATE")).TrimEnd();
                return seg2;
            }
            catch (Exception ex)
            {
                errorcount++;
                return "error line " + currline;
            }
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
                while (line != null)
                {
                    //Create out record (field collection)
                    List<Field> record = new List<Field>();

                    //Loop through the mapped fields
                    foreach (Field field in fields)
                    {
                        Field fileField = new Field();

                        //Use the mapped field's start and length
                        // properties to determine where in the
                        // line to pull our data from.
                        fileField.Value =
                            line.Substring(field.Start, field.Length);

                        //Set the name of the field.
                        fileField.Name = field.Name;

                        //Add the field to our record.
                        record.Add(fileField);
                    }

                    //Add the record to our record collection
                    records.Add(record);

                    //Read the next line.
                    line = reader.ReadLine();
                }
            }

            //Return all of our records.
            return records;
        }

        private List<Field> GetFields()
        {
            List<Field> fields = new List<Field>();
            XmlDocument map = new XmlDocument();

            //Load the mapping file into the XmlDocument
            map.Load(ProcessVars.gmappingFile);

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
                        Convert.ToInt32(fieldNode.Attributes["Length"].Value) ;

                //Set the field's starting position
                field.Start =
                        Convert.ToInt32(fieldNode.Attributes["Start"].Value) -1;

                //Add the field to the Field list.
                fields.Add(field);
            }

            return fields;
        }

        public string GetResultsWithHyphen(string phnumber)
        {
            String phone = phnumber;
            string countrycode="";
            string Areacode = "";
            string number = "";
            try
            {
                if (phnumber != "")
                {
                    countrycode = phone.Substring(0, 3);
                    Areacode = phone.Substring(3, 3);
                    number = phone.Substring(6);
                    phnumber = countrycode + "-" + Areacode + "-" + number;
                }
                else phnumber = "";
            }
            catch { }
           

           

            return phnumber;
           
        }

        public List<DataTable> SplitTableNj(DataTable originalTable, int batchSize)
        {
            List<DataTable> tables = new List<DataTable>();
            int i = 0;
            int j = 1;
            DataTable newDt = originalTable.Clone();
            newDt.TableName = "Table_" + j;
            newDt.Clear();
            foreach (DataRow row in originalTable.Rows)
            {
                DataRow newRow = newDt.NewRow();
                newRow.ItemArray = row.ItemArray;
                newDt.Rows.Add(newRow);
                i++;
                if (i == batchSize)
                {
                    tables.Add(newDt);
                    j++;
                    newDt = originalTable.Clone();
                    newDt.TableName = "Table_" + j;
                    newDt.Clear();
                    i = 0;
                }



            }
            if (newDt.Rows.Count > 0)
            {
                tables.Add(newDt);
                j++;
                newDt = originalTable.Clone();
                newDt.TableName = "Table_" + j;
                newDt.Clear();

            }
            return tables;
        }





        private static DataTable Data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Line");
            newt.Columns.Add("Group");
            newt.Columns.Add("Bundle");
            newt.Columns.Add("SeqBundle");
            newt.Columns.Add("Medicare");
            newt.Columns.Add("Code");
            newt.Columns.Add("CovCode");
            newt.Columns.Add("MarkUnique");
            newt.Columns.Add("Source_M");
            return newt;
        }
       
    }
}

