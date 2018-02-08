using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;


namespace Horizon_EOBS_Parse
{
    public class ZipFiles
    {
        DBUtility dbU;

        public string ManuallyCreateZipFile(string gName, string group, out int Txts, out int CSVs)
        {
            string[] arrayEOBS = new string[] { "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X" };
            string[] arraycHS = new string[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "Y", "Z" };
            string[] groups = group.Split(',');

            int totfilesTXT = 0;
            int totfilesCSVs = 0;
            Txts = 0; CSVs = 0;
            string FilesCSV = "";
            string FilesTXT = "";
            DateTime dayZip = GlobalVar.DateofProcess;
             switch (System.DateTime.Today.DayOfWeek)
            {
                case DayOfWeek.Saturday:
                    dayZip = GlobalVar.DateofProcess.AddDays(-2);
                    break;
                case DayOfWeek.Sunday:
                    dayZip = GlobalVar.DateofProcess.AddDays(-1);
                    break;
                default:
                    break;
            }

//            string zipName = ProcessVars.InputDirectory + @"FromCASS\" + gName + "-" + GlobalVar.DateofProcess.AddDays(-2).ToString("yyyyMMdd") + ".zip";
             string zipName = ProcessVars.InputDirectory + @"FromCASS\" + gName + "-" + dayZip.ToString("yyyyMMdd") + ".zip";

            //zipName = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\2015-08-31\FromCASS\test_upload_CONBILL-20150831.zip";

            if (File.Exists(zipName))
                File.Delete(zipName);
            List<string> filesToArchive = new List<string>();

            DirectoryInfo originaCSVs = new DirectoryInfo(ProcessVars.InputDirectory + @"\FromCASS");
            DirectoryInfo originaTXTs = new DirectoryInfo(ProcessVars.InputDirectory + @"\Decrypted");

            try
            {
                //FileInfo[] files = originaCSVs.GetFiles(group + "*.csv");
                FileInfo[] files = originaCSVs.GetFiles("*.csv");
                FileInfo[] filesT = originaTXTs.GetFiles("*.txt");

                //Creates a new, blank zip file to work with - the file will be
                //finalized when the using statement completes

                using (ZipArchive newFile = ZipFile.Open(zipName, ZipArchiveMode.Create))
                {
                    foreach (string word in groups)
                    {
                        List<string> cvsZipped = new List<string>();
                        switch (gName)
                        {
                            case "EOBS":
                                {

                                    foreach (FileInfo file in files)
                                    {
                                        if (word == "UCDS")
                                        {
                                            string Typef = file.Name.ToUpper().ToString().Substring(4, 1);
                                            if (arrayEOBS.Any(Typef.Contains) && file.Name.IndexOf(word) == 0)
                                            {
                                                newFile.CreateEntryFromFile(file.FullName, file.Name);
                                                totfilesCSVs++;
                                                cvsZipped.Add(file.Name.Substring(0, file.Name.Length - 4));
                                                FilesCSV = FilesCSV + file.Name.ToString() + "~";
                                            }
                                        }
                                        else
                                        {
                                            if (file.Name.IndexOf(word) == 0)
                                            {
                                                newFile.CreateEntryFromFile(file.FullName, file.Name);
                                                totfilesCSVs++;
                                                cvsZipped.Add(file.Name.Substring(0, file.Name.Length - 4));
                                                FilesCSV = FilesCSV + file.Name.ToString() + "~";
                                            }
                                        }
                                    }
                                    foreach (FileInfo file in filesT)
                                    {
                                        if (cvsZipped.Any(e => e.Contains(file.Name.Substring(0, file.Name.Length - 4))))
                                        {
                                            newFile.CreateEntryFromFile(file.FullName, file.Name);
                                            totfilesTXT++;
                                            FilesTXT = FilesTXT + file.Name.ToString() + "~";
                                        }
                                    }

                                }
                                break;
                            case "CHECKS":
                            case "CHECKS_Test":
                                {
                                    foreach (FileInfo file in files)
                                    {
                                        if (word == "UCDS")
                                        {
                                            string Typef = file.Name.ToUpper().ToString().Substring(4, 1);
                                            if (arraycHS.Any(Typef.Contains) && file.Name.IndexOf(word) == 0)
                                            {
                                                newFile.CreateEntryFromFile(file.FullName, file.Name);
                                                totfilesCSVs++;
                                                cvsZipped.Add(file.Name.Substring(0, file.Name.Length - 4));
                                                FilesCSV = FilesCSV + file.Name.ToString() + "~";
                                            }
                                        }
                                        else
                                        {
                                            if (file.Name.IndexOf(word) == 0)
                                            {
                                                newFile.CreateEntryFromFile(file.FullName, file.Name);
                                                totfilesCSVs++;
                                                cvsZipped.Add(file.Name.Substring(0, file.Name.Length - 4));
                                                FilesCSV = FilesCSV + file.Name.ToString() + "~";
                                            }
                                        }
                                    }
                                    foreach (FileInfo file in filesT)
                                    {
                                        if (cvsZipped.Any(e => e.Contains(file.Name.Substring(0, file.Name.Length - 4))))
                                        {
                                            newFile.CreateEntryFromFile(file.FullName, file.Name);
                                            totfilesTXT++;
                                            FilesTXT = FilesTXT + file.Name.ToString() + "~";
                                        }
                                    }
                                }
                                break;

                            default:
                                {
                                    foreach (FileInfo file in files)
                                    {
                                        if (file.Name.IndexOf(word) == 0)
                                        {
                                            newFile.CreateEntryFromFile(file.FullName, file.Name);
                                            totfilesCSVs++;
                                            cvsZipped.Add(file.Name.Substring(0, file.Name.Length - 4));
                                            FilesCSV = FilesCSV + file.Name.ToString() + "~";
                                        }
                                    }
                                    foreach (FileInfo file in filesT)
                                    {
                                        if (cvsZipped.Any(e => e.Contains(file.Name.Substring(0, file.Name.Length - 4))))
                                        {
                                            newFile.CreateEntryFromFile(file.FullName, file.Name);
                                            totfilesTXT++;
                                            FilesTXT = FilesTXT + file.Name.ToString() + "~";
                                        }
                                    }
                                }
                                break;
                        }
                    }
                }
                Txts = totfilesTXT;
                CSVs = totfilesCSVs;

                if (totfilesCSVs + totfilesTXT < 1)
                    File.Delete(zipName);
                else
                {
                    int totfiles = totfilesCSVs + totfilesTXT;
                    GlobalVar.dbaseName = "BCBS_Horizon";
                    dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                    dbU.ExecuteNonQuery("Insert into HOR_parse_Log_Zips (Logdate, Type, Zipname, ZipCount, csvNames, txtNames) Values (getdate(),'Ticket01 zip','"  +
                            Path.GetFileName(zipName) + "'," + totfiles + ",'" + FilesCSV + "','" + FilesTXT + "')");
                        
                    //fZips fzips = new fZips { zipName = Path.GetFileName(zipName), csvName = FilesCSV, txtName = FilesTXT };
                    //AcessfZips.zipsTicket.Add(fzips); 
                }
                return zipName;
            }
            catch (Exception e)
            {
                LogWriter logerror = new LogWriter();
                logerror.WriteLogToTable("Error creating zip " + gName, DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Zip", "no files in " + zipName, "email");
                
            }

            return zipName;
        }

        public string ManuallyCreateADTLZipFile(string gName, string group, out int Txts, out int CSVs)
        {
            

            int totfilesTXT = 0;
            int totfilesCSVs = 0;
            string zipName = ProcessVars.InputDirectory + @"adtlLCDS\" + gName + "-" + GlobalVar.DateofProcess.ToString("yyyyMMdd") + ".zip";

            string[] groups = group.Split(',');

            if (File.Exists(zipName))
                File.Delete(zipName);
            List<string> filesToArchive = new List<string>();

            DirectoryInfo originaCSVs = new DirectoryInfo(ProcessVars.InputDirectory + @"\FromCASS");
            DirectoryInfo originaTXTs = new DirectoryInfo(ProcessVars.InputDirectory + @"\Decrypted");
            //FileInfo[] files = originaCSVs.GetFiles(group + "*.csv");
            FileInfo[] files = originaCSVs.GetFiles("*.csv");
            FileInfo[] filesT = originaTXTs.GetFiles("*.txt");

            //Creates a new, blank zip file to work with - the file will be
            //finalized when the using statement completes

            using (ZipArchive newFile = ZipFile.Open(zipName, ZipArchiveMode.Create))
            {
                foreach (string word in groups)
                {
                    int findEXE = word.IndexOf(".");
                    string partialName = word.Substring(0, findEXE); 
                    List<string> cvsZipped = new List<string>();
                                       
                                foreach (FileInfo file in files)
                                {

                                    if (file.Name.Substring(0, file.Name.Length - 4).IndexOf(partialName) == 0)
                                        {
                                            newFile.CreateEntryFromFile(file.FullName, file.Name);
                                            totfilesCSVs++;
                                            cvsZipped.Add(file.Name.Substring(0, file.Name.Length - 4));
                                        }
                                    
                                }
                                foreach (FileInfo file in filesT)
                                {
                                    if (cvsZipped.Any(e => e.Contains(file.Name.Substring(0, file.Name.Length - 4))))
                                    {
                                        newFile.CreateEntryFromFile(file.FullName, file.Name);
                                        totfilesTXT++;
                                    }
                                }
                            
                    
                }
            }
            Txts = totfilesTXT;
            CSVs = totfilesCSVs;
            return zipName;
        }

        public string AddFilestoZip(string zipName, string newfile)
        {
            string result = "";
            try
            {
                FileInfo fileInfo = new System.IO.FileInfo(newfile);
                using (FileStream zipToOpen = new FileStream(zipName, FileMode.Open))
                {
                    using (ZipArchive archive = new ZipArchive(zipToOpen, ZipArchiveMode.Update))
                    {
                        
                        archive.CreateEntryFromFile(newfile, fileInfo.Name);
                        //archive.ExtractToDirectory(directoryTXT);
                        //ZipArchiveEntry readmeEntry = archive.CreateEntry(newfile);
                        //using (StreamWriter writer = new StreamWriter(readmeEntry.Open()))
                        //{
                        //    writer.WriteLine("Information about this package.");
                        //    writer.WriteLine("========================");
                        //}
                    }
                }
                //FileInfo fileInfo2 = new System.IO.FileInfo(newfile.Replace(".csv", "Bundle_Summary.csv"));
                //using (FileStream zipToOpen = new FileStream(zipName, FileMode.Open))
                //{
                //    using (ZipArchive archive = new ZipArchive(zipToOpen, ZipArchiveMode.Update))
                //    {

                //        archive.CreateEntryFromFile(newfile, fileInfo2.Name.Replace(".csv", "Bundle_Summary.csv"));
                       
                //    }
                //}
            }
            catch(Exception ex)
            {
                result = ex.Message.ToString();

            }
            return result;
        }

       


    }
}
