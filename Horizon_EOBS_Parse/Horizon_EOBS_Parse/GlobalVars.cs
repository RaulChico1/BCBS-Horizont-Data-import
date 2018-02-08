using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.IO;
using System.Xml;
using System.Xml.Linq;

namespace Horizon_EOBS_Parse
{
    public class fFile
    {
        public string Name { get; set; }
        public string Ext { get; set; }
    }

    //public class fZips
    //{
    //    public string zipName { get; set; }
    //    public string csvNames { get; set; }
    //    public string txtNames { get; set; }
    //}
    //public class AcessfZips
    //{
    //    public static List<fZips> zipsTicket = new List<fZips>();
    //}

    public static class ProcessVars
    {
        public static string gProgramPath = "";
        public static string gxmlLocation = "";
        public static string InputDirectory = "";
        public static string statusDir = "";
        public static string EOWDirectory = "";
        public static string OutputDirectory = "";
        public static string networkDir;
        public static string GBInputDirectory = "";
        public static string GBOutputDirectory = "";

        public static string CBInputDirectory = "";
        public static string CBOutputDirectory = "";

        public static string NoticeDirectory = "";
        public static string oNoticeDirectory = "";

        public static string ChecksDirectory;
        public static string oChecksDirectory;

        public static string NLDirectory;
        public static string oNLDirectory;

        public static string IDCardsDirectory;
        public static string oIDCardsDirectory;

        public static string oInsertBBB;

        public static string IDCardsMDirectory;
        public static string oIDCardsMDirectory;

        public static string NLpdfsDirectory;
        public static string oNLpdfsDirectory;

        public static string gsStart = "";
        public static bool serviceIsrunning = false;
        public static string gHostName = "";
        public static bool gTest = false;
        public static string Prefix = "";


        public static string gErrorLog = "";
        public static string gSmtpClient = "";
        public static string gemailProc = "";
        public static string gemailProc2 = "";
        public static string gemailErr = "";
        public static string gmappingFile = "";

        public static string gmappingFileMRDF = "";
        public static string gDMPs = "";
        public static string gODMPs = "";
        public static string gODMPsMedicaid = "";
        public static string gODMPs_IMB = "";

        public static string gPassword = "";
        public static string gPublicKey = "";

        public static string SourceDataRoster = "";
        public static string SourceDataRosterDir = "";
    }
    public static class GlobalVar
    {
        public static string dbaseName = "";
        public static string connectionKey = "conStrProd";
        public static DateTime DateofProcess;
        public static DateTime DateofFilesToProcess;
        public static DateTime EOWProcess;
        public static string adtLCDS = "";
    }

    public class appSets
    {
        public string checkDrives()
        {

            string errorsIndrives = "";
            try
            {
                if (!Directory.Exists(ProcessVars.gDMPs))
                    errorsIndrives = "BCC not running, Watched Directory does not exist: " + ProcessVars.gDMPs + Environment.NewLine;
                if (!Directory.Exists(ProcessVars.gODMPs))
                    errorsIndrives = errorsIndrives + "BCC not running, Results Directory does not exist: " + ProcessVars.gODMPs + Environment.NewLine;
                
            }
            catch (Exception ex)
            {
                errorsIndrives = errorsIndrives + ex.Message;
            }
            finally
            {
                if (errorsIndrives != "")
                {
                    string testFName = "99_HZService_ERROR_checkDrives_" + DateTime.Now.ToString("MM_dd_yyyy__HH_mm_ss") + ".txt";
                    if (File.Exists(ProcessVars.statusDir + testFName))
                        File.Delete(ProcessVars.statusDir + testFName);
                    File.WriteAllText(ProcessVars.statusDir + testFName, "Error at " + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") +
                    Environment.NewLine + "Process date " + GlobalVar.DateofFilesToProcess.ToString("yyyy-MM-dd") +
                    Environment.NewLine + "Error: " + errorsIndrives);


                    SendMails sendmail = new SendMails();
                    for (int i = 0; i < 10; i++)
                    {
                        sendmail.SendMailFatalError("Horizon_Checking Directories", "ErrorinProcess", "\n\n" + "Error " + errorsIndrives, "");
                    }
                }


            }
            return errorsIndrives;
        }
        public void getDateProcess()
        {
            //GlobalVar.DateofProcess = DateTime.Today.AddDays(0);
            //GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-1);
            switch (System.DateTime.Today.DayOfWeek)
            {
                case DayOfWeek.Saturday:
                    GlobalVar.DateofProcess = DateTime.Today.AddDays(+2);
                    GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-1);
                    GlobalVar.EOWProcess = DateTime.Today.AddDays(0);
                    break;
                case DayOfWeek.Sunday:
                    GlobalVar.DateofProcess = DateTime.Today.AddDays(+1);
                    GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-2);
                    GlobalVar.EOWProcess = DateTime.Today.AddDays(0);
                    break;
                case DayOfWeek.Monday:
                    GlobalVar.DateofProcess = DateTime.Today;
                    GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-3);
                    break;

                default:
                    if (DateTime.Now.Hour > 19)
                    {
                        GlobalVar.DateofProcess = DateTime.Today.AddDays(0);
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(0);
                    }
                    else
                    {
                        GlobalVar.DateofProcess = DateTime.Today;
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-1);
                    }

                    break;
            }
            

        }
        public void setVars()
        {

            //List<string> fileNamesImport = new List<string>();
            getDateProcess();
            

            ProcessVars.gProgramPath = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
            ProcessVars.gxmlLocation = ProcessVars.gProgramPath + "\\Parse.xml";
            ProcessVars.gmappingFile = ProcessVars.gProgramPath + "\\IDCards_Map.xml";
            ProcessVars.gmappingFileMRDF = ProcessVars.gProgramPath + "\\MRDF.xml";

            XDocument xDoc = XDocument.Load(ProcessVars.gxmlLocation);
            var name = from nm in xDoc.Root.Elements("Server")
                       select nm;
            foreach (XElement xEle in name)
            {
                //ProcessVars.InputDirectory = xEle.Element("IDirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                //ProcessVars.OutputDirectory = xEle.Element("ODirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                ProcessVars.EOWDirectory = xEle.Element("IDirectory").Value + GlobalVar.EOWProcess.ToString("yyyy-MM-dd") + @"\";
                ProcessVars.statusDir = @"\\criticalapps\Horizon\Service_2am\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\"; 
                
                ProcessVars.InputDirectory = xEle.Element("IDirectory").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                ProcessVars.OutputDirectory = xEle.Element("ODirectory").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                ProcessVars.networkDir = xEle.Element("networkDir").Value;
                ProcessVars.GBInputDirectory = xEle.Element("GBILLDirectory").Value;
                ProcessVars.GBOutputDirectory = xEle.Element("OGBILLDirectory").Value;
                ProcessVars.CBInputDirectory = xEle.Element("CBILLDirectory").Value;
                ProcessVars.CBOutputDirectory = xEle.Element("OCBILLDirectory").Value;
                ProcessVars.NoticeDirectory = xEle.Element("NoticeDirectory").Value;
                ProcessVars.oNoticeDirectory = xEle.Element("ONoticeDirectory").Value;

                ProcessVars.ChecksDirectory = xEle.Element("ChecksDirectory").Value;
                ProcessVars.oChecksDirectory = xEle.Element("OChecksDirectory").Value;

                ProcessVars.NLDirectory = xEle.Element("NLDirectory").Value;
                ProcessVars.oNLDirectory = xEle.Element("ONLDirectory").Value;

                ProcessVars.IDCardsMDirectory = xEle.Element("IDCardsMDirectory").Value;
                ProcessVars.oIDCardsMDirectory = xEle.Element("OIDCardsMDirectory").Value;

                ProcessVars.oInsertBBB = xEle.Element("insertBBB").Value;

                ProcessVars.IDCardsDirectory = xEle.Element("IDCardsDirectory").Value;
                ProcessVars.oIDCardsDirectory = xEle.Element("OIDCardsDirectory").Value;
                ProcessVars.gDMPs = xEle.Element("DMPsDirectory").Value;
                ProcessVars.gODMPs = xEle.Element("oDMPsDirectory").Value;
                ProcessVars.gODMPsMedicaid = xEle.Element("oDMPsDirectoryM").Value;
                ProcessVars.gODMPs_IMB = xEle.Element("oDMPSimb").Value;

                ProcessVars.NLpdfsDirectory = xEle.Element("pdfDirectory").Value;
                ProcessVars.oNLpdfsDirectory = xEle.Element("opdfDirectory").Value;


                ProcessVars.gErrorLog = ProcessVars.gProgramPath + xEle.Element("ErrorLog").Value;
                ProcessVars.gsStart = xEle.Element("sTimerHHMM").Value;
                //ProcessVars.gftpServer = xEle.Element("ftpServer").Value;
                //ProcessVars.gftpSubDirTo = xEle.Element("ftpSubDirTo").Value;
                ////ProcessVars.gftpEkey = xEle.Element("ftpekey").Value;
                //ProcessVars.gftpUserID = xEle.Element("ftpUserID").Value;
                ProcessVars.gemailProc = xEle.Element("processEmail").Value;
                ProcessVars.gemailProc2 = xEle.Element("processEmail2").Value;
                ProcessVars.gemailErr = xEle.Element("errorEmail").Value;
                //ProcessVars.gPassword = xEle.Element("dPassword").Value;
                //ProcessVars.gPublicKey = xEle.Element("dPublicKey").Value;
                //ProcessVars.gPrivateKeyOnly = xEle.Element("dPrivateKeyOnly").Value;
                ProcessVars.gSmtpClient = xEle.Element("SmtpClient").Value;
                ProcessVars.SourceDataRoster = xEle.Element("SourceDataRoster").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\Panel_Roster";
                ProcessVars.SourceDataRosterDir = xEle.Element("SourceDataRoster").Value;


                if (xEle.Element("Test").Value == "Y")
                {
                    ProcessVars.gTest = true;
                    ProcessVars.Prefix = "TEST ";
                }
                else
                {
                    ProcessVars.gTest = false;
                    ProcessVars.Prefix = "";
                }

            }

        }

        
        
        public IEnumerable<fFile> getFilesImport(String option)
        {
              //var f = XDocument.Load(ProcessVars.gxmlLocation);

              //var names = f.Descendants("ElementDefinition").Elements("Name").Select(e => e.Value).ToList();
              //  List<string> fnames = new List<string>(names);
                
           //return fnames;


            //XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
            //IEnumerable<fFile> iFiles =
            //    from s in xdoc.Descendants("FilesToImport2")
            //    //.Where(s => s.Attribute("cycle").Value == "2")
            //    select new fFile()
            //    {
            //        Name = (string)s.Element("ElementDefinition").Element("Name").Value, // you can cast to int
            //        Ext = (string)s.Element("ElementDefinition").Element("type").Value
            //    };
            //return iFiles;
            if (option == "Ticket1")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementT1")
                    select new fFile()
                    {
                        Name = (string)s.Element("Name"), // you can cast to int
                        Ext = (string)s.Element("type")
                    };
                return iFiles;
            }
            if (option == "Ticket2")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementT2")
                    select new fFile()
                    {
                        Name = (string)s.Element("Name"), // you can cast to int
                        Ext = (string)s.Element("type")
                    };
                return iFiles;
            }
            if (option == "Ticket2s")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementT2s")
                    select new fFile()
                    {
                        Name = (string)s.Element("Name"), // you can cast to int
                        Ext = (string)s.Element("type")
                    };
                return iFiles;
            }

            else if (option == "IDCards")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementCards")
                    select new fFile()
                    {
                        Name = (string)s.Element("Name"), // you can cast to int
                        Ext = (string)s.Element("type")
                    };
                return iFiles;
            }
            else if (option == "MRDF")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementMRDF")
                    select new fFile()
                    {
                        Name = (string)s.Element("Name"), // you can cast to int
                        Ext = (string)s.Element("type")
                    };
                return iFiles;
            }
            else
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementCards")
                    select new fFile()
                    {
                        Name = (string)s.Element("Name"), // you can cast to int
                        Ext = (string)s.Element("type")
                    };
                return iFiles;
            }
           }
    }
}
