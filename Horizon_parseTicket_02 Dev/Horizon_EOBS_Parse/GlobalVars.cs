using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Data;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{
    public class fFile
    {
        public string Name { get; set; }
        public string Ext { get; set; }
    }

    public class mFile
    {
        public string Name { get; set; }
        public string Dir { get; set; }
        public string Code { get; set; }
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
        public static string gmappingFileHNJH_Panel = "";
        public static string gmappingFileMRDF = "";
        

        public static string gDMPs = "";
        public static string gODMPs = "";
        public static string gODMPsMedicaid = "";
        public static string gODMPs_IMB = "";


        public static string gPassword = "";
        public static string gPublicKey = "";

        public static string uName = "AutomationUser";
        public static string Password = "aut0m8!";
        public static string DocumentIDs = "";

        public static string PR_DocumentID = "1928";
        public static string DestinationID_IDCARDS = "";
        public static string DestinationID_PROSTER = "1008";
        public static string RecipientTable_1928 = "HNJH_RosterProvider_Xmpie";
        public static string AdditionalTable_1928 = "Provider_Panel";
        public static string DataSourceID_1928 = "3476";
        public static string OutputFolderName = "";
        public static string DocumentID_ToProcess = "1928";
        public static string XMpieOut = "";
        public static string SourceDataRoster = "";
        public static string SourceDataRosterDir = "";
        public static string XMPIDataRoster = "";

        public static string RecipientsDataSourceQueryMLTSS = "SELECT [Recnum]  ,[FileName] ,[Timestamp] ,[FileDate]  ,[SEQ#] as SEQ_NO,[Meme_ID] ,[Meme_LastName] as [Mem_LastName]  ,[Meme_FirstName] as [Mem_FirstName],[Member_Name] ,[PCP_Name] ,[PCP_Phone_No]  ,[Meme_Medcd_No] ,[Meme_Addr1]    ,[Meme_Addr2]   ,[Meme_Addr3] ,[Meme_City] ,[Meme_State] ,[Meme_Zip] ,[Meme_Plan],[Meme_Plan_Eff_Dt] ,[Dental_Benefit] as [Dental_Benfit]  ,[Emergency_Amt]  ,[Pcp_CoPay] ,[Dental_CoPay] ,[Specialist_CoPay],[Rx_Generic] ,[Rx_Brand] ,[Source_Id_Card_Req],[Card_Ind] ,[Insert_Prev] ,[Insert]   ,[Form_Id]  ,[Imb]   ,[FileNameXMPie] FROM [dbo].[HNJH_IDCards_Xmpie_MLTSS] order by [SEQ#] ASC";
        public static string RecipientsDataSourceQueryNJFamily = "SELECT [Recnum]  ,[FileName] ,[Timestamp] ,[FileDate]  ,[SEQ#] as SEQ_NO,[Meme_ID] ,[Meme_LastName] as [Mem_LastName]  ,[Meme_FirstName] as [Mem_FirstName],[Member_Name] ,[PCP_Name]    ,[PCP_Phone_No]  ,[Meme_Medcd_No] ,[Meme_Addr1]    ,[Meme_Addr2]   ,[Meme_Addr3] ,[Meme_City] ,[Meme_State] ,[Meme_Zip] ,[Meme_Plan],[Meme_Plan_Eff_Dt] ,[Dental_Benefit] as [Dental_Benfit]  ,[Emergency_Amt]  ,[Pcp_CoPay] ,[Dental_CoPay] ,[Specialist_CoPay],[Rx_Generic] ,[Rx_Brand] ,[Source_Id_Card_Req],[Card_Ind] ,[Insert_Prev] ,[Insert]   ,[Form_Id]  ,[Imb]   ,[FileNameXMPie] FROM [dbo].[HNJH_IDCards_Xmpie] order by [SEQ#] ASC";

        public static string SplittedJobBatchSize = "10000";

        public static string wk_DocumentID = "2038";
        public static string wk_DataSourceID_1880 = "4200";
        public static string wk_RecipientTablet_4200 = "HNJH_WKits_NJH_XMpie_1";
        
        public static string wk_SplittedJobBatchSize = "10000";
        public static string wk_DestinationID_IDCards = "1012";

        public static string dmpsWatched = "";
        public static string oDMPsDirectoryM = "";

        public static string CRprocessed = "";
        public static string OtherProcessed = "";
        //public static string PR_DocumentID = ConfigurationManager.AppSettings["DocumentID_ToProcess"].ToString();
        //public static string RecipientTable_1928 = ConfigurationManager.AppSettings["RecipientTable_1928"].ToString();
        //public static string AdditionalTable_1928 = ConfigurationManager.AppSettings["AdditionalTable_1928"].ToString();
        //public static string DataSourceID_1928 = ConfigurationManager.AppSettings["DataSourceID_1928"].ToString();
        //public static string DestinationID_PROSTER = ConfigurationManager.AppSettings["DestinationID_PROSTER"].ToString();



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
        DBUtility dbU;

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
                    if (DateTime.Now.Hour > 18)
                    {
                        GlobalVar.DateofProcess = DateTime.Today.AddDays(+1);
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-3);
                    }
                    else
                    {
                        GlobalVar.DateofProcess = DateTime.Today;
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-0);
                    }
                    break;

                default:
                    if (DateTime.Now.Hour > 18)
                    {
                    DateTime date = DateTime.Now;
                    string dateToday = date.ToString("d");
                    if (date.DayOfWeek == DayOfWeek.Friday)
                        {
                        GlobalVar.DateofProcess = DateTime.Today.AddDays(+3);
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-1);
                        GlobalVar.EOWProcess = DateTime.Today.AddDays(0);
                        }
                    else
                        {
                        GlobalVar.DateofProcess = DateTime.Today.AddDays(+1);
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(0);
                        }
                    }
                    else
                    {
                    GlobalVar.DateofProcess = DateTime.Today.AddDays(-0);
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-1);
                    }

                    break;
            }
            

        }
        public string checkDrives()
        {

            string errorsIndrives = "";
            try
            {
                if (!Directory.Exists(ProcessVars.dmpsWatched))
                    errorsIndrives = "BCC not running, Watched Directory does not exist: " + ProcessVars.dmpsWatched + Environment.NewLine;
                if (!Directory.Exists(ProcessVars.gODMPsMedicaid))
                    errorsIndrives = errorsIndrives + "BCC not running, Results Directory does not exist: " + ProcessVars.gODMPsMedicaid + Environment.NewLine;
                
            }
            catch (Exception ex)
            {
                errorsIndrives = errorsIndrives + ex.Message;
            }
            finally
            {
                if (errorsIndrives != "")
                {
                    SendMails sendmail = new SendMails();
                    sendmail.SendMailError("Parsing Data_Checking Directories", "ErrorinProcess", "\n\n" + "Error " + errorsIndrives, "");
                }


            }
            return errorsIndrives;
        }
        public void setVars()
        {

            //List<string> fileNamesImport = new List<string>();
            getDateProcess();
            //if(optional == "Test")
            if (ProcessVars.gTest)
                {
                ProcessVars.InputDirectory = ProcessVars.InputDirectory.Replace("DailyFiles", "DailyFilesTEST");
                GlobalVar.connectionKey = "conStrDev";
                }
            string dirrr = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
            ProcessVars.gProgramPath = dirrr;//Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
            ProcessVars.gxmlLocation = ProcessVars.gProgramPath + "\\Parse.xml";
            ProcessVars.gmappingFile = ProcessVars.gProgramPath + "\\IDCards_Map.xml";
            ProcessVars.gmappingFileHNJH_Panel = ProcessVars.gProgramPath + "\\HNJH_Panel.xml";
            ProcessVars.gmappingFileMRDF = ProcessVars.gProgramPath + "\\MRDF.xml";

            ProcessVars.CRprocessed = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\" + GlobalVar.DateofProcess.ToString("MM-dd-yyyy") + @"\CARE RADIUS SENT\";
            ProcessVars.OtherProcessed = @"\\CIERANT-TAPER\Clients\Horizon BCBS\NoticeLetters\" + GlobalVar.DateofProcess.ToString("MM-dd-yyyy") + @"\__Other files processed\";
            try
            {
                Directory.CreateDirectory(ProcessVars.CRprocessed);
                Directory.CreateDirectory(ProcessVars.OtherProcessed);
            }catch (Exception ex)
            {
                var here = "mm";
            }
            XDocument xDoc = XDocument.Load(ProcessVars.gxmlLocation);
            var name = from nm in xDoc.Root.Elements("Server")
                       select nm;
            foreach (XElement xEle in name)
            {
                //ProcessVars.InputDirectory = xEle.Element("IDirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                //ProcessVars.OutputDirectory = xEle.Element("ODirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                ProcessVars.EOWDirectory = xEle.Element("IDirectory").Value + GlobalVar.EOWProcess.ToString("yyyy-MM-dd") + @"\";
                if (ProcessVars.gTest)
                    {
                    ProcessVars.InputDirectory = xEle.Element("IDirectory").Value.Replace("DailyFiles", "DailyFilesTEST") + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                    ProcessVars.OutputDirectory = xEle.Element("ODirectory").Value.Replace("DailyFiles", "DailyFilesTEST") + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                    }
                else
                    {
                    ProcessVars.InputDirectory = xEle.Element("IDirectory").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                    ProcessVars.OutputDirectory = xEle.Element("ODirectory").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                    }
                ProcessVars.networkDir = xEle.Element("networkDir").Value;
                ProcessVars.GBInputDirectory = xEle.Element("GBILLDirectory").Value;
                ProcessVars.GBOutputDirectory = xEle.Element("OGBILLDirectory").Value;
                ProcessVars.CBInputDirectory = xEle.Element("CBILLDirectory").Value;
                ProcessVars.CBOutputDirectory = xEle.Element("OCBILLDirectory").Value;
                ProcessVars.NoticeDirectory = xEle.Element("NoticeDirectory").Value;
                ProcessVars.oNoticeDirectory = xEle.Element("ONoticeDirectory").Value;


                ProcessVars.dmpsWatched = xEle.Element("dmpsWatched").Value;
                //ProcessVars.oDMPsDirectoryM = xEle.Element("oDMPsDirectoryMAS").Value;


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

                ProcessVars.uName = xEle.Element("uProduce_UN").Value;
                ProcessVars.Password = xEle.Element("uProduce_Pwd").Value;
                ProcessVars.DocumentIDs = xEle.Element("DocumentIDToProcess").Value;
                ProcessVars.PR_DocumentID = xEle.Element("DocumentID_ToProcess").Value;

                ProcessVars.DocumentIDs = xEle.Element("DocumentID_ToProcess").Value;
                //ProcessVars.DestinationID_IDCARDS = xEle.Element("DestinationID_IDCards").Value;
                ProcessVars.DestinationID_PROSTER = xEle.Element("DestinationID_PROSTER").Value;
                ProcessVars.OutputFolderName = xEle.Element("OutputFolderName").Value;
                ProcessVars.DataSourceID_1928 = xEle.Element("AdditionalTable_1928").Value;
                ProcessVars.DocumentID_ToProcess = xEle.Element("DocumentID_ToProcess").Value;
                ProcessVars.XMpieOut = xEle.Element("XMpieOut").Value;
                ProcessVars.SourceDataRoster = xEle.Element("SourceDataRoster").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\Panel_Roster";
                ProcessVars.SourceDataRosterDir = xEle.Element("SourceDataRoster").Value;
                ProcessVars.XMPIDataRoster = xEle.Element("XMPIDataRoster").Value;


                //if (xEle.Element("Test").Value == "Y")
                //{
                //    ProcessVars.gTest = true;
                //    ProcessVars.Prefix = "TEST ";
                //}
                //else
                //{
                //    ProcessVars.gTest = false;
                //    ProcessVars.Prefix = "";
                //}

            }

        }

        public IEnumerable<mFile> getFilesMisc()
        {
             GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable datamFiles = dbU.ExecuteDataTable("select MiscName, MiscDir, Code from HOR_parse_Category_Master where MiscName is not null and miscDir is not null and Category = 'Misc' order by MiscName");

            return datamFiles.AsEnumerable().Select(row =>
       {
           return new mFile
           {
               Name = row["MiscName"].ToString(),
               Dir = row["MiscDir"].ToString(),
               Code = row["Code"].ToString()

           };
       });

           // IEnumerable<mFile> mFiles = from p in datamFiles.AsEnumerable() 
           //                             .Select(p => new {

           //             Name = p.Field<string>("MiscName"),
           //             Dir = p.Field<string>("MiscDir"),
           //             Code = p.Field<string>("Code")
           //                 });

           //return null;
        }
        
        public IEnumerable<fFile> getFilesImport(String option, string notCRNJLRT = "Y")
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
            if (option == "Ticket2Control")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementT2C")

                    select new fFile()
                    {
                        Name = (string)s.Element("Name"), // you can cast to int
                        Ext = (string)s.Element("type")

                    };
                return iFiles;
            }
            //Ticket2Priority
            if (option == "Ticket2Priority")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementP")

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
            else if (option == "PLANS")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementPLANS")
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
