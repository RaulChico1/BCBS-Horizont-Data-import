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
        public static DBUtility dbU;
        public static DBUtility dbU_169;

        public static string MODE = "prod";


        public static string gProgramPath = "";
        public static string gxmlLocation = "";

        public static string appPath = "";

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
        public static string gmappingFileHNJHD = "";

        public static string gDMPs = "";
        public static string gODMPs = "";
        //njh id cards
        public static string gHNJHODMPs = ConfigurationManager.AppSettings["oHNJHDMPsDirectory"];//O/P FROM BCC     oHNJHDMPsDirectory


        public static string gHNJHSAPDODMPs = ConfigurationManager.AppSettings["oHNJHSAPDDMPsDirectory"];//O/P FROM BCC     oHNJHDMPsDirectory
        public static string gHNJHProdInLocal = "";//PROD//iNBOUND FROM FTP
        public static string gHNJHTESTInLocal = ""; //TEST//INBOUND FROM FTP



        public static string NJHIDCardsDirectory = "";
        public static string NJHSAPDDirectory = "";
        public static string NJHIDDsnpCardsDirectory = "";
        public static string gHNJHIDCards_FTP_URL = ConfigurationManager.AppSettings["NJHID_FTPURL"];



        //Test

        public static string gHNJHIDCards_FTPLocationTest = "";
        public static string gHNJHIDCards_FTPUserNameTest = "";
        public static string gHNJHIDCards_FTPPwdTest = "";
        //  public static string NJHIDCardsDirectoryTest="";




        //Prod
        //  public static string NJHIDCardsDirectoryProd ="";
        public static string gHNJHIDCards_FTPLocationProd = "";
        public static string gHNJHIDCards_FTPUserNameProd = "";
        public static string gHNJHIDCards_FTPPwdProd = "";

        public static string gHNJHIDCardsPdfLoc = "";
        public static string gPassword = "";
        public static string gPublicKey = "";
        public static string ConnectionString = "";
        public static string ConnectionString_169 = "";
        public static string uName = ConfigurationManager.AppSettings["uProduce_UN"].ToString();
        public static string Password = ConfigurationManager.AppSettings["uProduce_Pwd"].ToString();
        public static string DocumentIDs = ConfigurationManager.AppSettings["DocumentIDToProcess"].ToString();
        public static string PR_DocumentID = ConfigurationManager.AppSettings["DocumentID_ToProcess"].ToString();
        public static string DSNPDocumentId = ConfigurationManager.AppSettings["DocumentIDDsnpToProcess"].ToString();




        //SAPD
        public static string SAPD_DocumentID = ConfigurationManager.AppSettings["DocumentIDSapd_ToProcess"].ToString();
        public static string DestinationID_SAPD = ConfigurationManager.AppSettings["DestinationID_Sapd"].ToString();
        public static string DataSourceID_2010 = ConfigurationManager.AppSettings["DataSourceIDSapd_2010"].ToString();
        public static string XmpieSAPDpdfPath = ConfigurationManager.AppSettings["XmpieSAPDpdfPath"].ToString();
        public static string XmpieSAPDpdfProcessedPath = ConfigurationManager.AppSettings["XmpieSAPDpdfProcessedPath"].ToString();
        public static string OutputFolderNameSAPD = ConfigurationManager.AppSettings["OutputFolderNameSAPD"].ToString();

        //IDCARDS
        public static string JobIDs = "";
        public static string DestinationID_IDCARDS = ConfigurationManager.AppSettings["DestinationID_IDCards"].ToString();
        public static string RecipientsDataSourceQueryNJFamily = ConfigurationManager.AppSettings["RecipientsDataSourceQueryNJFamily"].ToString();
        public static string RecipientsDataSourceQueryMLTSS = ConfigurationManager.AppSettings["RecipientsDataSourceQueryMLTSS"].ToString();

        

        public static string RecipientsDataSourceQueryDSNP = ConfigurationManager.AppSettings["RecipientsDataSourceQueryDSNP"].ToString();





        public static string OutputFolderName = ConfigurationManager.AppSettings["OutputFolderName"].ToString();
        public static string DataSourceID_1880 = ConfigurationManager.AppSettings["DataSourceID_1880"].ToString();
        public static string DataSourceID_1882 = ConfigurationManager.AppSettings["DataSourceID_1882"].ToString();
        public static string DataSourceID_2764 = ConfigurationManager.AppSettings["DataSourceID_2764"].ToString();


        public static string SplittedJobBatchSize = ConfigurationManager.AppSettings["SplittedJobBatchSize"].ToString();

        public static string NJHIDCsvFormatNjCard = ConfigurationManager.AppSettings["NJHIDCsvFormatNjCard"].ToString();
        public static string NJHIDPdfFormatNjCard = ConfigurationManager.AppSettings["NJHIDPdfFormatNjCard"].ToString();
        public static string NJHIDCsvFormatMLTSS = ConfigurationManager.AppSettings["NJHIDCsvFormatMLTSS"].ToString();
        public static string NJHIDPdfFormatMLTSS = ConfigurationManager.AppSettings["NJHIDPdfFormatMLTSS"].ToString();

        public static string NJHIDDSNPCsvFormat = ConfigurationManager.AppSettings["NJHIDDSNPCsvFormat"].ToString();
        public static string NJHIDDSNPPdfFormat = ConfigurationManager.AppSettings["NJHIDDSNPPdfFormat"].ToString();


        public static string XmpiepdfPath = ConfigurationManager.AppSettings["XmpiepdfPath"].ToString();
        public static string XmpiepdfProcessedPath = ConfigurationManager.AppSettings["XmpiepdfProcessedPath"].ToString();



        public static DBUtility oDBUtility_169()
        {
            if (MODE.ToLower().Equals("prod"))
            {
                dbU_169 = new DBUtility("conStrProd_169", DBUtility.ConnectionStringType.Configured);
                ConnectionString_169 = ConfigurationManager.ConnectionStrings["conStrProd_169"].ToString();
            }
            return dbU_169;
        }


        public static DBUtility oDBUtility()
        {


            if (MODE.ToLower().Equals("test"))
            {
                dbU = new DBUtility("conStrTest", DBUtility.ConnectionStringType.Configured);
                ConnectionString = ConfigurationManager.ConnectionStrings["conStrTest"].ToString();
                gHNJHTESTInLocal = ConfigurationManager.AppSettings["NJH_LocalFileDirTest"].ToString();
                gHNJHIDCards_FTPLocationTest = ConfigurationManager.AppSettings["NJHID_FTPLocation_test"].ToString();
                gHNJHIDCards_FTPUserNameTest = ConfigurationManager.AppSettings["NJHID_FTPUserName_test"].ToString();
                gHNJHIDCards_FTPPwdTest = ConfigurationManager.AppSettings["NJHID_FTPPwd_test"].ToString();
                //  NJHIDCardsDirectoryTest = ConfigurationManager.AppSettings["NJHIDCardsDirectoryTest"];
                NJHIDCardsDirectory = ConfigurationManager.AppSettings["NJHIDCardsDirectoryTest"];
                NJHSAPDDirectory = ConfigurationManager.AppSettings["NJHSAPDDirectoryTest"];
            }
            else if (MODE.ToLower().Equals("prod"))
            {


                dbU = new DBUtility("conStrProd", DBUtility.ConnectionStringType.Configured);
              
                ConnectionString = ConfigurationManager.ConnectionStrings["conStrProd"].ToString();
                gHNJHProdInLocal = ConfigurationManager.AppSettings["NJH_LocalFileDirProd"].ToString();
                gHNJHIDCards_FTPLocationProd = ConfigurationManager.AppSettings["NJHID_FTPLocation_prod"].ToString();
                gHNJHIDCards_FTPUserNameProd = ConfigurationManager.AppSettings["NJHID_FTPUserName_prod"].ToString();
                gHNJHIDCards_FTPPwdProd = ConfigurationManager.AppSettings["NJHID_FTPPwd_prod"].ToString();
                //  NJHIDCardsDirectoryProd=ConfigurationManager.AppSettings["NJHIDCardsDirectoryProd"];
                NJHIDCardsDirectory = ConfigurationManager.AppSettings["NJHIDCardsDirectoryProd"];
                NJHSAPDDirectory = ConfigurationManager.AppSettings["NJHSAPDDirectoryProd"];
                NJHIDDsnpCardsDirectory = ConfigurationManager.AppSettings["NJHIDDsnpCardsDirectoryProd"];

            }


            return dbU;


        }



        public static string GetResultsWithHyphen(string phnumber)
        {
            String phone = phnumber;
            string countrycode = "";
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



    }
    public static class GlobalVar
    {
        public static string dbaseName = "";
        public static string connectionKey = "conStrProd";
      //  public static string connectionKey = "conStrTest";
        public static DateTime DateofProcess;
        public static DateTime DateofFilesToProcess;
        public static DateTime EOWProcess;
        public static string adtLCDS = "";






    }




   

    public class appSets
    {
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
                    GlobalVar.DateofProcess = DateTime.Today.AddDays(-0);  //0
                    GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(-3);   // -3
                    break;

                default:
                    if (DateTime.Now.Hour > 14)
                    {
                        GlobalVar.DateofProcess = DateTime.Today.AddDays(0);
                        GlobalVar.DateofFilesToProcess = DateTime.Today.AddDays(0);
                    }
                    else
                    {
                        GlobalVar.DateofProcess = DateTime.Today.AddDays(-0);
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








            XDocument xDoc = XDocument.Load(ProcessVars.gxmlLocation);
            var name = from nm in xDoc.Root.Elements("Server")
                       select nm;
            foreach (XElement xEle in name)
            {
                //ProcessVars.InputDirectory = xEle.Element("IDirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                //ProcessVars.OutputDirectory = xEle.Element("ODirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                ProcessVars.EOWDirectory = xEle.Element("IDirectory").Value + GlobalVar.EOWProcess.ToString("yyyy-MM-dd") + @"\";

                //  ProcessVars.NJHIDCardsDirectory = xEle.Element("NJHIDCardsDirectory").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";

                //  ProcessVars.NJHIDCardsDirectory = xEle.Element("NJHIDCardsDirectory").Value;//test
                ProcessVars.gODMPs = xEle.Element("oDMPsDirectory").Value;
                ProcessVars.gHNJHODMPs = xEle.Element("oHNJHDMPsDirectory").Value;//O/P DUMP FROM BCC
                //ProcessVars.gHNJHProdInLocal = xEle.Element("NJHIDCardsProdInputDirectory").Value + @"\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";//from ftp to network PROD
                //ProcessVars.gHNJHTESTInLocal = xEle.Element("NJHIDCardsTestInputDirectory").Value + @"\" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";//from ftp to network TEST

                //ProcessVars.gHNJHIDCards_FTP_URL=xEle.Element("NJHIDCardsFTPUrl").Value;

                //ProcessVars.gHNJHIDCards_FTPLocationTest = xEle.Element("NJHIDCardsFTPLocationTest").Value;
                //ProcessVars.gHNJHIDCards_FTPUserNameTest = xEle.Element("NJHIDCardsFTPUserNameTest").Value;
                //ProcessVars.gHNJHIDCards_FTPPwdTest = xEle.Element("NJHIDCardsFTPPwdTest").Value;


                //ProcessVars.gHNJHIDCards_FTPLocationProd = xEle.Element("NJHIDCardsFTPLocationProd").Value;
                //ProcessVars.gHNJHIDCards_FTPUserNameProd = xEle.Element("NJHIDCardsFTPUserNameProd").Value;
                //ProcessVars.gHNJHIDCards_FTPPwdProd = xEle.Element("NJHIDCardsFTPPwdProd").Value;
                //ProcessVars.gHNJHIDCardsPdfLoc=xEle.Element("NJHIDCardsPdfLoc").Value;








                ProcessVars.InputDirectory = xEle.Element("IDirectory").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                ProcessVars.appPath = xEle.Element("appPath").Value;
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
                ProcessVars.gDMPs = xEle.Element("DMPsDirectory").Value;//TO BCC 


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


            else if (option == "NJHIDCards")
            {
                XDocument xdoc = XDocument.Load(ProcessVars.gxmlLocation);
                IEnumerable<fFile> iFiles =
                    from s in xdoc.Descendants("ElementNJHIDCards")
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