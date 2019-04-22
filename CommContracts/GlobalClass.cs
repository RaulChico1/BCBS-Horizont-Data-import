using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;


namespace CommContracts
{
    public static class cVars
    {
        public static string CycleDirectory = "";
        public static string connectionKey = "conStrProd";
    }
    public static class GlobalVar
    {

        public static string UserUpd = "";
        public static string UserLoc = "";
        public static bool isLocal = false;
        public static string directoryOLB = "";
        public static string directoryLibrary = "";
        public static string directoryWdata = "";

        public static string dbaseName = "";
        public static string gHostName = "";
        public static string gErrorHTML = "";
        public static string gEmaillogo = "";
        public static string directoryLocation = "";
        public static string progLocation = "";
        public static string inputFile = "";
        public static string outputFile = "";
        public static string outputFileSelected = "";
        public static string fName = "";
        public static string Original_File_Name = "";
        public static string gxmlLocation = "";
        public static string colName = "";
        public static string fileDesignation = "";
        public static string fileDate = "";
        public static string Cycle = "";
        public static string MailDate = "";


        public static string directoryOriginal = "";
        public static string directoryConverted = "";
        public static string directoryFinal = "";


        public static string OriginalTicket = "";
        public static string OriginalCounts = "";
    }
    public class appSets
    {
        public void setVars()
        {
            if (GlobalVar.isLocal)
            {
                GlobalVar.directoryOLB = @"\\freenas\BCBSMA\PDF_Files\e__company_tradingpartners_Marketing_Comm_OLB\";
                GlobalVar.directoryLibrary = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Library\";
            }
            else
            {
                GlobalVar.directoryOLB = @"\\freenas\BCBSMA\PDF_Files\e__company_tradingpartners_Marketing_Comm_OLB\";
                GlobalVar.directoryLibrary = @"E:\BCBS_MA\Library\";
            }
        }
    }
}