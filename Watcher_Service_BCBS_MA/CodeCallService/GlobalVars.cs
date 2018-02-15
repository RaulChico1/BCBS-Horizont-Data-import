using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml;
using System.Xml.Linq;

namespace CodeCallService
{
    public static class GlobalVar
    {
        public static string dbaseName = "";
        public static string connectionKey = "conStrDev";
        public static DateTime DateofProcess;
        public static DateTime DateofFilesToProcess;
        public static DateTime EOWProcess;
        public static string adtLCDS = "";
    }
    public static class ProcessVars
    {
        public static string gProgramPath = "";
        public static string gxmlLocation = "";
        //public static string EOWDirectory = "";
        public static string[] arArrayWatch = new string[5];
        public static string gmappingFile = "";
        public static string dmpsWatched = "";
        public static string oDMPsDirectory = "";
        public static string oDMPsDirectoryM = "";
        public static string oDMPSimb = "";
        public static string dataEOC = "";
        public static string errEmail = "";
        public static string errMsg = "";
    }
    public class appSets
    {
        public string checkDrives()
        {
            
            string errorsIndrives = "";
            try
            {
                if (!Directory.Exists(ProcessVars.dmpsWatched))
                    errorsIndrives = "BCC not running, Watched Directory does not exist: " + ProcessVars.dmpsWatched + Environment.NewLine;
                if (!Directory.Exists(ProcessVars.oDMPsDirectoryM))
                    errorsIndrives = errorsIndrives + "BCC not running, Results Directory does not exist: " + ProcessVars.oDMPsDirectoryM + Environment.NewLine;
                if (!Directory.Exists(ProcessVars.dataEOC))
                {
                    Directory.CreateDirectory(ProcessVars.dataEOC);
                }
            }
            catch (Exception ex)
            {
                errorsIndrives = errorsIndrives + ex.Message;
            }
            finally
            {
                if (errorsIndrives != "")
                {
                    sendMails sendmail = new sendMails();
                    sendmail.SendMailError("BCBS_MA_Checking Directories", "ErrorinProcess", "\n\n" + "Error " + errorsIndrives, "");
                }


            }
            return errorsIndrives;
        }
        public void setVars()
        {
            WinEventLog wL = new WinEventLog();
            try
            {
                getDateProcess();
                string[] daysWeek = GetWeekRange(DateTime.Now);

                ProcessVars.gProgramPath = Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
                ProcessVars.gxmlLocation = ProcessVars.gProgramPath + "\\VarDirectories.xml";
                ProcessVars.gmappingFile = ProcessVars.gProgramPath + "\\OEC.xml";
                //ProcessVars.gmappingFileMRDF = ProcessVars.gProgramPath + "\\MRDF.xml";

                XDocument xDoc = XDocument.Load(ProcessVars.gxmlLocation);
                var name = from nm in xDoc.Root.Elements("Server")
                           select nm;
                foreach (XElement xEle in name)
                {
                    //ProcessVars.InputDirectory = xEle.Element("IDirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                    //ProcessVars.OutputDirectory = xEle.Element("ODirectory").Value + DateTime.Now.ToString("yyyy-MM-dd") + @"\";
                    ProcessVars.arArrayWatch[0] = xEle.Element("IDir01").Value;
                    ProcessVars.arArrayWatch[1] = xEle.Element("IDir02").Value;
                    ProcessVars.arArrayWatch[2] = xEle.Element("IDir03").Value;
                    ProcessVars.arArrayWatch[3] = xEle.Element("IDir04").Value;
                    ProcessVars.arArrayWatch[4] = xEle.Element("IDir05").Value;
                    ProcessVars.dmpsWatched = xEle.Element("dmpsWatched").Value;
                    ProcessVars.oDMPsDirectoryM = xEle.Element("oDMPsDirectoryMAS").Value;
                    ProcessVars.oDMPSimb = xEle.Element("oDMPSimb").Value;
                    //ProcessVars.dataEOC = xEle.Element("dataEOC").Value + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + @"\";
                    ProcessVars.dataEOC = xEle.Element("dataEOC").Value +  "Week_" + daysWeek[1].ToString() + @"\";
                    ProcessVars.errEmail = xEle.Element("errEmail").Value;
                    ProcessVars.errMsg = xEle.Element("errMsg").Value;

                }
                System.IO.Directory.CreateDirectory(ProcessVars.dataEOC );
                System.IO.Directory.CreateDirectory(ProcessVars.dataEOC + @"\from_Xmpie");
                System.IO.Directory.CreateDirectory(ProcessVars.dataEOC + @"\ToSCI");
            }
            catch (Exception ex)
            {
                wL.WriteEventLogEntry("Setting vars: " + ex.Message, 2, 1);
            }
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
                    if (DateTime.Now.Hour > 14)
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
        private string[] GetWeekRange(DateTime dateToCheck)
        {
            string[] result = new string[2];
            TimeSpan duration = new TimeSpan(0, 0, 0, 0); //One day 
            DateTime dateRangeBegin = dateToCheck;
            DateTime dateRangeEnd = DateTime.Today.Add(duration);

            dateRangeBegin = dateToCheck.AddDays(-(int)dateToCheck.DayOfWeek);
            dateRangeEnd = dateToCheck.AddDays(7 - (int)dateToCheck.DayOfWeek);

            result[1] = dateRangeBegin.Date.ToString("yyyy-MM-dd");
            result[0] = dateRangeEnd.Date.ToString("yyyy-MM-dd");
            return result;

        }
    }
}