using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;

namespace Horizon_EOBS_Parse
{
    public partial class Parse : ServiceBase
    {
        Timer Encry_timer;
        AutoResetEvent autoEvent = new AutoResetEvent(false);

        public Parse()
        {
            InitializeComponent();
            //#if DEBUG
            //            System.Diagnostics.Debugger.Launch();
            //#endif
            try
            {
                Encry_timer = new Timer(new TimerCallback(ServiceWorker), null, Timeout.Infinite, Timeout.Infinite);
                appSets appsets = new appSets();
                appsets.setVars();
                int totTime = 0;
                //totTime = (((Convert.ToInt16(ProcessVars.gsStart.Substring(0, 2))) * 60) +
                //                    (Convert.ToInt16(ProcessVars.gsStart.Substring(2, 2)))) * 1000;

                totTime = totTimeOneDay(ProcessVars.gsStart);

                Encry_timer.Change(totTime, totTime);

            }

            catch (Exception ex)
            {
                SendMails sendmails = new SendMails();
                sendmails.SendMail("error Service Parse EOBs  Initialize", "rchico@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                        "error.\n\n " + ex.Message);
                int ExitCode = 99;
                Environment.Exit(ExitCode);
            }
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                int totTime = 0;
                totTime = totTimeOneDay(ProcessVars.gsStart);

                TimeSpan duration = TimeSpan.FromMilliseconds(totTime);
                string timerdescUE = "daily at : " + ProcessVars.gsStart;
                totTime = totTimeOneDay(ProcessVars.gsStart);
                TimeSpan durationCleaner = TimeSpan.FromMilliseconds(totTime);

                LogWriter.WriteErrorLog("<tr><th><TABLE BORDER=\"3\"><tr><td td bgcolor=\"#FF0000\">Service start<br>" + ProcessVars.gHostName + "</td></tr></table></th><td>" + timerdescUE + "</td><td></td></tr>");

                SendMails sendmails = new SendMails();
                sendmails.SendMail("Service GEI Enrollment Start", "rchico@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                        " Running " + timerdescUE + " Mode test:" + ProcessVars.gTest);
            }
            catch (Exception ex)
            {
                SendMails sendmails = new SendMails();
                sendmails.SendMail("error Service GEI Enrollment on Start", "rchico@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                        "error.\n\n " + ex.Message);


            }
        }

        protected override void OnStop()
        {
            SendMails sendmails = new SendMails();
            sendmails.SendMail("Service GEI Enrollment STOP", "rchico@apps.cierant.com",
                                    "noreply@apps.cierant.com", "\n\n" +
                                    " Stopped.\n\n " + "Was running daily at : " + ProcessVars.gsStart);
        }
        protected void ServiceWorker(object state)
        {
            int totTime = totTimeOneDay(ProcessVars.gsStart);
            TimeSpan diff = TimeSpan.FromMilliseconds(totTime);
            string timer = " next collection time: " + diff.Hours + " Hours  " + diff.Minutes + "Min";

            Encry_timer.Change(totTime, totTime);
            //Orders_timer.Change(totTime, 0);

            //switch (System.DateTime.Today.DayOfWeek)
            //{
            //case DayOfWeek.Saturday:
            //case DayOfWeek.Sunday:
            //    break;
            //default:
            try
            {
                if (!ProcessVars.serviceIsrunning)
                {
                    LogWriter.WriteErrorLog("<tr><th>" + DateTime.Now.ToString("MM-dd-yy  hh.mm.ss") + "</th><td>" + "Before process, Next collection time: " + timer + " Mode test:" + ProcessVars.gTest + "</td><td></td></tr>");
                    string dateProcess = System.DateTime.Now.ToString("MM-dd-yy");
                    ParseWorker processFiles = new ParseWorker();
                    string result = processFiles.ProcessFiles(dateProcess);
                    autoEvent.WaitOne(300000, false);
                    totTime = totTimeOneDay(ProcessVars.gsStart);
                    diff = TimeSpan.FromMilliseconds(totTime);
                    timer = " next collection time: " + diff.Hours + " Hours  " + diff.Minutes + "Min";
                    Encry_timer.Change(totTime, totTime);
                    LogWriter.WriteErrorLog("<tr><th>" + DateTime.Now.ToString("MM-dd-yy  hh.mm.ss") + "</th><td>" + "Next collection time: " + timer + "</td><td></td></tr>");

                }
            }
            catch (Exception ow)
            {
                SendMails sendmails = new SendMails();
                sendmails.SendMail("error Service GEI_Ency ServiceWorker", "rchico@apps.cierant.com",
                                        "noreply@apps.cierant.com", "\n\n" +
                                        "error.\n\n " + ow.Message);

                int ExitCode = 99;
                Environment.Exit(ExitCode);
            }
            //  break;
            //}


        }
        protected int totTimeOneDay(string wTimer)
        {
            DateTime nextRun1 = System.DateTime.Today.AddMinutes(2);
            DateTime nextRun2 = nextRun1.AddHours((Convert.ToInt16(ProcessVars.gsStart.Substring(0, 2))));
            nextRun2 = nextRun2.AddMinutes((Convert.ToInt16(ProcessVars.gsStart.Substring(2, 2))));
            TimeSpan diff = nextRun2.Subtract(DateTime.Now);
            int totTime = Convert.ToInt32(diff.TotalMilliseconds);
            if (totTime < 0)
            {
                //next day
                nextRun1 = System.DateTime.Today.AddMinutes(1);
                nextRun2 = nextRun1.AddDays(1);
                nextRun2 = nextRun2.AddHours((Convert.ToInt16(ProcessVars.gsStart.Substring(0, 2))));
                nextRun2 = nextRun2.AddMinutes((Convert.ToInt16(ProcessVars.gsStart.Substring(2, 2))));
                diff = nextRun2.Subtract(DateTime.Now);
                totTime = Convert.ToInt32(diff.TotalMilliseconds);
                if (totTime < 0) { totTime = -(totTime); }
            }

            return totTime;
        }
    }
}
