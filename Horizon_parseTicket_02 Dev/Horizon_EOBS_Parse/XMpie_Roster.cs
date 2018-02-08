using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Horizon_EOBS_Parse
{
    public class XMpie_Roster
    {
        DBUtility dbU;
        private string JobIDs = "";
        public string JobID = "";

        public string P_Roster(string _DocumentID)
        {


            try
            {
                // Create the job ticket web service object    

                xmpiedirector_JobTicket.JobTicket_SSP jobTicketWS = new xmpiedirector_JobTicket.JobTicket_SSP();

                // Create a new job ticket
                string jobTicketID = jobTicketWS.CreateNewTicketForDocument(ProcessVars.uName, ProcessVars.Password, _DocumentID, "", false);

                // jobTicketWS.AddDestinationByID(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"], "", true);
                jobTicketWS.AddDestinationByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_PROSTER, "", true);


                //
                // Set a recipient ID
                xmpiedirector_JobTicket.RecipientsInfo recipientInfo = new xmpiedirector_JobTicket.RecipientsInfo();
                recipientInfo.m_FilterType = 1;     // 1 = Query
                int documentid = Convert.ToInt32(_DocumentID);
                if (documentid == 1882)
                {
                    recipientInfo.m_Filter = ProcessVars.RecipientsDataSourceQueryMLTSS;
                }
                else
                    recipientInfo.m_Filter = ProcessVars.RecipientsDataSourceQueryNJFamily;

                //jobTicketWS.SetOutputFolder(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"]); //"Horizon NJH Production"
                jobTicketWS.SetOutputFolder(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_IDCARDS); //"Horizon NJH Production"

                //// Set the job output type  ..PDF name from FileName ADOR
                xmpiedirector_JobTicket.Parameter[] my_params = new xmpiedirector_JobTicket.Parameter[2];
                my_params[0] = new xmpiedirector_JobTicket.Parameter();
                my_params[0].m_Name = "PDF_MULTI_SINGLE_RECORD";
                my_params[0].m_Value = "false";// single PDF for all records. Else set true. 

                my_params[1] = new xmpiedirector_JobTicket.Parameter();
                my_params[1].m_Name = "FILE_NAME_ADOR";
                my_params[1].m_Value = "FileName";

                jobTicketWS.SetOutputParameters(ProcessVars.uName, ProcessVars.Password, jobTicketID, my_params);
                jobTicketWS.SetOutputInfo(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PDFO", 1, ProcessVars.OutputFolderName, null, null);
                jobTicketWS.SetJobType(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PRINT");

                xmpiedirector_Production.Production_SSP productionWS = new xmpiedirector_Production.Production_SSP();
                //if (documentid == 1882)
                //{
                //    jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.DataSourceID_1882);
                //}
                //else
                    jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.DataSourceID_1928);

                // Submit the job
                // jobID = productionWS.SubmitJob(uName, Password, jobTicketID, "0", "", null);
                string[] jobid = productionWS.SubmitSplittedJob(ProcessVars.uName, ProcessVars.Password, jobTicketID, "0", ProcessVars.SplittedJobBatchSize, "Highest", null, null); //splitted         
                for (int i = 0; i < jobid.Length; i++)
                {
                    JobIDs += jobid[i] + ";";

                }

            }
            catch (Exception ex)
            {
                var exception = ex.Message;
            }

            return JobIDs;
        }

        public void welcomeKits(string _DocumentID)
        {
            try
            {
                // Create the job ticket web service object    

                xmpiedirector_JobTicket.JobTicket_SSP jobTicketWS = new xmpiedirector_JobTicket.JobTicket_SSP();

                // Create a new job ticket
                string jobTicketID = jobTicketWS.CreateNewTicketForDocument(ProcessVars.uName, ProcessVars.Password, _DocumentID, "", false);

                // jobTicketWS.AddDestinationByID(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"], "", true);
                jobTicketWS.AddDestinationByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.wk_DestinationID_IDCards, "", true);


                //
                // Set a recipient ID
                xmpiedirector_JobTicket.RecipientsInfo recipientInfo = new xmpiedirector_JobTicket.RecipientsInfo();
                recipientInfo.m_FilterType = 1;     // 1 = Query
               
                    recipientInfo.m_Filter = ProcessVars.RecipientsDataSourceQueryMLTSS;
            

                //jobTicketWS.SetOutputFolder(uName, Password, jobTicketID, ConfigurationManager.AppSettings["DestinationID"]); //"Horizon NJH Production"
                jobTicketWS.SetOutputFolder(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.wk_DestinationID_IDCards); //"Horizon NJH Production"

                //// Set the job output type  ..PDF name from FileName ADOR
                xmpiedirector_JobTicket.Parameter[] my_params = new xmpiedirector_JobTicket.Parameter[2];
                my_params[0] = new xmpiedirector_JobTicket.Parameter();
                my_params[0].m_Name = "PDF_MULTI_SINGLE_RECORD";
                my_params[0].m_Value = "false";// single PDF for all records. Else set true. 

                my_params[1] = new xmpiedirector_JobTicket.Parameter();
                my_params[1].m_Name = "FILE_NAME_ADOR";
                my_params[1].m_Value = "FileName";

                jobTicketWS.SetOutputParameters(ProcessVars.uName, ProcessVars.Password, jobTicketID, my_params);
                jobTicketWS.SetOutputInfo(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PDFO", 1, ProcessVars.OutputFolderName, null, null);
                jobTicketWS.SetJobType(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PRINT");

                xmpiedirector_Production.Production_SSP productionWS = new xmpiedirector_Production.Production_SSP();

                jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.wk_RecipientTablet_4200);
               
                // Submit the job
                // jobID = productionWS.SubmitJob(uName, Password, jobTicketID, "0", "", null);
                string[] jobid = productionWS.SubmitSplittedJob(ProcessVars.uName, ProcessVars.Password, jobTicketID, "0", ProcessVars.SplittedJobBatchSize, "Highest", null, null); //splitted         
                for (int i = 0; i < jobid.Length; i++)
                {
                    JobIDs += jobid[i] + ";";

                }

            }
            catch (Exception ex)
            {
                var msgError = ex.Message;
            }

        }
        public void CheckJobStatus(string JobID)
        {

            string[] JobIDb = JobIDs.Split(';');
            xmpiedirector_Job.Job_SSP Job = new xmpiedirector_Job.Job_SSP();
            foreach (string JID in JobIDb)
            {
                if (JID != "")
                {
                    int status = Job.GetStatus(ProcessVars.uName, ProcessVars.Password, JID);

                    if (status == 3)
                    {
                        // Job Completed
                        JobIDs = JobIDs.Replace(JID, "");
                    }
                    else if (status == 4) //"failed"
                    {

                        //FAILED
                        JobIDs = JobIDs.Replace(JID, "");
                        SendMails sendmail = new SendMails();
                        sendmail.SendMail("NJHId Cards Xmpie Job FAILED for " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,tkarintholil@apps.cierant.com,", "noreply@apps.cierant.com", JID);

                    }
                    else if (status == 2) //In progress
                    {
                        //Wait for some time then do next step
                        var t = Task.Run(async delegate
                        {
                            await Task.Delay(1000 * 60 * 1);
                            return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
                        });
                        t.Wait();
                        SendMails sendmail = new SendMails();
                        sendmail.SendMail("NJHId Cards Xmpie Job PAUSED " + DateTime.Now.ToString("yyyy-MM-dd"), "tkarintholil@apps.cierant.com,", "noreply@apps.cierant.com", JID);
                        CheckJobStatus(JobIDs);

                    }

                }

            }
        }


        //public string Panel_Roster(string _DocumentID)
        //{
        //    string result = "";
        //    string jobID = "";
        //    try
        //    {
        //        //Create the job ticket web service object            
        //        xmpiedirector_JobTicket.JobTicket_SSP jobTicketWS = new xmpiedirector_JobTicket.JobTicket_SSP();

        //        // Create a new job ticket
        //        string jobTicketID = jobTicketWS.CreateNewTicketForDocument(ProcessVars.uName, ProcessVars.Password, _DocumentID, "", false);

        //        jobTicketWS.AddDestinationByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_PROSTER, "", true);

        //        // Set a recipient ID
        //        xmpiedirector_JobTicket.RecipientsInfo recipientInfo = new xmpiedirector_JobTicket.RecipientsInfo();
        //        recipientInfo.m_FilterType = 3;     // 3 = TableName
        //        recipientInfo.m_Filter = ProcessVars.RecipientTable_1928; // the table name of the recipient data source  "HNJH_RosterProvider_Xmpie"; 

        //        jobTicketWS.SetOutputFolder(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.DestinationID_PROSTER); //"Horizon NJH Production"

        //        //// Set the job output type  ..PDF name from FileName ADOR
        //        xmpiedirector_JobTicket.Parameter[] my_params = new xmpiedirector_JobTicket.Parameter[2];
        //        my_params[0] = new xmpiedirector_JobTicket.Parameter();
        //        my_params[0].m_Name = "PDF_MULTI_SINGLE_RECORD";
        //        my_params[0].m_Value = "true";// 'false' if single PDF for all records. Else set 'true'.  

        //        my_params[1] = new xmpiedirector_JobTicket.Parameter();
        //        my_params[1].m_Name = "FILE_NAME_ADOR";
        //        my_params[1].m_Value = "OutPDF_Name";


        //        jobTicketWS.SetOutputParameters(ProcessVars.uName, ProcessVars.Password, jobTicketID, my_params);
        //        jobTicketWS.SetOutputInfo(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PDFO", 1, ProcessVars.OutputFolderName, null, null);
        //        jobTicketWS.SetJobType(ProcessVars.uName, ProcessVars.Password, jobTicketID, "PRINT");

        //        xmpiedirector_Production.Production_SSP productionWS = new xmpiedirector_Production.Production_SSP();

        //        //xmpiedirector_JobTicket.Connection Conn = new xmpiedirector_JobTicket.Connection();
        //        //Conn.m_ConnectionString = ConfigurationManager.AppSettings["SqlConn"]; 
        //        //Conn.m_Type = "MSQL" ;
        //        //jobTicketWS.SetRI(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, Conn);
        //        //Conn.m_AdditionalInfo = "@Provider_panel";

        //        jobTicketWS.SetRIByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, recipientInfo, ProcessVars.DataSourceID_1928);

        //        jobTicketWS.SetDataSourceByID(ProcessVars.uName, ProcessVars.Password, jobTicketID, ProcessVars.AdditionalTable_1928, ProcessVars.DataSourceID_1928);

        //        jobTicketWS.AddCompression(ProcessVars.uName, ProcessVars.Password, jobTicketID, "HOR_Medicaid_Panel_Roster_" + DateTime.Now.ToString("yyyyMMdd") + ".zip", false); //TO DO: CHANGE .zip FILE NAME

        //        //jobTicketWS.AddCompression(ProcessVars.uName, ProcessVars.Password, jobTicketID, "", false);

        //        // Submit the job
        //        jobID = productionWS.SubmitJob(ProcessVars.uName, ProcessVars.Password, jobTicketID, "0", "", null);
        //        JobIDs += jobID + ";";

        //    }
        //    catch (Exception ex)
        //    {
        //        //Handle error
        //        result = ex.Message;
        //    }
        //    return result;

        //}
        //private void CheckJobStatus(string JobID)
        //{
        //    string JID = JobID;

        //    xmpiedirector_Job.Job_SSP Job = new xmpiedirector_Job.Job_SSP();

        //    if (JID != "")
        //    {
        //        int status = Job.GetStatus(ProcessVars.uName, ProcessVars.Password, JID);

        //        if (status == 3)
        //        {
        //            // Job Completed
        //        }
        //        else if (status == 4) //"failed"
        //        {

        //            //FAILED
        //        }
        //        else if (status == 2) //In progress
        //        {
        //            //Wait for some time then do next step

        //        }

        //    }
        //}
    }
}
