﻿using System;
using System.Text;
using System.Data;
using System.Reflection;
using System.IO;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{
    public class createEmail
    {
        DBUtility dbU;
        public void produceSummary_ID_Maintenence()
        {
            
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();


            processedData = dbU.ExecuteDataTable("HOR_ZZ_rpt_errors_parse_ID_Maintenance_Card");
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>Files Imported for ID Cards Maintenance</p>");
            if (processedData.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in processedData.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in processedData.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");
                }

                SendMails sendmail = new SendMails();
                sendmail.SendMail("ID Cards Maintenance Upload", "rchico@apps.cierant.com",
                    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);  //tkrompinger@apps.cierant.com

            }


        }
        public void produceSummary_Uploaded_PR()
        {

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();

            //string strsqll = "select L.FileName, z.ZipCount as Files, L.CSVs as [Records in CSV],l.result as 'Status' " +
            //                 "from HOR_parse_Log_Zips Z " +
            //                 "join HOR_parse_files_uploaded L on z.Zipname = substring(l.FileName,1,len(l.FileName) - 4) " +
            //                 "where convert(date,z.logdate) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and z.Type = 'Ticket02 PR zip'";
            SqlParameter[] sqlParams;

            sqlParams = new SqlParameter[] { new SqlParameter("@Date", GlobalVar.DateofProcess.ToString("yyyy-MM-dd")),
                                             new SqlParameter("@type", "Ticket02 PR zip")};

            processedData = dbU.ExecuteDataTable("HOR_rpt_CR_Radius_to_email", sqlParams);

            //string strsqll = "select z.ZipName, z.ZipCount, z.csvNames as [Records in CSV],ISNULL(L.field1,L2.Field1) as 'Status'  from HOR_parse_Log_Zips Z " +
            //                 "left join HOR_parse_Log L " +
            //                 "on z.Zipname = substring(L.msg2,charindex('CASS\',L.msg2)+ 5, 50) " +
            //                 "left join HOR_parse_Log L2 on z.Zipname = substring(L2.msg2,charindex('Files',L2.msg2)+ 5, 50)  " +
            //                 "where convert(date,z.logdate) = convert(date,getdate()) and z.Type = 'Ticket02 PR zip'";

            //processedData = dbU.ExecuteDataTable(strsqll);
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>CR Priority Files Uploaded to FTP </p>");
            if (processedData.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in processedData.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in processedData.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString().Replace("~", "<br>"));
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");
                }

                SendMails sendmail = new SendMails();
                sendmail.SendMail("Uploads to CaptainCrunch " + DateTime.Now.ToString("yyyy-MM-dd"), "jcioban@apps.cierant.com,bdumont@apps.cierant.com, rchico@apps.cierant.com,cgaytan@apps.cierant.com" +
                    ",aaltamirano@sciimage.com,rconte@sciimage.com,pgnecco@sciimage.com,mscherman@sciimage.com,kcarpenter@apps.cierant.com,hcedeno@sciimage.com,ckearney@sciimage.com,stilford@apps.cierant.com,kmcnamara@apps.cierant.com,dgannuscio@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);

                //SendMails sendmail = new SendMails();
                //sendmail.SendMail("Uploads to CaptainCrunch " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com",
                //                            "noreply@apps.cierant.com", "\n\n" +
                //                             strHTMLBuilder); 
            }
            dbU.ExecuteNonQuery("update HOR_parse_Log_Zips set Type = 'Ticket02 PR zip 1st' where convert(date,logdate) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'  and Type = 'Ticket02 PR zip'");





        }

        public void produceSummary_Uploaded_RegCRR()
        {

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();

            //string strsqll = "select z.ZipName, z.ZipCount, z.csvNames as [Records in CSV],ISNULL(L.field1,L2.Field1) as 'Status'  from HOR_parse_Log_Zips Z " +
            //                 "left join HOR_parse_Log L " +
            //                 "on z.Zipname = substring(L.msg2,charindex('CASS\',L.msg2)+ 5, 50) " +
            //                 "left join HOR_parse_Log L2 on z.Zipname = substring(L2.msg2,charindex('Files',L2.msg2)+ 5, 50)  " +
            //                 "where convert(date,z.logdate) = convert(date,getdate()) and z.Type = 'Ticket02 zip'";
           // string strsqll = "select L.FileName, z.ZipCount as Files, L.CSVs as [Records in CSV],l.result as 'Status' " +
                            //"from HOR_parse_Log_Zips Z " +
                            //"join HOR_parse_files_uploaded L on z.Zipname = substring(l.FileName,1,len(l.FileName) - 4) " +
                            //"where convert(date,z.logdate) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "' and z.Type = 'Ticket02 zip'";

            SqlParameter[] sqlParams;

            sqlParams = new SqlParameter[] { new SqlParameter("@Date", GlobalVar.DateofProcess.ToString("yyyy-MM-dd")),
                                             new SqlParameter("@type", "Ticket02 zip")};  

            processedData = dbU.ExecuteDataTable("HOR_rpt_CR_Radius_to_email",sqlParams);
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>CR Regular Files Uploaded to FTP </p>");
            if (processedData.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in processedData.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in processedData.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString().Replace("~", "<br>"));
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");
                }

                SendMails sendmail = new SendMails();
                sendmail.SendMail("Uploads to CaptainCrunch " + DateTime.Now.ToString("yyyy-MM-dd"), "jcioban@apps.cierant.com,bdumont@apps.cierant.com, rchico@apps.cierant.com,cgaytan@apps.cierant.com" +
                    ",aaltamirano@sciimage.com,rconte@sciimage.com,pgnecco@sciimage.com,mscherman@sciimage.com,kcarpenter@apps.cierant.com,hcedeno@sciimage.com,ckearney@sciimage.com,stilford@apps.cierant.com,kmcnamara@apps.cierant.com,dgannuscio@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);

                //SendMails sendmail = new SendMails();
                //sendmail.SendMail("Uploads to CaptainCrunch " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com",
                //                            "noreply@apps.cierant.com", "\n\n" +
                //                             strHTMLBuilder);
            }
            dbU.ExecuteNonQuery("update HOR_parse_Log_Zips set Type = 'Ticket02 zip 1st' where convert(date,logdate) = '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'  and Type = 'Ticket02 zip'");


           


        }

        public void produceSummary_Uploaded()
        {

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();

            string strsqll = "select z.ZipName, z.ZipCount, z.csvNames as [CSVs],L.field1 as Status, z.txtNames as [TXTs] from HOR_parse_Log_Zips Z " +
                             "left join HOR_parse_Log L " +
                             "on z.Zipname = substring(L.msg2,charindex('CASS\',L.msg2)+ 5, 50) " + 
                             "where convert(date,z.logdate) = convert(date,getdate()) and z.Type = 'Ticket01 zip'";
            processedData = dbU.ExecuteDataTable(strsqll);
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>Files Uploaded to FTP</p>");
            if (processedData.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in processedData.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in processedData.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString().Replace("~","<br>"));
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");
                }

                SendMails sendmail = new SendMails();
                //sendmail.SendMail("Uploads to CaptainCrunch & Tickets Ready", "rchico@apps.cierant.com",
                sendmail.SendMail("Uploads to CaptainCrunch " + DateTime.Now.ToString("yyyy-MM-dd"), "jcioban@apps.cierant.com, rchico@apps.cierant.com,stilford@apps.cierant.com,snelson@apps.cierant.com,cgaytan@apps.cierant.com" +
                    //"",                
                    ",aaltamirano@sciimage.com,rconte@sciimage.com,pgnecco@sciimage.com,mscherman@sciimage.com",
                    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);  //tkrompinger@apps.cierant.com

            }


          


        }
        public void produceSummary_ID_NON_Maintenence(string LocalDirectory)
        {

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();

            dbU.ExecuteNonQuery("HOR_ZZ_rpt_Balance_DailyUpload_ID_Cards");
            dbU.ExecuteNonQuery("HOR_ZZ_rpt_Balance_DailyUpload_ID_Cards");
            processedData = dbU.ExecuteDataTable("select * from HOR_parse_ID_Cards_Processed order by Import_Date desc");
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>ID Cards Uploaded to CaptainCrunch </p>");
            if (processedData.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in processedData.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in processedData.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");

                }
                strHTMLBuilder.Append("</tr></table>");

                string strsql = "select InputDate, Code, FileName, Recordnum, Other_Instuctions,  case WHEN ProcessDate is null THEN 'Not Found' " +
                                "else CAST(ProcessDate AS varchar(32))  end as Result from HOR_parse_ID_Cards_Pull " +
                                "where CONVERT(date,InputDate) = CONVERT(date,getdate()) " +
                                "or (convert(date,inputdate) > '2016-02-28' and convert(date,ProcessDate) = CONVERT(date,getdate()))";
                DataTable processedResult = dbU.ExecuteDataTable(strsql);
                if (processedResult.Rows.Count > 0)
                {
                    strHTMLBuilder.Append("<p>Pull Out Records Result</p><p>");
                    strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                    foreach (DataColumn myColumnR in processedResult.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(myColumnR.ColumnName);
                        strHTMLBuilder.Append("</td>");

                    }
                    foreach (DataRow dr in processedResult.Rows)
                    {
                        strHTMLBuilder.Append("<tr >");
                        foreach (DataColumn myColumnR in processedResult.Columns)
                        {
                            strHTMLBuilder.Append("<td >");
                            strHTMLBuilder.Append(dr[myColumnR.ColumnName].ToString());
                            strHTMLBuilder.Append("</td>");

                        }
                        strHTMLBuilder.Append("</tr>");
                    }
                }
                 string result = "";
                 if (Directory.Exists(LocalDirectory))
                 {
                     string[] subdirectoryEntries = Directory.GetDirectories(LocalDirectory);

                     foreach (string subdirectory in subdirectoryEntries)
                     {
                         if (subdirectory.IndexOf("_") == -1)
                         {
                             result = result + "<td >" + subdirectory + "</td>";
                         }
                     }
                 }
                if (result != "")
                {
                    strHTMLBuilder.Append("<p>Files not processed: More than 1 DAT file inside:</p><p>");
                     strHTMLBuilder.Append(result) ;
                }
                //================
                SendMails sendmail = new SendMails();
                sendmail.SendMail("ID Cards Uploaded to CaptainCrunch " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,todonnell@sciimage.com, cgaytan@apps.cierant.com,pgnecco@sciimage.com,edymek@sciimage.com,jnunez@sciimage.com,bdumont@apps.cierant.com,snelson@apps.cierant.com",
                    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);  //tkrompinger@apps.cierant.com

            }
            dbU.ExecuteNonQuery("update HOR_parse_files_to_CASS set UserUpd = '1ST' where CONVERT(DATE,dateprocess)= CONVERT(DATE,GETDATE()) and tablename = 'HOR_parse_Maintenance_ID_Cards'");

        }
        public void produceSummary_Errors_Cycle_01()
        {
            
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            DataTable processedData = new DataTable();


            processedData = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_Errors");
            StringBuilder strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>Errors after Ticket 01  **************</p>");
            if (processedData.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in processedData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in processedData.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in processedData.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                        strHTMLBuilder.Append("</td>");
                        
                    }
                    strHTMLBuilder.Append("</tr>");
                 
                }
                strHTMLBuilder.Append("</table>");
                strHTMLBuilder.Append("<p>");
                DataTable errorsData = dbU.ExecuteDataTable("select Msg1, msg2 from HOR_parse_Log where (msg1 like '%error%' or msg1 like '%no files in zip%') and CONVERT(date,logdate) = CONVERT(date,getdate())");
            //strHTMLBuilder2 = new StringBuilder();
            strHTMLBuilder.Append("<p>Errors Ticket 01  </p>");
            if (errorsData.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in errorsData.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in errorsData.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in errorsData.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");

                }
            }
            strHTMLBuilder.Append("</table>");
            strHTMLBuilder.Append("<p>");

            DataTable errorsData2 = dbU.ExecuteDataTable("select substring(msg2,charindex('CASS\',msg2)+ 5, 50) as File_ , field1 as Error from HOR_parse_Log where convert(date,logdate) = convert(date,getdate()) and charindex('upload ok',field1) = 0 and msg1 = 'end of upload'");
           // strHTMLBuilder = new StringBuilder();
            strHTMLBuilder.Append("<p>Errors Upload Ticket 01  </p>");
            if (errorsData2.Rows.Count > 0)
            {

                strHTMLBuilder.Append("<table border='1px' cellpadding='1' cellspacing='1' bgcolor='lightyellow' style='font-family:Garamond; font-size:smaller'>");
                foreach (DataColumn myColumn in errorsData2.Columns)
                {
                    strHTMLBuilder.Append("<td >");
                    strHTMLBuilder.Append(myColumn.ColumnName);
                    strHTMLBuilder.Append("</td>");

                }
                foreach (DataRow dr in errorsData2.Rows)
                {
                    strHTMLBuilder.Append("<tr >");
                    foreach (DataColumn myColumn in errorsData2.Columns)
                    {
                        strHTMLBuilder.Append("<td >");
                        strHTMLBuilder.Append(dr[myColumn.ColumnName].ToString());
                        strHTMLBuilder.Append("</td>");

                    }
                    strHTMLBuilder.Append("</tr>");

                }
            }



                SendMails sendmail = new SendMails();
                sendmail.SendMail("Errors after Ticket 01", "rchico@apps.cierant.com",
                    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             strHTMLBuilder);  //tkrompinger@apps.cierant.com

            }
            string PhoMSG = "";
            DataTable errorsForMSG = dbU.ExecuteDataTable("HOR_ZZ_rpt_Balance_DailyUpload_Errors_MSG");
            foreach (DataRow dr in errorsForMSG.Rows)
            {
                PhoMSG = PhoMSG + dr[0].ToString() + Environment.NewLine;
               
            }
            SendMails sendmailTxt = new SendMails();
            sendmailTxt.SendMail("Errors after Ticket 01", "2038086157@tmomail.net",
                                        "noreply@apps.cierant.com", "\n\n" +
                                         PhoMSG);  
        }
    }
}
