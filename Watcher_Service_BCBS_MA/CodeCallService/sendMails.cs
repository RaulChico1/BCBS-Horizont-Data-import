using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Net.Mail;
using System.IO;
using System.Data;

namespace CodeCallService
{
    public class sendMails
    {
        DBUtility dbU;
        public void SendMail(string Subject, string appname, string Body, string fileName)
        {
            bool result = true;
            string ToAddresses, FromAddress;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable emails = dbU.ExecuteDataTable("Select * from HOR_parse_files_Master_emails where appcode = '" + appname + "'");
            if (emails.Rows.Count > 0)
            {
                ToAddresses = emails.Rows[0][1].ToString();
                FromAddress = emails.Rows[0][2].ToString();
            }
            else
            {
                ToAddresses = "rchico@apps.cierant.com";
                FromAddress = "noreply@apps.cierant.com";
            }
            //MailMessage mailObj = new MailMessage(FromAddress, ToAddresses, Subject, Body);
            //SmtpClient SMTPServer = new SmtpClient("ironport.cierant.com");
            //SMTPServer.Send(mailObj);
            WinEventLog wL = new WinEventLog();
            try
            {
                MailMessage mail = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient("email-smtp.us-east-1.amazonaws.com");

                mail.From = new MailAddress(FromAddress);
                mail.To.Add(ToAddresses);
                mail.Subject = Subject;
                mail.Body = Body;
                mail.IsBodyHtml = true;
                SmtpServer.Port = 25;
                SmtpServer.Credentials = new System.Net.NetworkCredential("AKIAIVWKOX7XGRAD2S4Q", "AhjvVJDZ1plWaN6jFrxHHHh/BnSBE+kyFlQrZxp8BQDJ");
                SmtpServer.EnableSsl = true;

                SmtpServer.Send(mail);

            }
            catch (Exception ex)
            {
                //MessageBox.Show(ex.ToString());
                wL.WriteEventLogEntry("Reading Manual Recnums: " + Environment.NewLine +
                    ToAddresses + Environment.NewLine +
                    Body + Environment.NewLine +
                    ex.Message, 2, 1);

                //write file because no email

                string errfileName = fileName.Substring(0, fileName.Length - 4) + "_Email.txt";
                if (File.Exists(errfileName))
                    File.Delete(errfileName);
                FileStream fs1 = new FileStream(errfileName, FileMode.OpenOrCreate, FileAccess.Write);
                StreamWriter writer = new StreamWriter(fs1);
                writer.Write("Record Numbers manually assigned " + Environment.NewLine + Body);
                writer.Close();

                result = false;
            }

        }
        public void SendMailError(string Subject, string appname, string Body, string fileName)
        {
            bool result = true;
            string ToAddresses, FromAddress;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable emails = dbU.ExecuteDataTable("Select * from Master_emails where appcode = '" + appname + "'");
            if (emails.Rows.Count > 0)
            {
                ToAddresses = emails.Rows[0][1].ToString();
                FromAddress = emails.Rows[0][2].ToString();
            }
            else
            {
                ToAddresses = "rchico@apps.cierant.com";
                FromAddress = "noreply@apps.cierant.com";
            }


            //MailMessage mailObj = new MailMessage(FromAddress, ToAddresses, Subject, Body);
            //SmtpClient SMTPServer = new SmtpClient("ironport.cierant.com");
            //SMTPServer.Send(mailObj);
            WinEventLog wL = new WinEventLog();
            try
            {
                MailMessage mail = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient("email-smtp.us-east-1.amazonaws.com");

                mail.From = new MailAddress(FromAddress);
                mail.To.Add(ToAddresses);
                mail.Subject = Subject;
                mail.Body = Body;
                mail.IsBodyHtml = true;
                SmtpServer.Port = 25;
                SmtpServer.Credentials = new System.Net.NetworkCredential("AKIAIVWKOX7XGRAD2S4Q", "AhjvVJDZ1plWaN6jFrxHHHh/BnSBE+kyFlQrZxp8BQDJ");
                SmtpServer.EnableSsl = true;

                SmtpServer.Send(mail);

            }
            catch (Exception ex)
            {
                //MessageBox.Show(ex.ToString());
                wL.WriteEventLogEntry(Subject + Environment.NewLine +
                    ToAddresses + Environment.NewLine +
                    Body + Environment.NewLine +
                    ex.Message, 2, 1);

                //write file because no email

                //string errfileName = fileName.Substring(0, fileName.Length - 4) + "_Email.txt";
                //if (File.Exists(errfileName))
                //    File.Delete(errfileName);
                //FileStream fs1 = new FileStream(errfileName, FileMode.OpenOrCreate, FileAccess.Write);
                //StreamWriter writer = new StreamWriter(fs1);
                //writer.Write("Record Numbers manually assigned " + Environment.NewLine + Body);
                //writer.Close();

                result = false;
            }

        }
    }
}
