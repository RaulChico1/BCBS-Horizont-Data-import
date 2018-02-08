using System;
using System.Net.Mail;
using System.IO;

namespace Horizon_EOBS_Parse
{
    public class SendMails
    {
        public string SendMail(string Subject, string ToAddresses, string FromAddress, string Body)
        {
            string msgg = "";
            try
            {

                //MailMessage mail = new MailMessage();
                //SmtpClient SmtpServer = new SmtpClient(ProcessVars.gSmtpClient);

                //mail.From = new MailAddress(FromAddress);
                //mail.To.Add(ToAddresses);
                //mail.Subject = Subject;
                //mail.Body = Body;
                //mail.IsBodyHtml = true;
                //SmtpServer.UseDefaultCredentials = true;
                ////SmtpServer.Port = 25;
                ////SmtpServer.Credentials = new System.Net.NetworkCredential("rchico@apps.cierant.com", "Rcvh1rcvh");
                ////SmtpServer.EnableSsl = true;

                //SmtpServer.Send(mail);



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
                msgg = ex.Message;
                
            }
            return msgg;
        }
        public void SendMailFatalError(string Subject, string appname, string Body, string fileName)
        {
            bool result = true;
            string ToAddresses, FromAddress;
            //GlobalVar.dbaseName = "BCBS_Horizon";
            //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            //DataTable emails = dbU.ExecuteDataTable("Select * from Master_emails where appcode = '" + appname + "'");
            //if (emails.Rows.Count > 0)
            //{
            //    ToAddresses = emails.Rows[0][1].ToString();
            //    FromAddress = emails.Rows[0][2].ToString();
            //}
            //else
            //{
                ToAddresses = "2038086157@tmomail.net";
                FromAddress = "noreply@apps.cierant.com";
            //}
                


            //MailMessage mailObj = new MailMessage(FromAddress, ToAddresses, Subject, Body);
            //SmtpClient SMTPServer = new SmtpClient("ironport.cierant.com");
            //SMTPServer.Send(mailObj);
           WinEventLogcs wL = new WinEventLogcs();
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
