using System;
using System.Net.Mail;
using System.IO;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public class SendMails
    {
        DBUtility dbU;
        public void SendMail(string Subject, string ToAddresses, string FromAddress, string Body)
        {
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
                var msg = ex.Message;
                //MessageBox.Show(ex.ToString());
            }

        }
        public void SendMailError(string Subject, string appname, string Body, string fileName)
        {
            bool result = true;
            string ToAddresses, FromAddress;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
           
                ToAddresses = "rchico@apps.cierant.com";
                FromAddress = "noreply@apps.cierant.com";
            


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
                
                result = false;
            }

        }
    }
}
