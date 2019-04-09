using System;
using System.Net.Mail;

namespace Horizon_EOBS_Parse
{
    public class SendMails
    {
        public void SendMail(string Subject, string ToAddresses, string FromAddress, string Body)
        {
            try
            {

                MailMessage mail = new MailMessage();
                SmtpClient SmtpServer = new SmtpClient(ProcessVars.gSmtpClient);

                mail.From = new MailAddress(FromAddress);
                mail.To.Add(ToAddresses);
                mail.Subject = Subject;
                mail.Body = Body;
                mail.IsBodyHtml = true;
                SmtpServer.UseDefaultCredentials = true;
                //SmtpServer.Port = 25;
                //SmtpServer.Credentials = new System.Net.NetworkCredential("rchico@apps.cierant.com", "Rcvh1rcvh");
                //SmtpServer.EnableSsl = true;

                SmtpServer.Send(mail);

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //MessageBox.Show(ex.ToString());
            }

        }
        
    }
}
