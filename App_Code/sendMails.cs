using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Net.Mail;

/// <summary>
/// Summary description for sendMails
/// </summary>
public class sendMails
{
    public void SendMail(string Subject, string ToAddresses, string FromAddress, string Body)
    {
        //MailMessage mailObj = new MailMessage(FromAddress, ToAddresses, Subject, Body);
        //SmtpClient SMTPServer = new SmtpClient("ironport.cierant.com");
        //SMTPServer.Send(mailObj);
        try
        {
            MailMessage mail = new MailMessage();
            SmtpClient SmtpServer = new SmtpClient("smtp.gmail.com");

            mail.From = new MailAddress(FromAddress);
            mail.To.Add(ToAddresses);
            mail.Subject = Subject;
            mail.Body = Body;
            mail.IsBodyHtml = true;
            SmtpServer.Port = 587;
            SmtpServer.Credentials = new System.Net.NetworkCredential("rchico@apps.cierant.com", "Rcvh1rcvh");
            SmtpServer.EnableSsl = true;

            SmtpServer.Send(mail);

        }
        catch (Exception ex)
        {
            //MessageBox.Show(ex.ToString());
        }

    }
}