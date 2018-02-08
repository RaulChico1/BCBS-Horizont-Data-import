using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;

namespace Horizon_EOBS_Parse
{
    public class Ticket
    {
        public void createTicket(DataTable dataToTicket, string location, string ticketNO)
        {
            int totalRecs = dataToTicket.Rows.Count;
            try
            {
                excelWritter.ExcelDocument document = new excelWritter.ExcelDocument();
                document.UserName = "Service";
                document.CodePage = CultureInfo.CurrentCulture.TextInfo.ANSICodePage;

                document.ColumnWidth(0, 140);
                document.ColumnWidth(1, 80);
                document.ColumnWidth(2, 80);
                document.ColumnWidth(3, 340);
                document.ColumnWidth(4, 100);
                document.ColumnWidth(5, 100);

                document[0, 0].Value = "Customer";
                document[0, 1].Value = "Horizon Blue Cross Blue Shield of NJ";
                document[1, 0].Value = "Process Date:";
                document[1, 1].Value = GlobalVar.DateofProcess.ToString("MM/dd/yyyy");
                document[2, 0].Value = "Process Time:";
                document[2, 1].Value = DateTime.Now.ToString("HH:mm:ss");

                document[4, 0].Value = "Total Files Processed:";
                document[4, 1].Value = totalRecs;
                document[5, 0].Value = "Files Destination:";

                document[0, 1].Font = new System.Drawing.Font("Tahoma", 10, System.Drawing.FontStyle.Bold);

                int xRow = 7;

                for (int index = 0; index < dataToTicket.Columns.Count; index++)
                {
                    //fieldnames.Add(dataToTicket.Columns[index].ColumnName);
                    document.Cell(xRow, index).Value = dataToTicket.Columns[index].ColumnName;
                    document[xRow, index].Font = new System.Drawing.Font("Tahoma", 10, System.Drawing.FontStyle.Bold);
                }
                xRow = 8;
                foreach (DataRow row in dataToTicket.Rows)
                {
                    for (int index = 0; index < dataToTicket.Columns.Count; index++)
                    {
                        
                        if (index == 0)
                        {
                            document.Cell(xRow, index).Value = row[index].ToString().Substring(0, 10);
                            
                        }
                        else if (index == 4 || index == 5)
                            document.Cell(xRow, index).Value = Convert.ToUInt32(row[index].ToString());
                        else
                            document.Cell(xRow, index).Value = row[index].ToString();
                    }
                    xRow++;
                }


                string FileNAME = location + "Job Ticket_" + ticketNO + "_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd") + ".xlsx";
                string FileNAME_network = @"\\Cierant-taper\clients\Horizon BCBS\NoticeLetters\JobTickets\" + "Job Ticket_" + ticketNO + "_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd") + ".xls";
                if (File.Exists(FileNAME))
                    File.Delete(FileNAME);
                if (File.Exists(FileNAME_network))
                    File.Delete(FileNAME_network);

                FileStream stream = new FileStream(FileNAME, FileMode.Create);
                document.Save(stream);
                stream.Close();

                File.Copy(FileNAME, FileNAME_network, true);


                SendMails sendmail = new SendMails();
                sendmail.SendMail("Horizon BCBS Daily Ticket " + ticketNO + " Ready  EOM", "rchico@apps.cierant.com,kcarpenter@apps.cierant.com,kmcnamara@apps.cierant.com,cgaytan@apps.cierant.com",
                    //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                                            "noreply@apps.cierant.com", "\n\n" +
                                             "Ticket ready");  //tkrompinger@apps.cierant.com

            }
            catch (Exception ex)
            {
                var errormsg = ex.Message;
            }
        }
    }
}
