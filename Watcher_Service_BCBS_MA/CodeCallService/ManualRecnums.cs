using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.IO;


namespace CodeCallService
{
    public class ManualRecnums
    {
        DBUtility dbU;

        public void evaluate_TXT(string fileName)
        {
             WinEventLog wL = new WinEventLog();
             try
             {
                 if (fileName.IndexOf("p_") != 0)
                 {
                     FileInfo fileInfo = new System.IO.FileInfo(fileName);
                     string errfileName = fileName.Substring(0, fileName.Length - 4) + "_error.txt";
                     if (File.Exists(errfileName))
                     {
                         File.Delete(errfileName);
                     }
                     string addedRecs = "";
                     string errRecords = "";

                     GlobalVar.dbaseName = "BCBS_Horizon";
                     dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
                     System.IO.StreamReader file =
                        new System.IO.StreamReader(fileName);
                     string line;
                     while ((line = file.ReadLine()) != null)
                     {
                         try
                         {
                             string[] words = line.Replace("  ", " ").Trim().Split(',');

                             string description = words[0];
                             string records = words[1];
                             int numValue = 0;
                             if (Int32.TryParse(records, out numValue))
                             {
                                 if (numValue > 0)
                                 {
                                     SqlParameter[] sqlParams2;
                                     sqlParams2 = null;
                                     sqlParams2 = new SqlParameter[] { new SqlParameter("@MESSAGE_CODE", description), new SqlParameter("@NeedRecs", records) };

                                     DataTable newRecords = dbU.ExecuteDataTable("HOR_upd_ManualRecnum", sqlParams2);
                                     if (newRecords.Rows.Count > 0)
                                     {
                                         foreach (DataRow row in newRecords.Rows)
                                         {
                                             addedRecs = addedRecs + "New records from : " + row[0].ToString() + " to  " + row[2].ToString() + "  total records: " + row[1].ToString() + "  for " + description + Environment.NewLine;
                                         }
                                     }
                                 }
                                 else
                                 {
                                     errRecords = errRecords + " Records < 1 " + line + Environment.NewLine;
                                 }
                             }
                             else
                             {
                                 errRecords = errRecords + " Records not numeric " + line + Environment.NewLine;
                             }
                         }
                         catch (Exception ex)
                         {
                             errRecords = errRecords + " error " + ex.Message + Environment.NewLine + line;
                         }
                     }
                     if (errRecords != "")
                     {
                         FileStream fs1 = new FileStream(errfileName, FileMode.OpenOrCreate, FileAccess.Write);
                         StreamWriter writer = new StreamWriter(fs1);
                         writer.Write(errRecords);
                         writer.Close();
                     }
                     if (addedRecs != "")
                     {
                         sendMails sendmail = new sendMails();
                         sendmail.SendMail("Record Numbers manually assigned", "snelson@apps.cierant.com,rchico@apps.cierant.com,tkarintholil@apps.cierant.com",
                                                     "noreply@apps.cierant.com", "\n\n" +
                                                      addedRecs);
                     }
                     string nfilename = fileInfo.Directory + "\\p_" + fileInfo.Name;
                     if (File.Exists(nfilename))
                         File.Delete(nfilename);
                     File.Move(fileInfo.FullName, nfilename);
                 }
             }
             catch (Exception ex)
             {
                 wL.WriteEventLogEntry("Reading Manual Recnums: " + ex.Message, 2, 1);
             }
        }
    }
}
