using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;


namespace Horizon_EOBS_Parse
{
    public class LogWriter
    {
        DBUtility dbU;
        public static void WriteErrorLog(string message, params object[] theMsgs)
        {
            try
            {
                FileStream fs = new FileStream(ProcessVars.gErrorLog, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
                StreamWriter m_streamWriter = new StreamWriter(fs);
                m_streamWriter.BaseStream.Seek(0, SeekOrigin.End);
                m_streamWriter.WriteLine(String.Format("{0}", message));

                m_streamWriter.Flush();
                m_streamWriter.Close();

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
            }
        }
        public void WriteLogToTable(string message, string theTime, string Step, string Type, string email = "",string msg3 = "")
        {
             
             GlobalVar.dbaseName = "BCBS_Horizon";
             dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
             int Seqnum = 1;
             var recnum = dbU.ExecuteScalar("select max(Lognum) from HOR_parse_Log");
            
             if (recnum.ToString() == "")
                 Seqnum = 1;
             else
                 Seqnum = Convert.ToInt32(recnum.ToString()) + 1;


            //dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            var recnumInsert = dbU.ExecuteScalar("Insert into HOR_parse_Log (Lognum, LogDate, Msg1, Field1, Msg2, msg3, field3) values (" +
                        Seqnum + ",'" + theTime + "','" + message.Replace("'", " ") + "','" + Step + "','" + Type.Replace("'", " ") + "','" + msg3.Replace("'", " ") + "','" + email + "')");


        }
    }
}
