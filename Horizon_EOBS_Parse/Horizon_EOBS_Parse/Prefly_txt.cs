using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Horizon_EOBS_Parse
{
    public class Prefly_txt
    {
        public string sysID_Creation(string filename)
        {
            string result = "";
            string line;
            bool found = false;
            bool end = false;
            bool done = false;
            string error = "";
            string sysout, jobname, jobID, pDate;
            sysout = jobID = jobname = pDate = string.Empty;
             System.IO.StreamReader file = 
               new System.IO.StreamReader(filename);
            while ((line = file.ReadLine()) != null)
            {
                try
                {
                    if (done)
                    {
                        break;
                    }
                    else
                    {
                        if (found)
                        {
                            
                            while (line.Contains("  ")) line = line.Replace("  ", " ");
                            string[] words = line.Replace("  ", " ").Trim().Split(' ');    //Previous Balance
                            jobID = words[4];
                            done = true;
                        }
                        else
                        {
                            if (line.IndexOf("* SYSOUT ID:") == 1)
                            {
                                while (line.Contains("  ")) line = line.Replace("  ", " ");
                                string[] words = line.Replace("  ", " ").Trim().Split(' ');    //Previous Balance

                                sysout = words[3];
                                jobname = words[5];
                                pDate = words[8];
                                found = true;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    error = error + ex.Message;
                }
            }
            file.Close();
            result = sysout + " " + jobname + " " + jobID + " " + pDate;
            return result;
        }
    }
}
