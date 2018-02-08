using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Configuration;
using System.Text.RegularExpressions;

namespace Horizon_EOBS_Parse
{
    public class NParse_HOR_FEP_UPS
    {
        DBUtility dbU;
        public string processData(string filename)
        {
            int updErrors = 0;
            string errors = "";
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            int GRecnum = 1;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
            int recordnumber = 0;
            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;



            string result = "";
            FileInfo fileInfo = new System.IO.FileInfo(filename);





            updateASCIIdata(filename, fileInfo.Directory.ToString());

            return "";
        }
        public void updateASCIIdata(string filename, string directory)
        {
            StringBuilder newFile = new StringBuilder();
            string keyWord = "";
            string pat = @"\b[A-Za-z]{2}(?=([0-9]*[1-9]){1,})\d{13}\b";      //@"(\w+)\s+(car)";
            string[] file = File.ReadAllLines(filename);

            foreach (string line in file)
            {
                if (keyWord == "")
                {
                    Regex r = new Regex(pat, RegexOptions.IgnoreCase);
                    Match m = r.Match(line);
                    if (m.Value != "")
                    {
                        keyWord = m.Value.Substring(0,4);
                        string nLine = line.Substring(0, line.IndexOf(keyWord) - 1);
                        newFile.Append(nLine + "\r\n");
                    }
                    else
                        newFile.Append(line + "\r\n");
                }
                else
                {
                    if ((line.IndexOf(keyWord) - 1) > 0)
                    {
                        string nLine = line.Substring(0, line.IndexOf(keyWord) - 1);
                        newFile.Append(nLine + "\r\n");

                    }
                    else
                        newFile.Append(line + "\r\n");
                }

            }



            File.WriteAllText(directory + "\\Results.txt", newFile.ToString());


        }
        static IEnumerable<string> ReadAsLines(string filename)
        {
            using (var reader = new StreamReader(filename))
                while (!reader.EndOfStream)
                    yield return reader.ReadLine();
        }
    }
}
