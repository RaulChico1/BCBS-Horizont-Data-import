using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Data;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Text.RegularExpressions;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{
    public class Nparse_pdf_addrs_to_csv
    {
        List<string> addrs = new List<string>();
        DataTable MBApdfs = pdfs_Table_CR2();
        int Recnum = 0;
        DBUtility dbU;

        public string extract_info_from_pdf()
        {

            string location = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\TEST_pdf_Addr_Only";
            DirectoryInfo originalZIPs = new DirectoryInfo(location);
            string unzipDirName = "";
            foreach (FileInfo f in originalZIPs.GetFiles("*.pdf"))
            {

                MBApdfs.Clear();
                evaluate_MBA_pdf(f.FullName, "");
                string pname = location + "\\" + f.Name.Replace(".pdf", ".csv");
                createCSV createFilecsv = new createCSV();
                createFilecsv.printCSV_fullProcess(pname, MBApdfs, "", "N");
            }

            return "";
        }

        public string evaluate_MBA_pdf(string fileName, string dest)
        {
            FileInfo fileInfo = new System.IO.FileInfo(fileName);

            int index_re = 0;
            string strText = string.Empty;
            try
            {

                PdfReader reader = new PdfReader(fileName);
                int totP = reader.NumberOfPages;
                for (int page = 1; page <= reader.NumberOfPages; page++)
                {
                    ITextExtractionStrategy its = new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy();
                    string s = PdfTextExtractor.GetTextFromPage(reader, page, its);

                    s = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(s)));
                    if (page == 1178)
                        index_re = index_re;
                    string[] words = s.Split('\n');
                    addrs.Clear();
                    for (int i = 0; i < words.Length; i++)
                    {
                        addrs.Add(words[i].ToString().TrimStart().TrimEnd());
                    }
                    while (addrs.Count < 6)
                    {
                        addrs.Add("");
                    }

                    addToTableMBA(1, fileInfo.Name, "MBA_SMN");
                }
                //addToTableMBA(1, fileInfo.Name, "MBA_SMN");
                reader.Close();




            }
            catch (Exception ex)
            {
                var errorf = ex.Message;
            }
            return "";
        }
        public void addToTableMBA(int currline, string fname, string jobClass)
        {
            string test = "";
            Recnum++;


            var row = MBApdfs.NewRow();
            row["Recnum"] = Recnum;
            row["FName"] = fname;
            row["ImportDate"] = DateTime.Now.ToString("yyyy/MM/dd");

            row["coverPageName"] = addrs[0];
            row["coverPageAddress1"] = addrs[1];
            row["coverPageAddress2"] = addrs[2];
            row["coverPageAddress3"] = addrs[3];
            row["coverPageAddress4"] = addrs[4];
            row["coverPageCityStateZip"] = addrs[5];


            MBApdfs.Rows.Add(row);
            addrs.Clear();



        }
        private static DataTable pdfs_Table_CR2()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Recnum");
            newt.Columns.Add("FName");
            newt.Columns.Add("ImportDate");

            newt.Columns.Add("coverPageName");
            newt.Columns.Add("coverPageAddress1");
            newt.Columns.Add("coverPageAddress2");
            newt.Columns.Add("coverPageAddress3");
            newt.Columns.Add("coverPageAddress4");
            newt.Columns.Add("coverPageCityStateZip");

            return newt;
        }

    }
}