using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Text.RegularExpressions;
using System.Data;


namespace Horizon_EOBS_Parse
{
    public struct Pages
    {
        public string FileName;
        public string ProvID;
        public int Recnum;
        public string FileDate;
        public int Pag;
        public string metadata;
        public int TotPags;
    };

    public class HNJH_pdf_Counts
    {
        DataTable XmPiepdfs = pdfs_Table_XMPiePDF();
        string m_Recnum, m_ProvID, m_FileDate, m_page, m_metadata, m_JulianDate, m_BatchID, m_importDate, m_IDNumber;
        string errorMSG = "";
        int totP = 0;

        DBUtility dbU;
        public string Read_RosterPDF(string filename)
        {
            string result = "";

            errorMSG = "";

            FileInfo fileInfo = new System.IO.FileInfo(filename);
            int index_re = 0;
            string strText = string.Empty;
            try
            {
                
               
                PdfReader reader = new PdfReader(filename);
                totP = reader.NumberOfPages;
                for (int page = 1; page <= reader.NumberOfPages; page++)
                {
                    ITextExtractionStrategy its = new iTextSharp.text.pdf.parser.LocationTextExtractionStrategy();
                    string s = PdfTextExtractor.GetTextFromPage(reader, page, its);

                    s = Encoding.UTF8.GetString(ASCIIEncoding.Convert(Encoding.Default, Encoding.UTF8, Encoding.Default.GetBytes(s)));
                   
                    string[] words = s.Split('\n');
                    //Text.Append(currentText);
                    int n;
                    if (words[0].ToString().IndexOf("$$METADATA$$") != -1)
                    {
                        Pages  wPages = new Pages();
                        string[] metaData = words[0].ToString().Split('|');
                        wPages.FileName = fileInfo.Name;
                        wPages.ProvID = metaData[1].ToString();
                        wPages.Recnum = Convert.ToInt32(metaData[2].ToString());
                        wPages.FileDate = metaData[3].ToString();
                        wPages.Pag = page;
                        wPages.metadata = words[0].ToString();
                        wPages.TotPags = reader.NumberOfPages;
                        addToTable(wPages);

                    }
                    else
                    {
                        Pages wPages = new Pages();
                        wPages.FileName = fileInfo.Name;
                        wPages.metadata = s;
                        wPages.TotPags = reader.NumberOfPages;
                        addToTable(wPages);
                    }
                }

                //SBCpdfs.Rows[SBCpdfs.Rows.Count - 1]["Page_addrs"] = page_addrs;

                reader.Close();

            }
            catch (Exception ex)
            {
                //errorcount++;
                //errorMSG = ex.Message;
                //addToTableSBC(1, fileInfo.Name, "SBC");

                //MessageBox.Show(ex.Message);
            }
            return "";


            return result;
        }
        public void addToTable(Pages pag)
        {

            var row = XmPiepdfs.NewRow();
            row["FileName"] = pag.FileName;
            row["ProvID"] = pag.ProvID;
            row["Recnum"] = pag.Recnum;
            row["FileDate"] = pag.FileDate;
            row["Pag"] = pag.Pag;
            row["metadata"] = pag.metadata;
            row["TotPags"] = pag.TotPags;

            XmPiepdfs.Rows.Add(row);
            
        }
        private static DataTable pdfs_Table_XMPiePDF()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("FileName");
            newt.Columns.Add("ProvID");
            newt.Columns.Add("Recnum");
            newt.Columns.Add("FileDate");
            newt.Columns.Add("Pag");
            newt.Columns.Add("Metadata");
            newt.Columns.Add("TotPags");
            return newt;
        }
    }
}
