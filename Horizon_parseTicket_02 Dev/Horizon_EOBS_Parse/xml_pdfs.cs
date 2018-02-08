using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;
using iTextSharp.text;
using iTextSharp.text.pdf;
using iTextSharp.text.pdf.parser;
using System.Data;
using Microsoft.VisualBasic.FileIO;

namespace Horizon_EOBS_Parse
{

    public class xml_pdfs
    {
        DataTable datafromPdfs = data_Table();
        //DataTable datafromCSV = data_Table();
        public void compare_csv_pdfs_inZip(string csvName, string zipName, int colFileName)
        {
            int totf = 0;

            try
            {
                if (Directory.Exists(ProcessVars.InputDirectory + "from_FTP\\tmp"))
                    Directory.Delete(ProcessVars.InputDirectory + "from_FTP\\tmp", true);

                Directory.CreateDirectory(ProcessVars.InputDirectory + "from_FTP\\tmp");

                using (ZipArchive archive = ZipFile.OpenRead(zipName))
                {
                    int linenum = 0;
                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase))
                        {
                            var row = datafromPdfs.NewRow();
                            entry.ExtractToFile(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                            linenum++;
                            row["Seqnum"] = linenum;
                            row["ZIP"] = zipName;
                            row["Fname"] = entry.Name.ToString();
                            int totPages = 0;
                            PdfReader reader = new PdfReader(Path.Combine(ProcessVars.InputDirectory + "from_FTP\\tmp", entry.FullName));
                            totPages = reader.NumberOfPages;
                            row["Pages"] = totPages.ToString();
                            row["FileInXML"] = "N";
                            datafromPdfs.Rows.Add(row);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                totf = 0;
            }
            readcsv(csvName, colFileName);
            
            string searchExpression = "FileInXML = 'xml only' or FileInXML = 'N'";
            string sort = "Seqnum";
            DataView view = new DataView(datafromPdfs, searchExpression, sort, DataViewRowState.CurrentRows);
            DataTable errorsTable = view.ToTable();

           
        }
        private static DataTable data_Table()
        {
            DataTable newt = new DataTable();
            newt.Clear();
            newt.Columns.Add("Seqnum", typeof(Int32));
            newt.Columns.Add("ZIP");
            newt.Columns.Add("Fname");
            newt.Columns.Add("Pages");
            newt.Columns.Add("FileInXML");

            return newt;
        }
        public void readcsv(string path, int colFileName)
        {
            //DataTable table = new DataTable();
            //table.Columns.Add("FileName", typeof(string));
            using (TextFieldParser csvParser = new TextFieldParser(path))
            {
                csvParser.CommentTokens = new string[] { "#" };
                csvParser.SetDelimiters(new string[] { "," });
                csvParser.HasFieldsEnclosedInQuotes = true;

                // Skip the row with the column names
                csvParser.ReadLine();

                while (!csvParser.EndOfData)
                {
                    // Read current line fields, pointer moves to the next line.
                    string[] fields = csvParser.ReadFields();
                    //string Name = fields[0];
                    //string Address = fields[1];
                    string searchExpression = "FName = '" + fields[colFileName].ToString() + "'";
                    DataRow[] foundRows = datafromPdfs.Select(searchExpression);
                    if (foundRows != null)
                    {
                        foreach (DataRow dr in foundRows)
                        {
                            dr["FileInXML"] = "Y";
                        }
                    }
                    else
                    {
                        var row = datafromPdfs.NewRow();
                        row["Fname"] = fields[colFileName].ToString();
                        row["FileInXML"] = "xml only";
                        datafromPdfs.Rows.Add(row);
                    }
                }
            }
            
        }
    }
}
