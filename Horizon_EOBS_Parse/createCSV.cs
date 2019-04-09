using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
namespace Horizon_EOBS_Parse
{
    public class createCSV
    {
        public bool addRecordsCSV(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (value.IndexOf(",") != -1)
                            {
                                if (sb.Length > 0)
                                    sb.Append(",\"" + value + "\"");
                                else
                                    sb.Append("\"" + value + "\"");
                            }
                            else
                            {
                                if (sb.Length > 0)
                                    sb.Append("," + value);
                                else
                                    sb.Append(value);
                                //sb.Append(value.Replace(",", " "));
                            }
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append(",");

                            sb.Append(value.Replace(",", " "));
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }
        public bool addRecordsPipe_CSV(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("|");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("|");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }
        public bool addStringToCSV(string filePath, string rowOutput)
        {
            //var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {

                        wr.WriteLine(rowOutput);
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {

                        wr.WriteLine(rowOutput);
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }
        public bool addRecordsTabDelimited(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\t");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\t");

                            sb.Append(value);
                        }
                        wr.WriteLine(sb.ToString());
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }

        public bool addRecordsQuoteCommaDelimited(string filePath, List<string> rowOutput)
        {
            var sb = new StringBuilder();
            try
            {
                if (File.Exists(filePath))
                {
                    using (StreamWriter wr = File.AppendText(filePath))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\",\"");
                            else
                                sb.Append("\"");
                            sb.Append(value.Replace(",", " "));
                        }
                        wr.WriteLine(sb.ToString() + "\"");
                    }
                }
                else
                {
                    using (var wr = new StreamWriter(filePath, true))
                    {
                        foreach (string value in rowOutput)
                        {
                            if (sb.Length > 0)
                                sb.Append("\",\"");
                            else
                                sb.Append("\"");
                            sb.Append(value.Replace(",", " "));
                        }
                        wr.WriteLine(sb.ToString() + "\"");
                    }
                }

            }
            catch (Exception ex)
            {
                var msg = ex.Message;
                //ErrHandler errhandler = new ErrHandler();
                //errhandler.trackError(ex);
            }

            return true;
        }


        public void CreateCSVFile(DataTable dt, string strFilePath)
        {
            try
            {
                StreamWriter sw = new StreamWriter(strFilePath, false);
                int columnCount = dt.Columns.Count;

                for (int i = 0; i < columnCount; i++)
                {
                    sw.Write(dt.Columns[i]);

                    if (i < columnCount - 1)
                    {
                        sw.Write("|");
                    }
                }

                sw.Write(sw.NewLine);

                foreach (DataRow dr in dt.Rows)
                {
                    for (int i = 0; i < columnCount; i++)
                    {
                        if (!Convert.IsDBNull(dr[i]))
                        {

                          string  item=FormatCSV(dr[i].ToString());

                         

                          sw.Write(item);
                        }

                        if (i < columnCount - 1)
                        {
                            sw.Write("|");
                        }
                    }

                    sw.Write(sw.NewLine);
                }

                sw.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }




    

        private static string QuoteValue(string value)
        {
            return String.Concat("\"",
            value.Replace("\"", "\"\""), "\"");
        }


        public static string FormatCSV(string input)
        {
            try
            {
                if (input == null)
                    return string.Empty;

                bool containsQuote = false;
                bool containsComma = false;
                int len = input.Length;
                for (int i = 0; i < len && (containsComma == false || containsQuote == false); i++)
                {
                    char ch = input[i];
                    if (ch == '"')
                        containsQuote = true;
                    else if (ch == ',')
                        containsComma = true;
                }

                if (containsQuote && containsComma)
                    input = input.Replace("\"", "\"\"");

                if (containsComma)
                    return "\"" + input + "\"";
                else
                    return input;
            }
            catch
            {
                throw;
            }
        }




    }
}
