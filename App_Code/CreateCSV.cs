using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Web;

/// <summary>
/// Summary description for CreateCSV
/// </summary>
public class CreateCSV
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

                        sb.Append(value.Replace(",", " "));
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

                        sb.Append(value.Replace(",", " "));
                    }
                    wr.WriteLine(sb.ToString());
                }
            }

        }
        catch (Exception ex)
        {
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
            //ErrHandler errhandler = new ErrHandler();
            //errhandler.trackError(ex);
        }

        return true;
    }
}