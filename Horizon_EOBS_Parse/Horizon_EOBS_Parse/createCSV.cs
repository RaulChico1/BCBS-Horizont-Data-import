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
        DBUtility dbU;
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
                            string nvalue = value.Replace("-", " ");
                            if (nvalue.IndexOf(",") != -1)
                            {
                                if (sb.Length > 0)
                                    sb.Append(",\"" + nvalue + "\"");
                                else
                                    sb.Append("\"" + nvalue + "\"");
                            }
                            else
                            {
                                if (sb.Length > 0)
                                    sb.Append("," + nvalue);
                                else
                                    sb.Append(nvalue);
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
                            string nvalue = value.Replace("-", " ");
                            if (sb.Length > 0)
                                sb.Append(",");

                            sb.Append(nvalue.Replace(",", " "));
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

        public bool printCSV_fullProcess(string filename, DataTable inputData, string suffix, string XMPieHeader)
        {
            bool process = false;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            FileInfo fileInfo = new System.IO.FileInfo(filename);

            if (inputData.Rows.Count > 0)
            {
                string fNewName = fileInfo.Name;
                if (suffix.Length > 0)
                {
                    fNewName = fileInfo.Name.Substring(0, fileInfo.Name.Length - (fileInfo.Extension.Length)) + suffix + fileInfo.Extension;
                }
                string pName = fileInfo.Directory + "\\" + fNewName;
                if (File.Exists(pName))
                    File.Delete(pName);
                var fieldnames = new List<string>();
                for (int index = 0; index < inputData.Columns.Count; index++)
                {
                    fieldnames.Add(inputData.Columns[index].ColumnName);
                }
                bool resp = addRecordsCSV(pName, fieldnames);
                if (XMPieHeader == "Y")
                    resp = addRecordsCSV(pName, fieldnames);
                foreach (DataRow row in inputData.Rows)
                {

                    var rowData = new List<string>();
                    for (int index = 0; index < inputData.Columns.Count; index++)
                    {
                        if (row[index].ToString() == "")
                            rowData.Add(" ");
                        else
                            rowData.Add(row[index].ToString());
                    }
                    resp = false;
                    resp = addRecordsCSV(pName, rowData);
                }

            }



            return process;
        }
    }
}
