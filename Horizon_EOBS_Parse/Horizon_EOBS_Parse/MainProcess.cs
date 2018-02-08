using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.IO;
using Horizon_EOBS_Parse;
using System.IO.Compression;
using System.Threading.Tasks;

namespace Horizon_EOBS_Parse
{
    public class MainProcess
    {
        public void MainProcessParse(string cycleP)
        {
            int totfilesPrecessed = 0;
            DBUtility dbU;
            string Parsed = "";
            int filesinDir = 0;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            string strsql = "delete from HOR_parse_files_to_CASS where (msg like 'error count%' or msg = 'No recods in file')  and CONVERT(DATE,ImportDate)= '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'";
            dbU.ExecuteScalar(strsql);
            strsql = "delete from HOR_parse_files_to_CASS where (msg like 'No SYSOUT ID%')  and CONVERT(DATE,ImportDate)= '" + GlobalVar.DateofProcess.ToString("yyyy-MM-dd") + "'";
            dbU.ExecuteScalar(strsql);
            string[] arrayEOBS = new string[] { "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X" };
            string[] arraycHS = new string[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "Y", "Z" };

            DirectoryInfo originaTXTs = new DirectoryInfo(ProcessVars.InputDirectory + @"\Decrypted");
            //DirectoryInfo originaTXTs = new DirectoryInfo(ProcessVars.InputDirectory + @"\5403");
            FileInfo[] files = originaTXTs.GetFiles("*.txt");
            //FileInfo[] files = originaTXTs.GetFiles("EP0GHBLP-20150814190049.txt");
            string errors = "";
            string newsysID = "";
            foreach (FileInfo file in files)
            {
                errors = "";
                var ProcessedFName = dbU.ExecuteScalar("select FileName from HOR_parse_files_to_CASS where filename = '" + file.Name + "'");
                if (ProcessedFName == null)
                {
                    Prefly_txt prefly = new Prefly_txt();
                    newsysID = prefly.sysID_Creation(file.FullName).Trim();

                    if (file.Name.ToUpper().IndexOf("UCDSO001") == 0)
                    {
                        var msg = "";
                    }

                    GlobalVar.adtLCDS = GlobalVar.adtLCDS + file.Name.Substring(0, file.Name.Length - 4) + ",";
                    if (file.Name.ToUpper().IndexOf("ALGS") == 0)
                    {
                        try
                        {
                            filesinDir++;
                            NParse_ALGS Algs = new NParse_ALGS();
                            string error = Algs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("EP005703") == 0)   // final notice
                    {
                        try
                        {
                            filesinDir++;
                            NParse_EP Eps = new NParse_EP();
                            string error = Eps.evaluate_EP005703(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("EP") == 0 && file.Name.ToUpper().IndexOf("EP005703") == -1
                                            && file.Name.ToUpper().IndexOf("EP005203") == -1
                                            && file.Name.ToUpper().IndexOf("EP005303") == -1
                                            && file.Name.ToUpper().IndexOf("EP005204") == -1
                                            && file.Name.ToUpper().IndexOf("EP006003") == -1
                                             && file.Name.ToUpper().IndexOf("EP0G") == -1
                                             && file.Name.ToUpper().IndexOf("EPB") == -1
                                             && file.Name.ToUpper().IndexOf("EPM") == -1
                                             && file.Name.ToUpper().IndexOf("EPA") == -1
                                             && file.Name.ToUpper().IndexOf("EPBM") == -1)
                    {

                        try
                        {
                            filesinDir++;
                            NParse_EP Eps = new NParse_EP();
                            string error = Eps.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("EP005203") == 0
                                            || file.Name.ToUpper().IndexOf("EP005303") == 0
                                            || file.Name.ToUpper().IndexOf("EP005204") == 0
                                            || file.Name.ToUpper().IndexOf("EP006003") == 0)
                    {
                        appSets appsets = new appSets();
                        appsets.setVars();
                        Notice_Letter processFiles = new Notice_Letter();
                        processFiles.processNotices(file.FullName, file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                    }
                    if (file.Name.ToUpper().IndexOf("IM") == 0)
                    {
                        try
                        {
                            filesinDir++;
                            NParse_IM IMs = new NParse_IM();
                            string error = IMs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("NAR") == 0)
                    {
                        try
                        {
                            filesinDir++;
                            NParse_NAR NARs = new NParse_NAR();
                            string error = NARs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("PND") == 0)
                    {
                        try
                        {
                            filesinDir++;
                            NParse_PND PNDs = new NParse_PND();
                            string error = PNDs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"), newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("QMLL") == 0)
                    {
                        try
                        {
                            filesinDir++;
                            NParse_QMLL QMLLs = new NParse_QMLL();
                            string error = QMLLs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("UCDS") == 0)
                    {
                        try
                        {
                            string error = "";
                            string Type = file.Name.ToUpper().ToString().Substring(4, 1);
                            if (file.Name.ToUpper().ToString().Substring(0, 5) == "UCDSI")
                                Type = file.Name.ToUpper().ToString().Substring(4, 1);
                            if (arrayEOBS.Any(Type.Contains))
                            {
                                filesinDir++;
                                NParse_UCDS UCDSs = new NParse_UCDS();
                                if (Type == "O")
                                    error = UCDSs.evaluate_EOB_O_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);

                                else
                                    error = UCDSs.evaluate_EOB_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                                totfilesPrecessed++;
                                if (error != "")
                                    errors = errors + error + "\n\n";
                                else
                                    Parsed = Parsed + file.Name;
                            }
                            else
                            {
                                filesinDir++;
                                NParse_UCDS UCDSs = new NParse_UCDS();
                                if (Type == "C")
                                    error = UCDSs.evaluate_EOB_C_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                                else if (file.Name.IndexOf("UCDSAR06") == 0)
                                {
                                    NParse_ALGS Algs = new NParse_ALGS();
                                    error = Algs.evaluate_TXTAR06(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                                }
                                else if (Type == "A")
                                    error = UCDSs.evaluate_EOB_A_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"));
                                else
                                    error = UCDSs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                                totfilesPrecessed++;
                                if (error != "")
                                    errors = errors + error + "\n\n";
                                else
                                    Parsed = Parsed + file.Name;
                            }
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                    if (file.Name.ToUpper().IndexOf("NPR") == 0 || file.Name.ToUpper().IndexOf("NPD") == 0)
                    {
                        try
                        {
                            filesinDir++;
                            NParse_NPR NPRs = new NParse_NPR();
                            string error = NPRs.evaluate_TXT(file.FullName, "", file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }


                    if (file.Name.ToUpper().IndexOf("EPB") == 0)
                    {

                        appSets appsets = new appSets();
                        appsets.setVars();
                        Parse_CBill processFiles = new Parse_CBill();
                        string result = processFiles.Process_Cbills(file.FullName, file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                    }
                    if (file.Name.ToUpper().IndexOf("EP0GH") == 0)
                    {


                        appSets appsets = new appSets();
                        appsets.setVars();
                        Parse_GBill processFiles = new Parse_GBill();
                        string result = processFiles.Process_Gbills(file.FullName, file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);

                    }
                    if (file.Name.ToUpper().IndexOf("EPA") == 0 || file.Name.ToUpper().IndexOf("EPM") == 0)
                    {
                        try
                        {
                            filesinDir++;           /// like NPR
                            NParse_EPA_EPM EPA_EPMs = new NParse_EPA_EPM();
                            string error = EPA_EPMs.evaluate_TXT(file.FullName, file.Name.ToUpper().Substring(0, 3), file.LastWriteTime.ToString("MM/dd/yyyy HH:mm:ss"),newsysID);
                            totfilesPrecessed++;
                            if (error != "")
                                errors = errors + error + "\n\n";
                            else
                                Parsed = Parsed + file.Name;
                        }
                        catch (Exception ez)
                        {
                            errors = errors + file + "  " + ez.Message + "\n\n";
                        }
                    }
                }
                if (errors != "")
                {
                    LogWriter logerror = new LogWriter();
                    //errors
                    logerror.WriteLogToTable("error parsing", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Parsing files", file.Name, errors);
                }
            }
            if (cycleP == "1")
            {
                Notice_Letter processFilesFinal = new Notice_Letter();
                processFilesFinal.produceSummary();
            }


            if (Parsed != "")
            {
                LogWriter logEndProcess = new LogWriter();
                logEndProcess.WriteLogToTable("end of parse", DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss"), "Parse", "Files:" + Parsed);

            }

        }

        
    }
}
