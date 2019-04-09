using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;

namespace Horizon_EOBS_Parse
{
  public  class NJHIDParse
    {
        DBUtility dbU;
        DBUtility dbu_169;
        int Recnum = 1;
        long GRecnum = 1;
        public string evaluate_HNJHIDDsnpCards(string fileName, string directoryTXT)
        {
            
            DataSet ds = null;
            dbU = ProcessVars.oDBUtility();
           // dbu_169 = ProcessVars.oDBUtility_169();
            Recnum = 1;
            string x = Path.GetFileName(fileName);
            string y = Path.GetDirectoryName(fileName);
            string z1 = Path.GetFileNameWithoutExtension(fileName);


         
            string z = GetFileNameAfterSplitwithoutExt(z1);
            DateTime dt = DateTime.Now;
            string s = dt.ToString("yyyyMMddHHmmss");
            int errors = 0;
            DataTable NJHMemoryTable = new DataTable("NJHMemoryTable");
            

               string filename = Path.GetFileName(fileName);
                ds = TextToDataSet.Convert(fileName, "NJHMemoryTable", "\t");

                int countforrows = ds.Tables[0].Rows.Count;



           
               var recnum = dbU.ExecuteScalar("SELECT MAX(RECNUM) FROM [dbo].[HOR_parse_SEQ]");
              //  var recnum = dbu_169.ExecuteScalar("SELECT MAX(RECNUM) FROM [dbo].[HOR_parse_SEQ]");
                int recordnumber = 0;
                if (recnum.ToString() == "")

                    GRecnum = 1;
                else
                    GRecnum = Convert.ToInt64(recnum.ToString()) + 1;

                NJHMemoryTable = ds.Tables[0];
                NJHMemoryTable.Columns.Add("RECNUM");
                NJHMemoryTable.Columns.Add("TIMESTAMP");
                NJHMemoryTable.Columns.Add("FILEDATE");
                NJHMemoryTable.Columns.Add("INSERT_PREV");
                NJHMemoryTable.Columns.Add("INSERT");
                NJHMemoryTable.Columns.Add("MEM_LASTNAME");
                NJHMemoryTable.Columns.Add("MEM_FIRSTNAME");
                NJHMemoryTable.Columns.Add("FORM_ID");
                NJHMemoryTable.Columns.Add("FILENAME");
                NJHMemoryTable.Columns.Add("GROUPID");
                NJHMemoryTable.Columns.Add("VARCODE");
                NJHMemoryTable.Columns.Add("GROUP NAME SHORT");
                NJHMemoryTable.Columns.Add("INSURED FNAME");
                NJHMemoryTable.Columns.Add("INSURED LNAME");
                NJHMemoryTable.Columns.Add("INSURED SUFFIX");
                NJHMemoryTable.Columns.Add("INSURED INITIALS");
                NJHMemoryTable.Columns.Add("ADMIN NAME");
                NJHMemoryTable.Columns.Add("ADMIN PRE");
                NJHMemoryTable.Columns.Add("STREET ADDRESS");
                NJHMemoryTable.Columns.Add("CITY");
                NJHMemoryTable.Columns.Add("STATE");
                NJHMemoryTable.Columns.Add("ZIP+4");
                NJHMemoryTable.Columns.Add("GROUP#_PRE");
                NJHMemoryTable.Columns.Add("GROUP#_POST");

                NJHMemoryTable.Columns.Add("GROUP NAME LONG");
                NJHMemoryTable.Columns.Add("MEMBER GRP PREFIX");
                NJHMemoryTable.Columns.Add("PLAN TYPE");
                NJHMemoryTable.Columns.Add("EFFECTIVE DATE");
                NJHMemoryTable.Columns.Add("PLAN CODES");
                NJHMemoryTable.Columns.Add("MEMBER ID");
                NJHMemoryTable.Columns.Add("GRP BUNDLE");
                NJHMemoryTable.Columns.Add("DL");
                NJHMemoryTable.Columns.Add("MED_FLAG");
                NJHMemoryTable.Columns.Add("TYPE");
                NJHMemoryTable.Columns.Add("IMB");
                NJHMemoryTable.Columns.Add("RECIEVESTATUS");
                NJHMemoryTable.Columns.Add("Supress");
               



                FileInfo fileInfo = new System.IO.FileInfo(fileName);
                DateTime lastWriteTime = File.GetLastWriteTime(fileName);
                int SeqNoMLTSS = 1;
                int SeqNoNjFamily = 1;
                string FileDate = lastWriteTime.ToString("MM/dd/yyyy");
                string Filex = z + "_ID_DSNP_" + s;

                //Data Cleansing in inmemory Data Table
                Regex rgx = new Regex(@"^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$");
                foreach (DataRow row in NJHMemoryTable.Rows) // Loop over the rows.
                {

                    try
                    {


                        if (row["PCP_PHONE_NO"].ToString() != "" && row["PCP_PHONE_NO"] != null && !rgx.IsMatch(row["PCP_PHONE_NO"].ToString()))
                        {
                            string fieldval = row["PCP_PHONE_NO"].ToString().Replace("-", "");
                            string PhoneNoWithDash = ProcessVars.GetResultsWithHyphen(fieldval.Trim());
                            row["PCP_PHONE_NO"] = PhoneNoWithDash;
                        }

                        row["RECNUM"] = GRecnum;
                        GRecnum++;

                        row["FILENAME"] = Filex.Trim();
                        row["TIMESTAMP"] = s.Trim();
                        row["FILEDATE"] = FileDate.Trim();
                        if (row["MEMBER_NAME"].ToString().Contains("\"") && row["MEMBER_NAME"].ToString() != null && row["MEMBER_NAME"].ToString() != "")
                        {
                            string memberName = row["MEMBER_NAME"].ToString().Replace("\"", "");
                            row["MEMBER_NAME"] = memberName.Trim();

                        }

                        if (row["PCP_NAME"].ToString() != null && row["PCP_NAME"].ToString() != "" && row["PCP_NAME"].ToString().Contains("\""))
                        {
                            string pcpname = row["PCP_NAME"].ToString().Replace("\"", "");
                            row["PCP_NAME"] = pcpname.Trim();

                        }




                        if (row["MEMBER_NAME"].ToString() != null && row["MEMBER_NAME"].ToString().IndexOf(',') >= 0)
                        {

                            string[] MemberNameSplit = row["MEMBER_NAME"].ToString().Split(',');
                            string MemberLastName = MemberNameSplit[0].ToString().Replace("\"", "").Trim();
                            string MemberFirstName = MemberNameSplit[1].ToString().Replace("\"", "").Trim();
                            row["MEM_LASTNAME"] = MemberLastName;
                            row["MEM_FIRSTNAME"] = MemberFirstName;
                        }

                      
                        if (row["DENTAL_BENFIT"].ToString() != "" && row["DENTAL_COPAY"].ToString() != null)
                        {
                            string dentalbenfit = row["DENTAL_BENFIT"].ToString().Trim();
                            if (dentalbenfit.ToUpper() == "NO")
                            {
                                row["DENTAL_COPAY"] = "N/A";

                            }

                        }


                        try
                        {
                            if (row["MEME_PLAN"].ToString() != null && row["MEME_PLAN"].ToString() != "")
                            {
                                string MemberPlan = row["MEME_PLAN"].ToString().Trim();
                               
                              //  if (MemberPlan.IndexOf('7') >= 0)
                                if (MemberPlan.StartsWith("7"))
                                {
                                    row["FORM_ID"] = "DS100";

                                }
                                else
                                {
                                    row["FORM_ID"] = "ML100";

                                }
                            }


                        }


                        catch (Exception EX)
                        {
                            errors = errors + 1;

                        }
                     
                        
                        if (row["CARD_IND"].ToString() != null && row["CARD_IND"].ToString() != "" && row["MEME_PLAN"] != null && row["MEME_PLAN"] != "")
                        {
                            string CardIndicator = (row["NEW_ENROLLEE_FLAG"].ToString()).Trim();
                            string Meme_plan = (row["MEME_PLAN"].ToString()).Trim();
                            if (CardIndicator == "New Enrollee"  && (Meme_plan.StartsWith("7")))

                                row["INSERT"] = "HNJH_DSNP,HRA,BRE";

                            else

                                row["INSERT"] = "HNJH_DSNP";

                            


                        }


                        if (row["MEME_MEDCD_NO"].ToString() != null && row["MEME_MEDCD_NO"].ToString() != "")
                        {
                        

                            string medcdno = row["MEME_MEDCD_NO"].ToString();

                            int exists1 = Convert.ToInt32(dbU.ExecuteScalar("SELECT count(*) FROM [HNJH_DSNPIDCards] WHERE Meme_Medcd_No='" + medcdno + "'"));
                            if (exists1 == 1)
                            {
                                row["INSERT_PREV"] = "Y";
                            }

                            else
                            {
                                row["INSERT_PREV"] = "";
                            }



                        }


                   

                        try
                        {
                            if (row["FORM_ID"].ToString() != "" && row["FORM_ID"].ToString() != null)
                            {
                                row["GROUPID"] = row["FORM_ID"];

                            }
                        }
                        catch (Exception EX)
                        {
                            errors = errors + 1;

                        }
                        row["VARCODE"] = "";
                        row["GROUP NAME SHORT"] = "";

                        if (row["MEM_FIRSTNAME"].ToString() != "" && row["MEM_FIRSTNAME"] != null)
                        {
                            row["INSURED FNAME"] = row["MEM_FIRSTNAME"];


                        }

                        if (row["MEM_LASTNAME"].ToString() != "" && row["MEM_LASTNAME"] != null)
                        {
                            row["INSURED LNAME"] = row["MEM_LASTNAME"];

                        }

                        row["INSURED SUFFIX"] = DBNull.Value;
                        row["INSURED INITIALS"] = DBNull.Value;
                        if (row["MEME_ADDR1"].ToString() != "" && row["MEME_ADDR1"] != null)
                        {
                            row["ADMIN NAME"] = row["MEME_ADDR1"];

                        }

                        if (row["MEME_ADDR2"].ToString() != "" && row["MEME_ADDR2"] != null)
                        {
                            row["ADMIN PRE"] = row["MEME_ADDR2"];

                        }
                        else if (row["MEME_ADDR3"].ToString() != "" && row["MEME_ADDR3"] != null)
                        {
                            row["ADMIN PRE"] = row["MEME_ADDR3"];
                        }


                        if (row["MEME_ADDR1"].ToString() != "" && row["MEME_ADDR1"] != null)
                        {
                            row["STREET ADDRESS"] = row["MEME_ADDR1"];
                        }
                        else if (row["MEME_ADDR2"].ToString() != "" && row["MEME_ADDR2"] != null)
                        {
                            row["STREET ADDRESS"] = row["MEME_ADDR2"];
                        }

                        if (row["MEME_CITY"].ToString() != "" && row["MEME_CITY"] != null)
                        {
                            row["CITY"] = row["MEME_CITY"];
                        }

                        if (row["MEME_STATE"].ToString() != "" && row["MEME_STATE"] != null)
                        {
                            row["STATE"] = row["MEME_STATE"];
                        }

                        if (row["MEME_ZIP"].ToString() != "" && row["MEME_ZIP"] != null)
                        {
                            row["ZIP+4"] = row["MEME_ZIP"];
                        }


                        row["GROUP#_PRE"] = DBNull.Value;
                        row["GROUP#_POST"] = DBNull.Value;
                        row["GROUP NAME LONG"] = DBNull.Value;
                        row["MEMBER GRP PREFIX"] = DBNull.Value;


                        if (row["MEME_PLAN"].ToString() != "" && row["MEME_PLAN"] != null)
                        {
                            row["PLAN TYPE"] = row["MEME_PLAN"];
                        }


                        if (row["MEME_PLAN_EFF_DT"].ToString() != "" && row["MEME_PLAN_EFF_DT"] != null)
                        {
                            row["EFFECTIVE DATE"] = row["MEME_PLAN_EFF_DT"];
                        }

                        row["PLAN CODES"] = "";

                        if (row["MEME_MEDCD_NO"].ToString() != "" && row["MEME_MEDCD_NO"] != null)
                        {
                            row["MEMBER ID"] = row["MEME_MEDCD_NO"];
                        }
                        row["GRP BUNDLE"] = DBNull.Value;
                        row["DL"] = 'Y';
                        row["MED_FLAG"] = "Y";
                        row["type"] = DBNull.Value;
                        row["Supress"] = DBNull.Value;
                        row["RECIEVESTATUS"] = "Recieve";








                    }
                    catch (Exception ex)
                    {
                        var msg = ex.InnerException;
                        errors = errors + 1;


                    }

                }



                //var lastRow = NJHMemoryTable.Rows[NJHMemoryTable.Rows.Count - 1];   commented R Chico 08-23-2018
                //NJHMemoryTable.Rows.Remove(lastRow);
                //NJHMemoryTable.AcceptChanges();



                dbU = ProcessVars.oDBUtility();
                using (SqlConnection cn = new SqlConnection(ProcessVars.ConnectionString))
                {

                    cn.Open();
                    using (SqlBulkCopy copy = new SqlBulkCopy(cn))
                    {
                        try
                        {
                            copy.DestinationTableName = "HNJH_DSNPIDCards_Temp";

                            copy.ColumnMappings.Add("RECNUM", "Recnum");
                            copy.ColumnMappings.Add("FILENAME", "FileName");
                            copy.ColumnMappings.Add("FILEDATE", "FileDate");
                            copy.ColumnMappings.Add("TIMESTAMP", "Timestamp");
                            // copy.ColumnMappings.Add("SEQ#", "Seq#");
                            copy.ColumnMappings.Add("MEME_ID", "Meme_ID");
                            copy.ColumnMappings.Add("MEMBER_NAME", "Member_Name");
                            copy.ColumnMappings.Add("MEM_LASTNAME", "Meme_LastName");
                            copy.ColumnMappings.Add("MEM_FIRSTNAME", "Meme_FirstName");
                            copy.ColumnMappings.Add("PCP_NAME", "PCP_Name");
                            copy.ColumnMappings.Add("PCP_PHONE_NO", "PCP_Phone_No");
                            copy.ColumnMappings.Add("MEME_MEDCD_NO", "Meme_Medcd_No");
                            copy.ColumnMappings.Add("MEME_ADDR1", "Meme_Addr1");
                            copy.ColumnMappings.Add("MEME_ADDR2", "Meme_Addr2");
                            copy.ColumnMappings.Add("MEME_ADDR3", "Meme_Addr3");
                            copy.ColumnMappings.Add("MEME_CITY", "Meme_City");
                            copy.ColumnMappings.Add("MEME_STATE", "Meme_State");
                            copy.ColumnMappings.Add("MEME_ZIP", "Meme_Zip");
                            copy.ColumnMappings.Add("MEME_PLAN", "Meme_Plan");
                            copy.ColumnMappings.Add("MEME_PLAN_EFF_DT", "Meme_Plan_Eff_Dt");
                            copy.ColumnMappings.Add("DENTAL_BENFIT", "Dental_Benefit");
                            copy.ColumnMappings.Add("EMERGENCY_AMT", "Emergency_Amt");
                            copy.ColumnMappings.Add("PCP_COPAY", "Pcp_CoPay");
                            copy.ColumnMappings.Add("DENTAL_COPAY", "Dental_CoPay");
                            copy.ColumnMappings.Add("SPECIALIST_COPAY", "Specialist_CoPay");
                            copy.ColumnMappings.Add("RX_GENERIC", "Rx_Generic");
                            copy.ColumnMappings.Add("RX_BRAND", "Rx_Brand");
                            copy.ColumnMappings.Add("Source_ID_CARD_REQ", "Source_Id_Card_Req");
                            copy.ColumnMappings.Add("CARD_IND", "Card_Ind");
                            copy.ColumnMappings.Add("INSERT_PREV", "Insert_Prev");
                            copy.ColumnMappings.Add("INSERT", "Insert");
                            copy.ColumnMappings.Add("FORM_ID", "Form_Id");

                            copy.ColumnMappings.Add("VARCODE", "varcode");
                            copy.ColumnMappings.Add("GROUP NAME SHORT", "group name short");
                            copy.ColumnMappings.Add("INSURED FNAME", "insured fname");
                            copy.ColumnMappings.Add("INSURED LNAME", "insured lname");
                            copy.ColumnMappings.Add("INSURED SUFFIX", "insured suffix");
                            copy.ColumnMappings.Add("INSURED INITIALS", "insured initials");
                            copy.ColumnMappings.Add("ADMIN NAME", "admin name");
                            copy.ColumnMappings.Add("ADMIN PRE", "admin pre");
                            copy.ColumnMappings.Add("STREET ADDRESS", "street address");
                            copy.ColumnMappings.Add("CITY", "city");
                            copy.ColumnMappings.Add("STATE", "state");
                            copy.ColumnMappings.Add("ZIP+4", "zip+4");
                            copy.ColumnMappings.Add("GROUP#_PRE", "group#_pre");
                            copy.ColumnMappings.Add("GROUP#_POST", "group#_post");
                            copy.ColumnMappings.Add("GROUP NAME LONG", "group name long");
                            copy.ColumnMappings.Add("MEMBER GRP PREFIX", "member grp prefix");
                            copy.ColumnMappings.Add("PLAN TYPE", "plan type");
                            copy.ColumnMappings.Add("EFFECTIVE DATE", "effective date");
                            copy.ColumnMappings.Add("PLAN CODES", "plan codes");
                            copy.ColumnMappings.Add("GRP BUNDLE", "grp bundle");
                            copy.ColumnMappings.Add("DL", "dl");
                            copy.ColumnMappings.Add("MED_FLAG", "med_flag");
                            copy.ColumnMappings.Add("TYPE", "type");
                            copy.ColumnMappings.Add("RECIEVESTATUS", "RecieveStatus");
                            copy.ColumnMappings.Add("Supress", "Supress");
                            copy.ColumnMappings.Add("RX_ID", "RX_ID");
                            copy.ColumnMappings.Add("NEW_ENROLLEE_FLAG", "NEW_ENROLLEE_FLAG");
                            
                            copy.WriteToServer(NJHMemoryTable);





                        }
                        catch (Exception ex)
                        {
                            errors = errors + 1;


                        }
                    }


                }



                SqlParameter[] sqlParamsLoadedFileName = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };

            //  Sp with added lines :   comment to supress output  and set to N  Raul dec 2017

                DataSet hnj_idcardsDs = dbU.ExecuteDataSet("CleanHNJH_DSNPIDCards_Temp", sqlParamsLoadedFileName);

                string newprocess = "n";
                if (newprocess == "y")
                {
                   

                    //creating csv files to go into hold folder in dsnp path daily.Using above storedproc -CleanHNJH_DSNPIDCards_Temp

                    SqlParameter[] sqlParamsToBCCHeld = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };

                    string HoldFolder = fileInfo.DirectoryName + "\\Hold\\";
                    if (!Directory.Exists(HoldFolder))
                        Directory.CreateDirectory(HoldFolder);

                    string HeldFilename = "DSNP-HOLD_" + z + ".csv";
                    string pNameTHeld = HoldFolder + "\\" + HeldFilename;
                    if (File.Exists(pNameTHeld))
                        File.Delete(pNameTHeld);

                    DataSet hnj_CreateHold = dbU.ExecuteDataSet("HNJHDSNP_CreateTableForDsnpHold", sqlParamsToBCCHeld);
                    if (hnj_CreateHold.Tables[0].Rows.Count > 0)
                    {
                        createCSV createcsvT = new createCSV();

                        var fieldnamesT = new List<string>();
                        for (int index = 0; index < hnj_CreateHold.Tables[0].Columns.Count; index++)
                        {
                            fieldnamesT.Add(hnj_CreateHold.Tables[0].Columns[index].ColumnName);
                        }

                        bool respT2 = createcsvT.addRecordsCSV(pNameTHeld, fieldnamesT);
                        foreach (DataRow row in hnj_CreateHold.Tables[0].Rows)
                        {

                            var rowData = new List<string>();
                            for (int index = 0; index < hnj_CreateHold.Tables[0].Columns.Count; index++)
                            {
                                rowData.Add(row[index].ToString());

                            }
                            respT2 = false;
                            respT2 = createcsvT.addRecordsCSV(pNameTHeld, rowData);


                        }

                    }

                }

                //end creating csv files to go into held folder in dsnp path daily.






                //// //create csv file to bcc machine

                SqlParameter[] sqlParamsToBCC = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };

                string BCCname = "HNJH-ID_" +z+"DSNP" + "_toBCC.csv";
                string pNameT = fileInfo.DirectoryName + "\\" + BCCname;
                if (File.Exists(pNameT))
                    File.Delete(pNameT);
                DataSet hnj_ToBcc = dbU.ExecuteDataSet("HNJHDSNP_CreateTableForBcc", sqlParamsToBCC);
                if (hnj_ToBcc.Tables[0].Rows.Count > 0)
                {
                    createCSV createcsvT = new createCSV();

                    var fieldnamesT = new List<string>();
                    for (int index = 0; index < hnj_ToBcc.Tables[0].Columns.Count; index++)
                    {
                        fieldnamesT.Add(hnj_ToBcc.Tables[0].Columns[index].ColumnName);
                    }

                    bool respT2 = createcsvT.addRecordsCSV(pNameT, fieldnamesT);
                    foreach (DataRow row in hnj_ToBcc.Tables[0].Rows)
                    {

                        var rowData = new List<string>();
                        for (int index = 0; index < hnj_ToBcc.Tables[0].Columns.Count; index++)
                        {
                            rowData.Add(row[index].ToString());

                        }
                        respT2 = false;
                        respT2 = createcsvT.addRecordsCSV(pNameT, rowData);


                    }

                }

                //// copy to CASS
                //BCCname=HNJH-ID_HNJHID110416DSNP_toBCC
                string cassFileName = ProcessVars.gDMPs + BCCname;
                File.Copy(pNameT, cassFileName);

                // // wait foR 3 min
                var t = Task.Run(async delegate
               {
                   await Task.Delay(1000 * 60 * 1);
                   return DateTime.Now.ToString("yyyy-MM-dd hh mm ss");
               });
                t.Wait();


                BackCASS processReturns = new BackCASS();
                string result = processReturns.HNJHDSNPProcessFiles(BCCname);

              
                //After bcc check is done 

                if (result == "")
                {
                    ///create seq# now


                    try
                    {
                        string sqlcommand = "HNJHDSNP_CreateSeqNo";
                        dbU.ExecuteScalar(sqlcommand);
                    }
                    catch (Exception ex)
                    { errors = errors + 1; }







                    //  move data from temptable to master table.

                    try
                    {

                        SqlParameter[] sqlParamsLoadedFileName5 = new SqlParameter[]
                       {                   
                      DBUtility.GetInParameter("@FileName",Filex),
                        };

                        DataSet hnj_MasterDSet = dbU.ExecuteDataSet("HNJH_DsnpMoveDataToMaster", sqlParamsLoadedFileName5);

                    }
                    catch (Exception ex)
                    {
                        errors = errors + 1;
                    }

                    //MOVE ONLY DELEVERABLE TO XMPIE TABLEs-(hnjhid_xmpie and hnjh_mltss)


                    try
                    {
                        SqlParameter[] sqlParamsLoadedFileName2 = new SqlParameter[]
                    {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                    };



                        DataSet hnj_XmpieDSet = dbU.ExecuteDataSet("HNJH_DsnpMoveDataToXmpie", sqlParamsLoadedFileName2);
                    }
                    catch (Exception ex)
                    {
                        errors = errors + 1;
                    }



                    //CREATE .CSV FILES FOR  SCI


                    try
                    {
                        SqlParameter[] sqlParamsLoadedFileName3 = new SqlParameter[]
                    {                   
                    
                        DBUtility.GetInParameter("@FileName",Filex), 
                      
                       
                    };


                        DataSet hnj_dsnp = dbU.ExecuteDataSet("HNJH_DSNP_SCIcsv", sqlParamsLoadedFileName3);


                        DataTable dtable1 = hnj_dsnp.Tables[0];
                        int tablecount = hnj_dsnp.Tables[0].Rows.Count;

                        //HNJHID031016_ID-CD_DSNP_DateTimeStamp.csv
                        string pNameT1 = fileInfo.DirectoryName + "\\";
                        if (hnj_dsnp.Tables[0].Rows.Count > 0)
                        {



                            List<DataTable> dttableaftersplit = new List<DataTable>();
                            dttableaftersplit = SplitTable(dtable1, 10000);
                            DateTime begintime = DateTime.Now;
                            int xc = 1;
                            int k = 1;
                            createCSV createcsv = new createCSV();
                            foreach (var dtable in dttableaftersplit)
                            {
                                //HNJHID121616_ID-CD_DSNP_20161216174704 change to HNJHID121616_ID_DSNP_20161216174704_1.csv 
                                string stringfilepath = pNameT1+z + "_ID_DSNP_" + begintime.ToString("yyyyMMddHHmmss")+xc.ToString() +"_"+k.ToString()+ ".csv";
                                if (File.Exists(stringfilepath))
                                    File.Delete(stringfilepath);

                                createcsv.CreateCSVFile(dtable, stringfilepath);
                                begintime.AddMinutes(2);
                                k = k + 1;
                                xc = xc + 1;
                            }

                        }


                    }
                    catch(Exception ex)
                    {

                        errors = errors + 1;
                    }









                    ///Create HNJHID031016_RCV_20160311172130.csv for horizon

                    SqlParameter[] sqlParamsRcv = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                     
                       
                  };

                    DateTime dt1 = DateTime.Now;
                    string s1 = dt1.ToString("yyyyMMddHHmmss");
                    string OutputFolder = fileInfo.DirectoryName + "\\Output\\";
                    if (!Directory.Exists(OutputFolder))
                        Directory.CreateDirectory(OutputFolder);

                    string RCVname = z + "_RCV_" + s1 + ".csv";
                    string FileName = OutputFolder + "\\" + RCVname;
                    if (File.Exists(FileName))
                        File.Delete(FileName);
                    DataSet hnj_Rcv = dbU.ExecuteDataSet("HNJH_DsnpCreateRcvTableForNJ", sqlParamsRcv);
                    if (hnj_Rcv.Tables[0].Rows.Count > 0)
                    {
                        createCSV createcsvT = new createCSV();

                        var fieldnamesT = new List<string>();
                        for (int index = 0; index < hnj_Rcv.Tables[0].Columns.Count; index++)
                        {
                            fieldnamesT.Add(hnj_Rcv.Tables[0].Columns[index].ColumnName);
                        }

                        bool respT2 = createcsvT.addRecordsCSV(FileName, fieldnamesT);
                        foreach (DataRow row in hnj_Rcv.Tables[0].Rows)
                        {

                            var rowData = new List<string>();
                            for (int index = 0; index < hnj_Rcv.Tables[0].Columns.Count; index++)
                            {
                                rowData.Add(row[index].ToString());

                            }
                            respT2 = false;
                            respT2 = createcsvT.addRecordsCSV(FileName, rowData);


                        }

                    }

                    ///Create HNJHID031016_AV_20160311182514.xlsx for horizon

                    SqlParameter[] sqlParamsNonDeliverable = new SqlParameter[]
                 {                   
                      DBUtility.GetInParameter("@FileName",Filex), 
                                      
                  };

                    DateTime dtToday = DateTime.Now;
                    string s2 = dtToday.ToString("yyyyMMddHHmmss");


                    string AVName = z + "_AV_" + s2 + ".xlsx";
                    if (!Directory.Exists(OutputFolder))
                        Directory.CreateDirectory(OutputFolder);
                    string AVFileName = OutputFolder + "\\" + AVName;

                    if (File.Exists(AVFileName))
                        File.Delete(AVFileName);
                    DataSet hnj_ACV = dbU.ExecuteDataSet("HNJH_DsnpCreateAcvTableForNJ", sqlParamsNonDeliverable);
                    if (hnj_ACV.Tables[0].Rows.Count > 0)
                    {

                        ClassExcel excelcreate = new ClassExcel();
                        excelcreate.createExcelFile(hnj_ACV, OutputFolder + "\\" + AVName);
                      


                    }

                }

                if (errors == 0)
                {

                    try
                    {

                        var recnumFromTemp = dbU.ExecuteScalar("select max(recnum) from HNJH_DSNPIDCards_Temp");

                        //  dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (GRecnum-1) + ",'HNJH_IDCards', GETDATE())");
                        dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Convert.ToInt64(recnumFromTemp.ToString())) + ",'HNJH_DSNPIDCards', GETDATE())");

                       // dbu_169.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Convert.ToInt64(recnumFromTemp.ToString())) + ",'HNJH_DSNPIDCards', GETDATE())");

                    }
                    catch (Exception EX)
                    {
                        errors = errors + 1;
                    }

                }
            


            return "";




        }




        public string GetFileNameAfterSplitwithoutExt(string txtFileName)
        {

            //incoming file name=HNJHID_Medicare_12162016 make it HNJHID12162016
            string[] stringaftersplit;
            stringaftersplit = (txtFileName.Split('_'));
            string stringafterremovingtxt = stringaftersplit[2].ToString().Remove(4) + DateTime.Now.ToString("yy");

            //string finalstring = string.Concat(stringaftersplit[0].ToString(), stringafterremovingtxt);
            //return finalstring;
            string dateFileName = stringaftersplit[2].ToString();
            string finalstring = "";
            if (dateFileName.ToString().Substring(0, 4) == DateTime.Today.Year.ToString() && dateFileName.Length == 8)
            {
                finalstring = string.Concat(stringaftersplit[0].ToString(), dateFileName.ToString().Replace(".txt", ""));
            }
            else
            {
                DateTime dt = new DateTime(Int32.Parse(dateFileName.Substring(4, 4)), Int32.Parse(dateFileName.Substring(0, 2)), Int32.Parse(dateFileName.Substring(2, 2)));
                int julianF = dt.Year * 1000 + dt.DayOfYear;
                 finalstring = string.Concat(stringaftersplit[0].ToString(), stringafterremovingtxt);
                if (stringaftersplit.Length == 4)
                    finalstring = string.Concat(stringaftersplit[0].ToString(), julianF, stringaftersplit[3].ToString().Replace(".txt", ""));
                else
                    finalstring = string.Concat(stringaftersplit[0].ToString(), julianF, "0");
            }
            return finalstring;


        }










    public List<DataTable> SplitTable(DataTable originalTable, int batchSize)
    {
        List<DataTable> tables = new List<DataTable>();
        int i = 0;
        int j = 1;
        DataTable newDt = originalTable.Clone();
        newDt.TableName = "Table_" + j;
        newDt.Clear();
        foreach (DataRow row in originalTable.Rows)
        {
            DataRow newRow = newDt.NewRow();
            newRow.ItemArray = row.ItemArray;
            newDt.Rows.Add(newRow);
            i++;
            if (i == batchSize)
            {
                tables.Add(newDt);
                j++;
                newDt = originalTable.Clone();
                newDt.TableName = "Table_" + j;
                newDt.Clear();
                i = 0;
            }



        }
        if (newDt.Rows.Count > 0)
        {
            tables.Add(newDt);
            j++;
            newDt = originalTable.Clone();
            newDt.TableName = "Table_" + j;
            newDt.Clear();

        }
        return tables;
    }






       














    }

    






}
