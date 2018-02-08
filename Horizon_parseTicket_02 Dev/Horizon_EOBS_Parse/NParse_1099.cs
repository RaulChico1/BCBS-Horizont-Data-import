using System;
using System.Collections.Generic;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Text;

namespace Horizon_EOBS_Parse
{
    public class NParse_1099
    {
         DBUtility dbU;
         int Recnum = 1;
         int prevIndex = 0;
         int currcol = 0;
         DataTable Data1099 = Data_Table();
         List<string> details1 = new List<string>();
         List<string> details2 = new List<string>();
        
         string currLine = "";
         string O_ImportDate = "";
         string O_FileName = "";
         public string ProcessFiles(string dateProcess)
         {
             string result = "";
             

             GlobalVar.dbaseName = "BCBS_Horizon";
             dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
             Recnum = 1;

             var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");
             if (recnum.ToString() == "")
                 Recnum = 1;
             else
                 Recnum = Convert.ToInt32(recnum.ToString()) + 1;


            

             //DataTable d1099 = dbU.ExecuteDataTable("select * from hor_parse_D1099_Original where convert(date,dateimport) = convert(date,getdate()) order by O_Seq");
             DataTable d1099 = dbU.ExecuteDataTable("select * from hor_parse_D1099_Original where convert(date,dateimport) = '2016-02-04' order by [filename],[Letter No], [Index]");
             foreach (DataRow row in d1099.Rows)
             {
                 if(row["Index"].ToString() == "1")
                 {
                     if (prevIndex == 1)
                     {
                         addToTable(currLine);
                     }

                      currLine = row[0].ToString();
                      //currLine = row[4].ToString();
                      //currLine = row[4].ToString();
                      O_ImportDate = row["DateImport"].ToString();
                      O_FileName = row["FileName"].ToString();
                      //var rowN = DataTable.NewRow();
                     //rowN[0] = Convert.ToInt32( row[3].ToString());
                     //rowN[1] = Convert.ToInt32(row[4].ToString());
                     
                     //save details
                    
                     prevIndex = 1;

                     details1.Add(row[2].ToString().Trim());
                     for (int j = 0; j < 28; j++)
                     {
                         details1.Add(row[j + 3].ToString().Trim());
                     }
                     currcol = 29;
                 }

                 else if (Convert.ToInt16(row["Index"].ToString()) > 1 && Convert.ToInt16(row["Index"].ToString()) < 13)
                 {
                     int indexx = Convert.ToInt16(row["Index"].ToString());

                     for (int j = 5; j < 9; j++)
                     {
                         details2.Add(row[j].ToString().Trim());
                     }
                     for (int j2 = 14; j2 < 31; j2++)
                     {
                         details2.Add(row[j2].ToString().Trim());
                     }
                     currcol = currcol + 21;
                 }



             }
             if (details1.Count > 0)
                         addToTable(currLine);


             //for (int i = Data1099.Rows.Count - 1; i >= 0; i--)
             //{
             //    // whatever your criteria is
             //    if (Data1099.Rows[i]["DOB4"] == DBNull.Value)
             //        Data1099.Rows[i].Delete();
             //    // Data1099.Rows[i]["FyllYear13"] = "XX";
             //}



             createCSV createcsv = new createCSV();
             string pName = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + DateTime.Now.ToString("yyyy-MM-dd") + @"\from_FTP\D1099\D1099_process_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd_HH_mm") + ".csv";
             //string pName = ProcessVars.OutputDirectory + sysout + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm") + ".csv";
             if(!Directory.Exists(@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + DateTime.Now.ToString("yyyy-MM-dd") + @"\from_FTP\D1099\"))
                Directory.CreateDirectory(@"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\" + DateTime.Now.ToString("yyyy-MM-dd") + @"\from_FTP\D1099\");
             if (File.Exists(pName))
                 File.Delete(pName);
             var fieldnames = new List<string>();
             for (int index = 0; index < Data1099.Columns.Count; index++)
             {
                 string nColname = "";
                 string colname = Data1099.Columns[index].ColumnName;
                
                     fieldnames.Add(colname);
             }
             bool resp = createcsv.addRecordsCSV(pName, fieldnames);
             resp = createcsv.addRecordsCSV(pName, fieldnames);
             foreach (DataRow row in Data1099.Rows)
             {

                 var rowData = new List<string>();
                 for (int index = 0; index < Data1099.Columns.Count; index++)
                 {
                     rowData.Add(row[index].ToString());
                 }
                 resp = false;
                 resp = createcsv.addRecordsCSV(pName, rowData);
                 //if (UpdSQL != "")
                 //    dbU.ExecuteScalar(UpdSQL + row[0]);
             }

             string errors = "";
             GlobalVar.dbaseName = "BCBS_Horizon";
             dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
             dbU.ExecuteScalar("delete from HOR_Parse_D1099_TMP");

             int updErrors = 0;
             SqlConnection Connection = new SqlConnection(ConfigurationManager.ConnectionStrings[GlobalVar.connectionKey].ConnectionString);

             Connection.Open();

             using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection))
             {
                 //bulkCopy.DestinationTableName =
                 //    "[dbo].[Tempo_fsaData]";
                 bulkCopy.DestinationTableName = "[dbo].[HOR_Parse_D1099_TMP]";

                 try
                 {
                     // Write from the source to the destination.
                     bulkCopy.WriteToServer(Data1099);
                 }
                 catch (Exception ex)
                 {
                     errors = errors + ex.Message;
                     updErrors++;
                 }
             }
             Connection.Close();
             dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + (Recnum - 1) + ",'HOR_Parse_D1099', GETDATE())");
             dbU.ExecuteScalar("Insert into HOR_Parse_D1099 select * from HOR_Parse_D1099_TMP");

             return result;
         }
         public void addToTable(string currline)
         {
             prevIndex = 1;
             string ok = "";
             var row = Data1099.NewRow();
             try
             {
                
                 for (int j = 0; j < 29; j++)
                 {
                     row[j] = details1[j].Trim();
                 }
                 if (details2.Count > 0)
                 {
                     int nindex2 = 29;
                     for (int j = 0; j < details2.Count ; j++)
                     {
                         row[nindex2] = details2[j].Trim();
                         nindex2++;
                     }
                 }
               
                 row[281] = Recnum;
                 row[282] = O_ImportDate;
                 row[283] = O_FileName;

             

                 Data1099.Rows.Add(row);
                 details1.Clear();
                 details2.Clear();
                
                 Recnum++;
             }
             catch (Exception ex)
             {
                 string error = ex.Message + "\n\n";

             }
         }
         private static DataTable Data_Table()
         {
             DataTable newt = new DataTable();
             newt.Clear();
             newt.Columns.Add("LetterNo");
             //newt.Columns.Add("Index");
             newt.Columns.Add("INS_CO_ADMIN");
             newt.Columns.Add("FID");
             newt.Columns.Add("CustCCID");
             newt.Columns.Add("FirstName");
             newt.Columns.Add("LastName");
             newt.Columns.Add("SubCode");
             newt.Columns.Add("Street");
             newt.Columns.Add("Street2");
             newt.Columns.Add("City");
             newt.Columns.Add("State");
             newt.Columns.Add("Zip");
             newt.Columns.Add("DOB");
             newt.Columns.Add("MainGrp");
             newt.Columns.Add("UppEffdate");
             newt.Columns.Add("UppTermDate");
             newt.Columns.Add("Jan");
             newt.Columns.Add("Feb");
             newt.Columns.Add("Mar");
             newt.Columns.Add("Apr");
             newt.Columns.Add("May");
             newt.Columns.Add("Jun");
             newt.Columns.Add("Jul");
             newt.Columns.Add("Aug");
             newt.Columns.Add("Sep");
             newt.Columns.Add("Oct");
             newt.Columns.Add("Nov");
             newt.Columns.Add("Dec");
             newt.Columns.Add("FyllYear");

             newt.Columns.Add("CustCCID1");
             newt.Columns.Add("FirstName1");
             newt.Columns.Add("LastName1");
             newt.Columns.Add("SubCode1");
             newt.Columns.Add("DOB1");
             newt.Columns.Add("MainGrp1");
             newt.Columns.Add("UppEffdate1");
             newt.Columns.Add("UppTermDate1");
             newt.Columns.Add("Jan1");
             newt.Columns.Add("Feb1");
             newt.Columns.Add("Mar1");
             newt.Columns.Add("Apr1");
             newt.Columns.Add("May1");
             newt.Columns.Add("Jun1");
             newt.Columns.Add("Jul1");
             newt.Columns.Add("Aug1");
             newt.Columns.Add("Sep1");
             newt.Columns.Add("Oct1");
             newt.Columns.Add("Nov1");
             newt.Columns.Add("Dec1");
             newt.Columns.Add("FyllYear1");

             newt.Columns.Add("CustCCID2");
             newt.Columns.Add("FirstName2");
             newt.Columns.Add("LastName2");
             newt.Columns.Add("SubCode2");
             newt.Columns.Add("DOB2");
             newt.Columns.Add("MainGrp2");
             newt.Columns.Add("UppEffdate2");
             newt.Columns.Add("UppTermDate2");
             newt.Columns.Add("Jan2");
             newt.Columns.Add("Feb2");
             newt.Columns.Add("Mar2");
             newt.Columns.Add("Apr2");
             newt.Columns.Add("May2");
             newt.Columns.Add("Jun2");
             newt.Columns.Add("Jul2");
             newt.Columns.Add("Aug2");
             newt.Columns.Add("Sep2");
             newt.Columns.Add("Oct2");
             newt.Columns.Add("Nov2");
             newt.Columns.Add("Dec2");
             newt.Columns.Add("FyllYear2");

             newt.Columns.Add("CustCCID3");
             newt.Columns.Add("FirstName3");
             newt.Columns.Add("LastName3");
             newt.Columns.Add("SubCode3");
             newt.Columns.Add("DOB3");
             newt.Columns.Add("MainGrp3");
             newt.Columns.Add("UppEffdate3");
             newt.Columns.Add("UppTermDate3");
             newt.Columns.Add("Jan3");
             newt.Columns.Add("Feb3");
             newt.Columns.Add("Mar3");
             newt.Columns.Add("Apr3");
             newt.Columns.Add("May3");
             newt.Columns.Add("Jun3");
             newt.Columns.Add("Jul3");
             newt.Columns.Add("Aug3");
             newt.Columns.Add("Sep3");
             newt.Columns.Add("Oct3");
             newt.Columns.Add("Nov3");
             newt.Columns.Add("Dec3");
             newt.Columns.Add("FyllYear3");

             newt.Columns.Add("CustCCID4");
             newt.Columns.Add("FirstName4");
             newt.Columns.Add("LastName4");
             newt.Columns.Add("SubCode4");
             newt.Columns.Add("DOB4");
             newt.Columns.Add("MainGrp4");
             newt.Columns.Add("UppEffdate4");
             newt.Columns.Add("UppTermDate4");
             newt.Columns.Add("Jan4");
             newt.Columns.Add("Feb4");
             newt.Columns.Add("Mar4");
             newt.Columns.Add("Apr4");
             newt.Columns.Add("May4");
             newt.Columns.Add("Jun4");
             newt.Columns.Add("Jul4");
             newt.Columns.Add("Aug4");
             newt.Columns.Add("Sep4");
             newt.Columns.Add("Oct4");
             newt.Columns.Add("Nov4");
             newt.Columns.Add("Dec4");
             newt.Columns.Add("FyllYear4");

             newt.Columns.Add("CustCCID5");
             newt.Columns.Add("FirstName5");
             newt.Columns.Add("LastName5");
             newt.Columns.Add("SubCode5");
             newt.Columns.Add("DOB5");
             newt.Columns.Add("MainGrp5");
             newt.Columns.Add("UppEffdate5");
             newt.Columns.Add("UppTermDate5");
             newt.Columns.Add("Jan5");
             newt.Columns.Add("Feb5");
             newt.Columns.Add("Mar5");
             newt.Columns.Add("Apr5");
             newt.Columns.Add("May5");
             newt.Columns.Add("Jun5");
             newt.Columns.Add("Jul5");
             newt.Columns.Add("Aug5");
             newt.Columns.Add("Sep5");
             newt.Columns.Add("Oct5");
             newt.Columns.Add("Nov5");
             newt.Columns.Add("Dec5");
             newt.Columns.Add("FyllYear5");

             newt.Columns.Add("CustCCID6");
             newt.Columns.Add("FirstName6");
             newt.Columns.Add("LastName6");
             newt.Columns.Add("SubCode6");
             newt.Columns.Add("DOB6");
             newt.Columns.Add("MainGrp6");
             newt.Columns.Add("UppEffdate6");
             newt.Columns.Add("UppTermDate6");
             newt.Columns.Add("Jan6");
             newt.Columns.Add("Feb6");
             newt.Columns.Add("Mar6");
             newt.Columns.Add("Apr6");
             newt.Columns.Add("May6");
             newt.Columns.Add("Jun6");
             newt.Columns.Add("Jul6");
             newt.Columns.Add("Aug6");
             newt.Columns.Add("Sep6");
             newt.Columns.Add("Oct6");
             newt.Columns.Add("Nov6");
             newt.Columns.Add("Dec6");
             newt.Columns.Add("FyllYear6");

             newt.Columns.Add("CustCCID7");
             newt.Columns.Add("FirstName7");
             newt.Columns.Add("LastName7");
             newt.Columns.Add("SubCode7");
             newt.Columns.Add("DOB7");
             newt.Columns.Add("MainGrp7");
             newt.Columns.Add("UppEffdate7");
             newt.Columns.Add("UppTermDate7");
             newt.Columns.Add("Jan7");
             newt.Columns.Add("Feb7");
             newt.Columns.Add("Mar7");
             newt.Columns.Add("Apr7");
             newt.Columns.Add("May7");
             newt.Columns.Add("Jun7");
             newt.Columns.Add("Jul7");
             newt.Columns.Add("Aug7");
             newt.Columns.Add("Sep7");
             newt.Columns.Add("Oct7");
             newt.Columns.Add("Nov7");
             newt.Columns.Add("Dec7");
             newt.Columns.Add("FyllYear7");

             newt.Columns.Add("CustCCID8");
             newt.Columns.Add("FirstName8");
             newt.Columns.Add("LastName8");
             newt.Columns.Add("SubCode8");
             newt.Columns.Add("DOB8");
             newt.Columns.Add("MainGrp8");
             newt.Columns.Add("UppEffdate8");
             newt.Columns.Add("UppTermDate8");
             newt.Columns.Add("Jan8");
             newt.Columns.Add("Feb8");
             newt.Columns.Add("Mar8");
             newt.Columns.Add("Apr8");
             newt.Columns.Add("May8");
             newt.Columns.Add("Jun8");
             newt.Columns.Add("Jul8");
             newt.Columns.Add("Aug8");
             newt.Columns.Add("Sep8");
             newt.Columns.Add("Oct8");
             newt.Columns.Add("Nov8");
             newt.Columns.Add("Dec8");
             newt.Columns.Add("FyllYear8");

             newt.Columns.Add("CustCCID9");
             newt.Columns.Add("FirstName9");
             newt.Columns.Add("LastName9");
             newt.Columns.Add("SubCode9");
             newt.Columns.Add("DOB9");
             newt.Columns.Add("MainGrp9");
             newt.Columns.Add("UppEffdate9");
             newt.Columns.Add("UppTermDate9");
             newt.Columns.Add("Jan9");
             newt.Columns.Add("Feb9");
             newt.Columns.Add("Mar9");
             newt.Columns.Add("Apr9");
             newt.Columns.Add("May9");
             newt.Columns.Add("Jun9");
             newt.Columns.Add("Jul9");
             newt.Columns.Add("Aug9");
             newt.Columns.Add("Sep9");
             newt.Columns.Add("Oct9");
             newt.Columns.Add("Nov9");
             newt.Columns.Add("Dec9");
             newt.Columns.Add("FyllYear9");

             newt.Columns.Add("CustCCID10");
             newt.Columns.Add("FirstName10");
             newt.Columns.Add("LastName10");
             newt.Columns.Add("SubCode10");
             newt.Columns.Add("DOB10");
             newt.Columns.Add("MainGrp10");
             newt.Columns.Add("UppEffdate10");
             newt.Columns.Add("UppTermDate10");
             newt.Columns.Add("Jan10");
             newt.Columns.Add("Feb10");
             newt.Columns.Add("Mar10");
             newt.Columns.Add("Apr10");
             newt.Columns.Add("May10");
             newt.Columns.Add("Jun10");
             newt.Columns.Add("Jul10");
             newt.Columns.Add("Aug10");
             newt.Columns.Add("Sep10");
             newt.Columns.Add("Oct10");
             newt.Columns.Add("Nov10");
             newt.Columns.Add("Dec10");
             newt.Columns.Add("FyllYear10");

             newt.Columns.Add("CustCCID11");
             newt.Columns.Add("FirstName11");
             newt.Columns.Add("LastName11");
             newt.Columns.Add("SubCode11");
             newt.Columns.Add("DOB11");
             newt.Columns.Add("MainGrp11");
             newt.Columns.Add("UppEffdate11");
             newt.Columns.Add("UppTermDate11");
             newt.Columns.Add("Jan11");
             newt.Columns.Add("Feb11");
             newt.Columns.Add("Mar11");
             newt.Columns.Add("Apr11");
             newt.Columns.Add("May11");
             newt.Columns.Add("Jun11");
             newt.Columns.Add("Jul11");
             newt.Columns.Add("Aug11");
             newt.Columns.Add("Sep11");
             newt.Columns.Add("Oct11");
             newt.Columns.Add("Nov11");
             newt.Columns.Add("Dec11");
             newt.Columns.Add("FyllYear11");

             newt.Columns.Add("CustCCID12");
             newt.Columns.Add("FirstName12");
             newt.Columns.Add("LastName12");
             newt.Columns.Add("SubCode12");
             newt.Columns.Add("DOB12");
             newt.Columns.Add("MainGrp12");
             newt.Columns.Add("UppEffdate12");
             newt.Columns.Add("UppTermDate12");
             newt.Columns.Add("Jan12");
             newt.Columns.Add("Feb12");
             newt.Columns.Add("Mar12");
             newt.Columns.Add("Apr12");
             newt.Columns.Add("May12");
             newt.Columns.Add("Jun12");
             newt.Columns.Add("Jul12");
             newt.Columns.Add("Aug12");
             newt.Columns.Add("Sep12");
             newt.Columns.Add("Oct12");
             newt.Columns.Add("Nov12");
             newt.Columns.Add("Dec12");
             newt.Columns.Add("FyllYear12");

             newt.Columns.Add("Recnum");
             newt.Columns.Add("ImportDate");
             newt.Columns.Add("FileName");

             return newt;

         }
    }
}
