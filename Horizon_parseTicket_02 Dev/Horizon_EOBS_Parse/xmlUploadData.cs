using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Xml.Linq;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.IO;

namespace Horizon_EOBS_Parse
{
    [Table(Name = "HOR_Parse_TH_Letters_Detail_xml")]
    public class MAPdata
    {
        [Column(DbType = "Int NOT NULL", IsPrimaryKey = true, IsDbGenerated = false)]
        //public int RowID { get; set; }
        public int Recnum { get; set; }
        [Column]
        public string ImportDate { get; set; }
        [Column]
        public string Cycle { get; set; }
        [Column]
        public string cycledate { get; set; }
        [Column]
        public string OriginalFileName { get; set; }
        [Column]
        public string ClientTransactionID { get; set; }
        [Column]
        public string DocumentDate { get; set; }
        [Column]
        public string BatchID { get; set; }
        [Column]
        public string TransactionNo { get; set; }
        [Column]
        public string ProductName { get; set; }
        [Column]
        public string BusinessName { get; set; }
        [Column]
        public string CompanyName { get; set; }
        [Column]
        public string FlightNumber { get; set; }
        [Column]
        public string Bind { get; set; }
        [Column]
        public string InsertCode1 { get; set; }
        [Column]
        public string Filename { get; set; }
        [Column]
        public string SequenceOrder { get; set; }
        [Column]
        public string NumberOfCopy { get; set; }
        [Column]
        public string NameLine1 { get; set; }
        [Column]
        public string NameLine2 { get; set; }
        [Column]
        public string AddressLine1 { get; set; }
        [Column]
        public string AddressLine2 { get; set; }
        [Column]
        public string city { get; set; }
        [Column]
        public string state { get; set; }
        [Column]
        public string zip { get; set; }
        [Column]
        public string groupno { get; set; }
        [Column]
        public string fullname { get; set; }
        [Column]
        public string SubscriberID { get; set; }
        [Column]
        public string IncludeBRE { get; set; }

    }
    class MAP_Database : DataContext
    {

        private const String LoginString = @"Server=BUSINESSSQL\SQLSERVER2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
        public Table<MAPdata> HOR_Parse_TH_Letters_Detail_xml;
        public MAP_Database()
            : base(LoginString)
        {
        }
    }
    public class xmlUploadData
    {
        public string UploadXML(string fileName, string TimeCareRadius, string CycleDate)
        {
            string fustFName = Path.GetFileName(fileName);

            int errors = 0;
            string results = "ok";
            //string newCycle = "00";
            MAP_Database db = new MAP_Database();

            GetSet_Recnum getBatch = new GetSet_Recnum();
            string newCycle = getBatch.getNextCycle("HOR_Parse_TH_Letters_Detail_xml", DateTime.Now.ToString("yyyy-MM-dd"));
            int Recnum = getBatch.Get_Recnum();


            try
            {
                DataSet ds = new DataSet();
                //ds.ReadXml(fileName);
                //string myXMLfile = @"C:\MySchema.xml";

                XElement XTemp = XElement.Load(fileName);
                var queryCDATAXML = from element in XTemp.DescendantNodes()
                                    where element.NodeType == System.Xml.XmlNodeType.CDATA
                                    select element.Parent.Value.Trim();

                XDocument xdoc = XDocument.Load(fileName);

                List<MAPdata> clientdata = (from cntry in xdoc.Element("BatchRequests").Elements("index")
                                           orderby Convert.ToInt32(cntry.Element("batchTransactionId").Value) ascending

                                            select new MAPdata
                                           {
                                              
                                               ImportDate = TimeCareRadius,
                                               //Cycle = newCycle,
                                               //cycledate = CycleDate,
                                               //OriginalFileName = fustFName,
                                               //ClientTransactionID = cntry.Element("ClientTransactionID").Value,
                                               //DocumentDate = cntry.Element("DocumentDate").Value,
                                               //BatchID = cntry.Element("BatchID").Value,
                                               //TransactionNo = cntry.Element("TransactionNo").Value,
                                               //ProductName = cntry.Element("fax").Element("faxNumber").Value,
                                               //coverPageName = cntry.Element("fax").Element("coverPageName").Value,
                                               //coverPageAddress1 = cntry.Element("fax").Element("coverPageAddress1").Value,
                                               //coverPageAddress2 = cntry.Element("fax").Element("coverPageAddress2").Value,
                                               //coverPageAddress3 = cntry.Element("fax").Element("coverPageAddress3").Value,
                                               //coverPageAddress4 = cntry.Element("fax").Element("coverPageAddress4").Value,
                                               //coverPageCity = cntry.Element("fax").Element("coverPageCity").Value,
                                               //coverPageState = cntry.Element("fax").Element("coverPageState").Value,
                                               //coverPageZIP = cntry.Element("fax").Element("coverPageZIP").Value,
                                               //fileName = cntry.Element("archive").Element("fileName").Value,
                                               //folderPath = cntry.Element("archive").Element("folderPath").Value,
                                               //fallBackToPrintOnFaxFailure = cntry.Element("archive").Element("fallBackToPrintOnFaxFailure").Value,
                                               IncludeBRE = cntry.Element("archive").Element("IncludeBRE").Value
                                           }).ToList();




                foreach (var seqqqq in clientdata)
                {
                    if (seqqqq.Recnum == 0)
                    {
                        seqqqq.Recnum = Recnum;
                        Recnum++;
                    }
                }


                foreach (var co in clientdata)
                {
                    //Console.WriteLine(co.batchTransactionId);
                    db.HOR_Parse_TH_Letters_Detail_xml.InsertOnSubmit(co);
                    db.SubmitChanges();
                }
            }
            catch (Exception ex)
            {
                results = ex.Message;
                errors++;
            }
            if (errors == 0)
                File.Move(fileName, Path.GetDirectoryName(fileName) + "\\__" + Path.GetFileName(fileName));

            //insert recordnumber
            getBatch.Set_Recnum((Recnum - 1), "HOR_Parse_TH_Letters_Detail_xml");


            return results;
        }
    }
}
