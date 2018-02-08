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
    public class xmlupload_MAPAR
    {
        #region All other Commercial Contract Files
        [Table(Name = "HOR_parse_MAPAR_Client")]
        public class ClientData
        {

            [Column(DbType = "Int NOT NULL", IsPrimaryKey = true, IsDbGenerated = true)]
            public int RowID { get; set; }
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
            public string FromFile { get; set; }
            [Column]
            public string UploadDate { get; set; }
            [Column]
            public string LastEditedBy { get; set; }

            //[Column]
            //public string SequenceOrder { get; set; }
            public IList<Flight> flights = new List<Flight>();
            public IList<RecipientInfo> receipts = new List<RecipientInfo>();
        }

        [Table(Name = "HOR_parse_MAPAR_Flight")]
        public class Flight
        {
            [Column(DbType = "Int NOT NULL", IsPrimaryKey = true, IsDbGenerated = true)]
            public int RowID { get; set; }
            [Column]
            public string ClientTransactionID { get; set; }
            [Column]
            public string FlightNumber { get; set; }
            [Column]
            public string Bind { get; set; }
            [Column]
            public string InsertCode1 { get; set; }
            [Column]
            public string FileName { get; set; }
            [Column]
            public string SequenceOrder { get; set; }
            [Column]
            public string FromFile { get; set; }
            [Column]
            public string UploadDate { get; set; }
            [Column]
            public string NumberOfCopy { get; set; }

        }

        [Table(Name = "HOR_parse_MAPAR_Receipt")]
        public class RecipientInfo
        {
            [Column(DbType = "Int NOT NULL", IsPrimaryKey = true, IsDbGenerated = false)]
            //public int RowID { get; set; }
            //[Column]
            public int Recnum { get; set; }
            [Column]
            public string ClientTransactionID { get; set; }
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
            public string GroupNo { get; set; }
            [Column]
            public string FullName { get; set; }
            [Column]
            public string SubscriberID { get; set; }
            [Column]
            public string EffectiveDate { get; set; }
            [Column]
            public string ContractType { get; set; }
            [Column]
            public string FromFile { get; set; }
            [Column]
            public string UploadDate { get; set; }

        }

        class MyDatabase : DataContext
        {
            private const String LoginString = @"Server=BUSINESSSQL\SQLSERVER2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
            public Table<ClientData> BatchDataXML;
            public Table<Flight> BatchDetail_Flight;
            public Table<RecipientInfo> BatchDetail_Receipt;
            public MyDatabase()
                : base(LoginString)
            //  : base(global::System.Configuration.ConfigurationManager.ConnectionStrings["conStrnjHORizon"].ConnectionString)
            {
            }
        }

        #endregion







        public string MAPAR_UploadXML(string fileName, string TimeCareRadius, string CycleDate)
        {
            int errors = 0;
            string results = "ok";
            GetSet_Recnum getBatch = new GetSet_Recnum();
            int Rn = getBatch.Get_Recnum();
            MyDatabase db = new MyDatabase();
            try
            {
                XDocument xdoc = XDocument.Load(fileName);

                //loaded file name only.
                int ind = fileName.LastIndexOf('\\') + 1;
                string fN = fileName.Substring(ind, (fileName.Length - ind)).Replace(".zip", ".xml");

                //----------------------------------------
                List<ClientData> clientdata = (from cntry in xdoc.Element("BatchRequests").Elements("BatchRequest")

                                               select new ClientData
                                               {
                                                   //RowID = Convert.ToInt32( cntry.Element("ClientTransactionID").Value),
                                                   //Recnum = 0,
                                                   ClientTransactionID = cntry.Element("ClientTransactionID").Value,
                                                   DocumentDate = cntry.Element("DocumentDate").Value,
                                                   BatchID = cntry.Element("BatchID").Value,
                                                   TransactionNo = cntry.Element("TransactionNo").Value,
                                                   //ProductName = cntry.Element("ProductName").Value,
                                                   //BusinessName = cntry.Element("BusinessName").Value,
                                                   //CompanyName = cntry.Element("CompanyName").Value,

                                                   ProductName = cntry.Element("ProductName") == null ? "" : cntry.Element("ProductName").Value,
                                                   BusinessName = cntry.Element("BusinessName") == null ? "" : cntry.Element("BusinessName").Value,
                                                   CompanyName = cntry.Element("CompanyName") == null ? "" : cntry.Element("CompanyName").Value,


                                                   FromFile = fN,
                                                   UploadDate = DateTime.Now.ToString(),
                                                   LastEditedBy = "RChico",

                                                   flights = (from ste in cntry.Element("MailPiece").Elements("Flight")

                                                              select new Flight
                                                              {
                                                                  FlightNumber = (string)ste.Element("FlightNumber"),
                                                                  Bind = (string)ste.Element("Bind"),
                                                                  InsertCode1 = (string)ste.Element("InsertCode1"),
                                                                  FileName = (string)ste.Element("Files").Element("Filename"),
                                                                  SequenceOrder = (string)ste.Element("Files").Element("SequenceOrder"),
                                                                  FromFile = fN,
                                                                  UploadDate = DateTime.Now.ToString(),
                                                                  NumberOfCopy = ste.Element("Files").Element("NumberOfCopy") == null ? "" : (string)ste.Element("Files").Element("NumberOfCopy")
                                                              }).ToList(),

                                                   receipts = (from ste in cntry.Element("MailPiece").Elements("MailingInfo").Elements("RecipientInfo")


                                                               select new RecipientInfo
                                                               {
                                                                   // NumberOfCopy = ste.Element("NumberOfCopy") == null ? "" : (string)ste.Element("NumberOfCopy").Value,
                                                                   Recnum = 0,
                                                                   NameLine1 = (string)ste.Element("NameLine1").Value,
                                                                   NameLine2 = (string)ste.Element("NameLine2").Value,
                                                                   AddressLine1 = (string)ste.Element("AddressLine1").Value,
                                                                   AddressLine2 = (string)ste.Element("AddressLine2").Value,
                                                                   city = (string)ste.Element("city").Value,
                                                                   state = (string)ste.Element("State").Value,
                                                                   zip = (string)ste.Element("Zip").Value,
                                                                   GroupNo = (string)ste.Element("GroupNo").Value,
                                                                   FullName = (string)ste.Element("FullName").Value,
                                                                   SubscriberID = (string)ste.Element("SubscriberID").Value,
                                                                   //EffectiveDate = (string)ste.Element("EffectiveDate").Value,
                                                                   //ContractType = (string)ste.Element("ContractType").Value,
                                                                   FromFile = fN,
                                                                   UploadDate = DateTime.Now.ToString()

                                                               }).ToList()
                                               }).ToList();

                foreach (var seqqqq in clientdata)
                {
                    foreach (var st in seqqqq.receipts)
                    {

                        if (st.Recnum == 0)
                        {
                            st.Recnum = Rn;
                            Rn++;
                        }
                        else
                        {
                            st.Recnum = Rn;
                            Rn++;

                        }
                    }
                }


                foreach (var co in clientdata)
                {
                    db.BatchDataXML.InsertOnSubmit(co);
                    db.SubmitChanges();

                    foreach (var st in co.flights)
                    {
                        st.ClientTransactionID = co.ClientTransactionID;
                        db.BatchDetail_Flight.InsertOnSubmit(st);
                        db.SubmitChanges();

                    }
                    foreach (var st in co.receipts)
                    {
                        st.ClientTransactionID = co.ClientTransactionID;
                        db.BatchDetail_Receipt.InsertOnSubmit(st);
                        db.SubmitChanges();
                    }
                }
            }
            catch (Exception ex)
            {
                results = ex.Message;
                errors++;
            }
            if (errors == 0)
            {
                 File.Move(fileName, Path.GetDirectoryName(fileName) + "\\__" + Path.GetFileName(fileName));

                //insert recordnumber
                getBatch.Set_Recnum((Rn - 1), "HOR_parse_MAPAR_Receipt");
            }
            return results;
        }

    }
}
