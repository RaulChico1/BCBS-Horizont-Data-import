using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Configuration;
using System.Data;
using System.Xml;
using System.Text;
using System.Data.SqlClient;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Data.Linq;
using System.Data.Linq.Mapping;



public partial class _Default : System.Web.UI.Page
{
    [Table(Name = "BatchDataXML")]

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
        //[Column]
        //public string FlightNumber { get; set; }
        //[Column]
        //public string Bind { get; set; }
        //[Column]
        //public string InsertCode1 { get; set; }
        //[Column]
        //public string FileName { get; set; }
        //[Column]
        //public string SequenceOrder { get; set; }
        public IList<Flight> flights = new List<Flight>();
        public IList<Receipt> receipts = new List<Receipt>();
    }

    [Table(Name = "BatchDetail_Flight")]
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

    }
    [Table(Name = "BatchDetail_Receipt")]
    public class Receipt
    {
        [Column(DbType = "Int NOT NULL", IsPrimaryKey = true, IsDbGenerated = true)]
        public int RowID { get; set; }
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
    }
    class MyDatabase : DataContext
    {

        private const String LoginString = @"Server=BusinessSQL\sqlserver2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
        public Table<ClientData> BatchDataXML;
        public Table<Flight> BatchDetail_Flight;
        public Table<Receipt> BatchDetail_Receipt;
        public MyDatabase()
            : base(LoginString)
        {
        }
    }

  

    protected void Page_Load(object sender, EventArgs e)
    {
        try
        {
            string connStr = ConfigurationManager.ConnectionStrings["conStrProd"].ConnectionString;
            XDocument xdoc = XDocument.Load("C:/BCBS_Horizon_Data/MultiFlight_SABKLCNT_Test.XML");

            List<ClientData> clientdata = (from cntry in xdoc.Element("BatchRequests").Elements("BatchRequest")

                                           select new ClientData

                                           {
                                               //RowID = Convert.ToInt32( cntry.Element("ClientTransactionID").Value),
                                               ClientTransactionID = cntry.Element("ClientTransactionID").Value,
                                               DocumentDate = cntry.Element("DocumentDate").Value,
                                               BatchID = cntry.Element("BatchID").Value,
                                               TransactionNo = cntry.Element("TransactionNo").Value,
                                               ProductName = cntry.Element("ProductName").Value,
                                               BusinessName = cntry.Element("BusinessName").Value,
                                               CompanyName = cntry.Element("CompanyName").Value,

                                               flights = (from ste in cntry.Element("MailPiece").Elements("Flight")

                                                          select new Flight
                                                       {
                                                           FlightNumber = (string)ste.Element("FlightNumber"),
                                                           Bind = (string)ste.Element("Bind"),
                                                           InsertCode1 = (string)ste.Element("InsertCode1"),
                                                           FileName = (string)ste.Element("Files").Element("Filename"),
                                                           SequenceOrder = (string)ste.Element("Files").Element("SequenceOrder")

                                                       }).ToList(),

                                               receipts = (from ste in cntry.Element("MailPiece").Elements("MailingInfo").Elements("RecipientInfo")


                                                         select new Receipt
                                                       {
                                                           NumberOfCopy = (string)ste.Element("NumberOfCopy").Value,
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
                                                           EffectiveDate = (string)ste.Element("EffectiveDate").Value,
                                                           ContractType = (string)ste.Element("ContractType").Value

                                                       }).ToList()
                                           }).ToList();
            MyDatabase db = new MyDatabase();
 

            foreach (var co in clientdata)
            {

                Console.WriteLine(co.ClientTransactionID);



                db.BatchDataXML.InsertOnSubmit(co);
                    db.SubmitChanges();



                    foreach (var st in co.flights)
                {
                    st.ClientTransactionID = co.ClientTransactionID;
                    db.BatchDetail_Flight.InsertOnSubmit(st);
                    db.SubmitChanges();
                    Console.WriteLine(st.FileName);

                }
                    foreach (var st in co.receipts)
                    {
                        st.ClientTransactionID = co.ClientTransactionID;
                        db.BatchDetail_Receipt.InsertOnSubmit(st);
                        db.SubmitChanges();
                        //Console.WriteLine(st.FileName);

                    }
            }

          

        }
        catch (Exception ex)
        { }

    }

    

}
