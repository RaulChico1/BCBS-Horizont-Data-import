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
    [Table(Name = "BatchDataXML_new")]

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

        public IList<PDFs> pdfs = new List<PDFs>();
    }

    [Table(Name = "BatchDataDetailXML_new")]
      public class PDFs
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
    class MyDatabase : DataContext
    {

        private const String LoginString = @"Server=BusinessSQL\sqlserver2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
        public Table<ClientData> BatchDataXML_new;
        public Table<PDFs> BatchDataDetailXML_new;
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
            XDocument xdoc = XDocument.Load("C:/BCBS_Horizon_Data/MultiFlight_SABKLCNT_14197_095427_copy.xml");

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
                                               //flight
                                               //FlightNumber = cntry.Element("MailPiece").Element("Flight").Element("FlightNumber").Value,
                                               //Bind = cntry.Element("MailPiece").Element("Flight").Element("Bind").Value,
                                               //InsertCode1 = cntry.Element("MailPiece").Element("Flight").Element("InsertCode1").Value,

                                               //FileName = cntry.Element("MailPiece").Element("Flight").Element("Files").Element("FileName").Value,
                                               //SequenceOrder = cntry.Element("MailPiece").Element("Flight").Element("Files").Element("SequenceOrder").Value,

                                               NumberOfCopy = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("NumberOfCopy").Value,
                                               NameLine1 = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("NameLine1").Value,
                                               NameLine2 = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("NameLine2").Value,
                                               AddressLine1 = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("AddressLine1").Value,
                                               AddressLine2 = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("AddressLine2").Value,
                                               city = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("city").Value,
                                               state = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("State").Value,
                                               zip = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("Zip").Value,
                                               GroupNo = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("GroupNo").Value,
                                               FullName = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("FullName").Value,
                                               SubscriberID = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("SubscriberID").Value,
                                               EffectiveDate = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("EffectiveDate").Value,
                                               ContractType = cntry.Element("MailPiece").Element("MailingInfo").Element("RecipientInfo").Element("ContractType").Value,

                                               pdfs = (from ste in cntry.Element("MailPiece").Elements("Flight")


                                                       select new PDFs

                                                       {
                                                           FlightNumber = (string)ste.Element("FlightNumber"),
                                                           Bind = (string)ste.Element("Bind"),
                                                           InsertCode1 = (string)ste.Element("InsertCode1"),
                                                           FileName = (string)ste.Element("Files").Element("Filename"),
                                                           SequenceOrder = (string)ste.Element("Files").Element("SequenceOrder")

                                                       }).ToList()

                                           }).ToList();
            MyDatabase db = new MyDatabase();
 

            foreach (var co in clientdata)
            {

                Console.WriteLine(co.ClientTransactionID);
              
                   

                    db.BatchDataXML_new.InsertOnSubmit(co);
                    db.SubmitChanges();

                

                foreach (var st in co.pdfs)
                {
                    st.ClientTransactionID = co.ClientTransactionID;
                    db.BatchDataDetailXML_new.InsertOnSubmit(st);
                    db.SubmitChanges();
                    Console.WriteLine(st.FileName);

                }
            }

          

        }
        catch (Exception ex)
        { }

    }

    

}
