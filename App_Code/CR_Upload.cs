using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Xml.Linq;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.IO;
/// <summary>
/// Summary description for CR_Upload
/// </summary>
/// 
[Table(Name = "HOR_Care_Radius_BatchDataXML")]
public class DCdata
{
    [Column(DbType = "Int NOT NULL", IsPrimaryKey = true, IsDbGenerated = true)]
    public int RowID { get; set; }
    [Column]
    public string ImportDate { get; set; }
    [Column]
    public string Cycle { get; set; }
    [Column]
    public string OriginalFileName { get; set; }
    [Column]
    public string batchId { get; set; }
    [Column]
    public string batchTransactionId { get; set; }
    [Column]
    public string dlgUId { get; set; }
    [Column]
    public string letterTemplateId { get; set; }
    [Column]
    public string documentDate { get; set; }
    [Column]
    public string faxNumber { get; set; }
    [Column]
    public string coverPageName { get; set; }
    [Column]
    public string coverPageAddress1 { get; set; }
    [Column]
    public string coverPageAddress2 { get; set; }
    [Column]
    public string coverPageAddress3 { get; set; }
    [Column]
    public string coverPageAddress4 { get; set; }
    [Column]
    public string coverPageCity { get; set; }
    [Column]
    public string coverPageState { get; set; }
    [Column]
    public string coverPageZIP { get; set; }
    [Column]
    public string fileName { get; set; }
    [Column]
    public string folderPath { get; set; }
    [Column]
    public string fallBackToPrintOnFaxFailure { get; set; }
    [Column]
    public string IncludeBRE { get; set; }

}
class CR_Database : DataContext
{

    private const String LoginString = @"Server=BusinessSQL\sqlserver2008R2;User ID=BCBS_AuditUser;Password=weffAmFoS;Database=BCBS_Horizon";
    public Table<DCdata> HOR_Care_Radius_BatchDataXML;
    public CR_Database()
        : base(LoginString)
    {
    }
}

public class CR_Upload
{
    public string CR_UploadXML(string fileName)
    {
        int errors = 0;
        string results = "ok";
        string newCycle = "00";
        CR_Database db = new CR_Database();
        try
        {
            var query = (from t in db.HOR_Care_Radius_BatchDataXML
                         where t.ImportDate == DateTime.Now.ToString("yyyy-MM-dd")
                         select new { t.Cycle }).AsEnumerable();


            if (query != null)
            {
                var nResult = (from p in query
                               select p.Cycle).Max();
                if (nResult != null)
                    newCycle = (Convert.ToInt16(nResult) + 1).ToString("00");
            }
        }
        catch (Exception ex)
        { }
        try
        {
            DataSet ds = new DataSet();
            //ds.ReadXml(fileName);
            //string myXMLfile = @"C:\MySchema.xml";
            string fustFName = Path.GetFileName(fileName);
            XElement XTemp = XElement.Load(fileName);
            var queryCDATAXML = from element in XTemp.DescendantNodes()
                                where element.NodeType == System.Xml.XmlNodeType.CDATA
                                select element.Parent.Value.Trim();

            XDocument xdoc = XDocument.Load(fileName);

            List<DCdata> clientdata = (from cntry in xdoc.Element("BatchRequests").Elements("index")

                                       select new DCdata
                                       {
                                           batchId = cntry.Element("batchId").Value,
                                           ImportDate = DateTime.Now.ToString("yyyy-MM-dd"),
                                           Cycle = newCycle,
                                           OriginalFileName = fustFName,
                                           batchTransactionId = cntry.Element("batchTransactionId").Value,
                                           dlgUId = cntry.Element("dlgUId").Value,
                                           letterTemplateId = cntry.Element("letterTemplateId").Value,
                                           documentDate = cntry.Element("documentDate").Value,
                                           faxNumber = cntry.Element("fax").Element("faxNumber").Value,
                                           coverPageName = cntry.Element("fax").Element("coverPageName").Value,
                                           coverPageAddress1 = cntry.Element("fax").Element("coverPageAddress1").Value,
                                           coverPageAddress2 = cntry.Element("fax").Element("coverPageAddress2").Value,
                                           coverPageAddress3 = cntry.Element("fax").Element("coverPageAddress3").Value,
                                           coverPageAddress4 = cntry.Element("fax").Element("coverPageAddress4").Value,
                                           coverPageCity = cntry.Element("fax").Element("coverPageCity").Value,
                                           coverPageState = cntry.Element("fax").Element("coverPageState").Value,
                                           coverPageZIP = cntry.Element("fax").Element("coverPageZIP").Value,
                                           fileName = cntry.Element("archive").Element("fileName").Value,
                                           folderPath = cntry.Element("archive").Element("folderPath").Value,
                                           fallBackToPrintOnFaxFailure = cntry.Element("archive").Element("fallBackToPrintOnFaxFailure").Value,
                                           IncludeBRE = cntry.Element("archive").Element("IncludeBRE").Value
                                       }).ToList();

            foreach (var co in clientdata)
            {
                //Console.WriteLine(co.batchTransactionId);
                db.HOR_Care_Radius_BatchDataXML.InsertOnSubmit(co);
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
        return results;
    }
}