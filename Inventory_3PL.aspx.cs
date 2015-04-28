using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Xml;
using System.Net;
using System.IO;
using System.Data;
using System.Xml.Linq;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Text.RegularExpressions;

public partial class Inventory_3PL : System.Web.UI.Page
{
    protected void Page_Load(object sender, EventArgs e)
    {
        if (!Page.IsPostBack)
        {
        }
    }
    protected void UploadFiles_old(object sender, EventArgs e)
    {

        Inventory3PL.UserLoginData L = new Inventory3PL.UserLoginData();
        L.Login = "CierantDev";
        L.Password = "CieDev2015";
        L.ThreePLID = 778;
        Inventory3PL.ServiceExternalSoapClient service3pl = new Inventory3PL.ServiceExternalSoapClient();
        var X = service3pl.ReportStockStatus(L);
    }
    protected void UploadFiles(object sender, EventArgs e)
    {
        var _url = "https://secure-wms.com/webserviceexternal/contracts.asmx";
        var _action = "http://www.JOI.com/schemas/ViaSub.WMS/ReportStockStatus";

        XmlDocument soapEnvelopeXml = CreateSoapEnvelope();
        HttpWebRequest webRequest = CreateWebRequest(_url, _action);

        InsertSoapEnvelopeIntoWebRequest(soapEnvelopeXml, webRequest);



        // begin async call to web request.
        IAsyncResult asyncResult = webRequest.BeginGetResponse(null, null);

        // suspend this thread until call is complete. You might want to
        // do something usefull here like update your UI.
        asyncResult.AsyncWaitHandle.WaitOne();

        // get the response from the completed web request.
        string soapResult;
        string myNamespace = "http://www.JOI.com/schemas/ViaSub.WMS/ReportStockStatus";


        XmlDocument xmlSoapRequest = new XmlDocument();
        string newsoap = "";
        using (WebResponse webResponse = webRequest.EndGetResponse(asyncResult))
        {
            using (StreamReader rd = new StreamReader(webResponse.GetResponseStream()))
            {
                soapResult = rd.ReadToEnd();

                //xmlSoapRequest.Load(rd);
            }
            //Console.Write(soapResult);


            newsoap = soapResult
                .Replace("<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\"><soap:Body><string xmlns=\"http://www.JOI.com/schemas/ViaSub.WMS/\">&lt;MyDataSet&gt;", "<MyDataSet>")
                .Replace("&lt;", "<").Replace("&gt;", ">").Replace("</MyDataSet></string></soap:Body></soap:Envelope>", "</MyDataSet>");

            Regex.Replace(newsoap,
            @"\s*(?<capture><(?<markUp>\w+)>.*<\/\k<markUp>>)\s*", "${capture}", RegexOptions.Singleline);
        }
        string fileDate = DateTime.Now.ToString("yyyy_MM_dd_HH_mm");
        string pName = @"C:\CierantProjects_dataLocal\3PL\Inv_" + fileDate + ".xml";
        if (File.Exists(pName))
            File.Delete(pName);
        System.IO.File.WriteAllText(pName, newsoap);

        XElement x = XElement.Load(pName);
        DataTable dt = XElementToDataTable(x);
        writeCSV(dt, @"C:\CierantProjects_dataLocal\3PL\Inv_" + fileDate + ".csv");
        Inventory_upd updateinv = new Inventory_upd();
        string result = updateinv.uploadtoSQL(dt);

        Button1.Attributes.Add("style", "color:green");
    }
    public void writeCSV(DataTable plData, string pName)
    {
        CreateCSV createcsv = new CreateCSV();
        if (File.Exists(pName))
            File.Delete(pName);
        var fieldnames = new List<string>();
        for (int index = 0; index < plData.Columns.Count; index++)
        {
            fieldnames.Add(plData.Columns[index].ColumnName);
        }
        bool resp = createcsv.addRecordsCSV(pName, fieldnames);
        foreach (DataRow row in plData.Rows)
        {

            var rowData = new List<string>();
            for (int index = 0; index < plData.Columns.Count; index++)
            {
                rowData.Add(row[index].ToString());
            }
            resp = false;
            resp = createcsv.addRecordsCSV(pName, rowData);

        }
    }
    public DataTable XElementToDataTable(XElement x)
    {
        DataTable dt = new DataTable();

        XElement setup = (from p in x.Descendants() select p).First();
        foreach (XElement xe in setup.Descendants()) // build your DataTable
            dt.Columns.Add(new DataColumn(xe.Name.ToString(), typeof(string))); // add columns to your dt

        var all = from p in x.Descendants(setup.Name.ToString()) select p;
        foreach (XElement xe in all)
        {
            DataRow dr = dt.NewRow();
            foreach (XElement xe2 in xe.Descendants())
                try
                {
                    dr[xe2.Name.ToString()] = xe2.Value; //add in the values
                }
                catch (Exception ex)
                { };
            dt.Rows.Add(dr);
        }
        return dt;
    }

    //public DataSet ConvertXMLToDataSet(string xmlData)
    //{
    //    StringReader stream = null;
    //    XmlTextReader reader = null;
    //    try
    //    {
    //        DataSet xmlDS = new DataSet();
    //        stream = new StringReader(xmlData);
    //        // Load the XmlTextReader from the stream
    //        //reader = new XmlTextReader(stream);
    //        //xmlDS.ReadXml(reader);
    //        //return xmlDS;
    //    }
    //    catch
    //    {
    //        return null;
    //    }
    //    finally
    //    {
    //        if (reader != null) reader.Close();
    //    }
    //}// Use this function to get XML string from a dataset


    private static HttpWebRequest CreateWebRequest(string url, string action)
    {
        HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(url);
        webRequest.Headers.Add("SOAPAction", action);
        webRequest.ContentType = "text/xml;charset=\"utf-8\"";
        webRequest.Accept = "text/xml";
        webRequest.Method = "POST";
        return webRequest;
    }

    private static XmlDocument CreateSoapEnvelope()
    {
        XmlDocument soapEnvelop = new XmlDocument();
        soapEnvelop.LoadXml(@"<?xml version=""1.0"" encoding=""utf-8""?>
      <soap:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" 
            xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" 
            xmlns:soap=""http://schemas.xmlsoap.org/soap/envelope/"">
            <soap:Body>
                <userLoginData xmlns=""http://www.JOI.com/schemas/ViaSub.WMS/"">
                <ThreePLID>778</ThreePLID>
                <Login>CierantDev</Login>
                <Password>CieDev2015</Password>
                </userLoginData>
            </soap:Body>
     </soap:Envelope>");
        return soapEnvelop;
    }

    private static void InsertSoapEnvelopeIntoWebRequest(XmlDocument soapEnvelopeXml, HttpWebRequest webRequest)
    {
        using (Stream stream = webRequest.GetRequestStream())
        {
            soapEnvelopeXml.Save(stream);
        }
    }
}