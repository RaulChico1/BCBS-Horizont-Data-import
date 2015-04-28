using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.IO;
using System.Configuration;
using System.Drawing;
using System.IO.Compression;
using System.Data;
using iTextSharp.text.pdf;

public partial class NL_UploadFiles : System.Web.UI.Page
{
    string originalDataPath = @"\\Cierant-taper\clients\Horizon BCBS\NoticeLetters\2014_DEC";
    string unzipPath = @"C:\CierantProjects_dataLocal\Horizont_unzip_NoticeLetters";
    string extractPath = "";
    string selectedNode = "";
    DataTable dataPDFs = pdfs_in_Zip_Table();

    private static DataTable pdfs_in_Zip_Table()
    {
        DataTable newt = new DataTable();
        newt.Clear();
        newt.Columns.Add("ZipName");
        newt.Columns.Add("PDFname");
        newt.Columns.Add("Pages");
        newt.Columns.Add("Date");
        return newt;
    }

    protected void Page_Load(object sender, EventArgs e)
    {
        if (!Page.IsPostBack)
        {


            LabelLocation.InnerText = originalDataPath;

            List<ListItem> filesP = new List<ListItem>();
            List<ListItem> filesD = new List<ListItem>();
            string[] fileinRoot = Directory.GetDirectories(originalDataPath);

            foreach (string f in fileinRoot)
            {
                filesP.Add(new ListItem(f, Path.GetFileName(f)));
            }
            if (filesP.Count > 0)
            {
                CheckBoxListFilesP.DataSource = filesP;
                CheckBoxListFilesP.DataTextField = "Value";
                CheckBoxListFilesP.DataValueField = "Text";
                CheckBoxListFilesP.DataBind();
            }
        }
    }
    protected void getCounts(object sender, EventArgs e)
    {

        if (!Button2.Text.Contains("Get Counts only, 0") && Button2.Text != "Get counts only")
        {
            if (selectedNode != "")
                CountsUnzipped(selectedNode);
        }
    }
    public void CountsUnzipped(string InitialDirectory)
    {

        DirectoryInfo di = new DirectoryInfo(InitialDirectory);
        var directories = di.GetFiles("*.pdf", SearchOption.AllDirectories);

        foreach (FileInfo d in directories)
        {
            PdfReader pdfReader = new PdfReader(d.FullName);
            int numberOfPages = pdfReader.NumberOfPages;
            var row = dataPDFs.NewRow();
            row["ZipName"] = d.DirectoryName.Substring(d.DirectoryName.IndexOf("unzip\\") + 6, 10);
            row["PDFname"] = d.Name;
            row["Pages"] = numberOfPages;
            row["Date"] = DateTime.Now.ToString("yyyyMMdd_HH_mm_ss");
            dataPDFs.Rows.Add(row);
            pdfReader.Close();
        }
        if (dataPDFs != null)
        {
            if (dataPDFs.Rows.Count > 0)
            {
                var fieldnames = new List<string>();
                for (int index = 0; index < dataPDFs.Columns.Count; index++)
                {
                    fieldnames.Add(dataPDFs.Columns[index].ColumnName);
                }

                CreateCSV createcsv = new CreateCSV();
                string stringRecords = dataPDFs.Rows.Count.ToString();
                DateTime today = DateTime.Today;

                string filenameOut = InitialDirectory + "\\Revised_Care_Radius_" + today.ToString("d").Replace("/", "_") + ".csv";
                if (File.Exists(filenameOut))
                    File.Delete(filenameOut);
                bool resp = createcsv.addRecordsCSV(filenameOut, fieldnames);

                foreach (DataRow row in dataPDFs.Rows)
                {
                    var rowData = new List<string>();

                    for (int index = 0; index < dataPDFs.Columns.Count; index++)
                    {
                        rowData.Add(row[index].ToString());
                    }
                    resp = createcsv.addRecordsCSV(filenameOut, rowData);
                }
            }
        }
    }

    protected void ExtractFiles(object sender, EventArgs e)
    {
        foreach (ListItem lstItem in CheckBoxListFilesP.Items)
        {
            if (lstItem.Selected == true)
            {
                string[] extensions = { "zip" };

                string[] fileinRoot = Directory.GetFiles(lstItem.Value)
                    .Where(f => extensions.Contains(f.Split('.').Last().ToLower())).ToArray();
                string dirname = "";
                string zipName = "";
                foreach (string filePath in fileinRoot)
                {
                    try
                    {
                        zipName = filePath.Split('\\').Last().ToUpper();
                        dirname = zipName.Split('.').First().ToUpper();
                        extractPath = unzipPath + "\\" + lstItem.Text + "\\" + dirname;
                        if (!Directory.Exists(extractPath))
                            Directory.CreateDirectory(extractPath);

                        System.IO.DirectoryInfo unzippedFiles = new DirectoryInfo(extractPath);
                        foreach (FileInfo file in unzippedFiles.GetFiles())
                        {
                            file.Delete();
                        }
                        //string Location1 = zipFile.LastIndexOf("\\");
                        //string intname = zipFile.Substring(0, Location1);
                        //int intLength = intname.Length;

                        //string location2 = intname.LastIndexOf("\\");
                        //string strDirName = intname.Substring(location2 + 1, intLength - (location2 + 1));
                        System.IO.Compression.ZipFile.ExtractToDirectory(filePath, extractPath);
                        //System.IO.Compression.ZipFile.ExtractToDirectory(zipFile, extractPath + "\\" + strDirName);
                    }
                    catch (Exception ex)
                    {

                    }
                }


            }
        }
        Label3.BackColor = Color.LightGreen;

    }

    protected void selectAllP(object sender, EventArgs e)
    {
        if (All1.Text == "All")
        {
            foreach (ListItem li in CheckBoxListFilesP.Items)
            {
                li.Selected = true;
            };
            All1.Text = "all";
        }
        else
        {
            foreach (ListItem li in CheckBoxListFilesP.Items)
            {
                li.Selected = false;
            };
            All1.Text = "All";
        }
    }
    protected void CheckBoxListFilesP_SelectedIndexChanged(object sender, EventArgs e)
    {
        selectedNode = "";
        CheckBoxListFilesD.DataSource = "";
        CheckBoxListFilesD.DataBind();
        List<ListItem> filesD = new List<ListItem>();
        try
        {
            for (int chkcount = 0; chkcount < CheckBoxListFilesP.Items.Count; chkcount++)
            {
                if (CheckBoxListFilesP.Items[chkcount].Selected)
                //lblCheckBoxList.Text += ", " + chkList.Items[chkcount].Text; 
                {
                    string filesIN_Path = unzipPath + "\\" + CheckBoxListFilesP.Items[chkcount].Text.ToUpper();
                    selectedNode = filesIN_Path;
                    string[] fileDirs = Directory.GetFiles(filesIN_Path, "*.*",
                                        SearchOption.AllDirectories);


                    foreach (string filePath in fileDirs)
                    {
                        if (Path.GetFileNameWithoutExtension(filePath).IndexOf("SummaryReport") == -1 && Path.GetExtension(filePath).ToUpper() != ".XLSX")
                            filesD.Add(new ListItem(filePath, Path.GetFileName(filePath)));
                    }
                }
            }
            if (filesD.Count > 0)
            {
                CheckBoxListFilesD.DataSource = filesD;
                CheckBoxListFilesD.DataTextField = "Value";
                CheckBoxListFilesD.DataValueField = "Text";
                CheckBoxListFilesD.DataBind();
            }
            Button2.Text = "Get Counts only, " + filesD.Count + " files";

        }
        catch (Exception ex)
        { }
    }




}