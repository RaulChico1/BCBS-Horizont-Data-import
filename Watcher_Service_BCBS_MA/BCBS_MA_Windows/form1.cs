using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;


namespace BCBS_MA_Windows
{
    public partial class Form1 : Form
    {
        
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            CodeCallService.appSets appsets = new CodeCallService.appSets();
            appsets.setVars();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            CodeCallService.UploadMasters uploadm = new CodeCallService.UploadMasters();
            label1.Text = uploadm.uploadMasters();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            CodeCallService.UploadEOC upload = new CodeCallService.UploadEOC();
            label1.Text = upload.uploadData();
        }

        private void button3_Click(object sender, EventArgs e)
        {
            CodeCallService.CreateKits newkits = new CodeCallService.CreateKits();
            string result = newkits.generalProess_Kits();
        }

        private void button4_Click(object sender, EventArgs e)
        {
            CodeCallService.UploadEOC upload = new CodeCallService.UploadEOC();
            label1.Text = upload.uploadDataAcct();
        }

        private void button5_Click(object sender, EventArgs e)
        {
            CodeCallService.CreateKits newkits = new CodeCallService.CreateKits();
            newkits.CleanDirsFor_Kits();
        }

        private void button6_Click(object sender, EventArgs e)
        {
            CodeCallService.Zipping zipper = new CodeCallService.Zipping();
            zipper.zipECO();
        }

        private void button7_Click(object sender, EventArgs e)
        {
            CodeCallService.CreateKitsAcct createK = new CodeCallService.CreateKitsAcct();
            label1.Text = createK.generalProess_Kits();
        }

        private void button8_Click(object sender, EventArgs e)
        {
            CodeCallService.CreateKitsAcct newkits = new CodeCallService.CreateKitsAcct();
            newkits.CleanDirsFor_Kits();
        }

        private void button9_Click(object sender, EventArgs e)
        {
            CodeCallService.Zipping zipper = new CodeCallService.Zipping();
            zipper.zipECOAcct();
        }

        private void button10_Click(object sender, EventArgs e)
        {
            CodeCallService.CreateKits multidoc = new CodeCallService.CreateKits();
            label1.Text = multidoc.MultiDocProcess_Kits();
        }

        private void button11_Click(object sender, EventArgs e)
        {
            string input = Microsoft.VisualBasic.Interaction.InputBox("Enter FileName", "to re print", "", -1, -1);

            CodeCallService.UploadEOC upload = new CodeCallService.UploadEOC();
            label1.Text = upload.uploadData_Reprint(input);
        }

        private void button13_Click(object sender, EventArgs e)
        {


            string input = Microsoft.VisualBasic.Interaction.InputBox("Enter XLSX Name", "Re print", "", -1, -1);

            CodeCallService.UploadEOC upload = new CodeCallService.UploadEOC();
            label1.Text = upload.uploadData_AcctReprint(input);
        }

        private void button14_Click(object sender, EventArgs e)
        {
            CodeCallService.Process_262 process = new CodeCallService.Process_262();
            process.processKits262();
        }

        private void button15_Click(object sender, EventArgs e)
        {
            string input = Microsoft.VisualBasic.Interaction.InputBox("Enter XLSX Name", "Re process", "", -1, -1);
            CodeCallService.UploadEOC upload = new CodeCallService.UploadEOC();
            label1.Text = upload.Reprocess_EOC(input);
        }

        private void button16_Click(object sender, EventArgs e)
        {
            string input = Microsoft.VisualBasic.Interaction.InputBox("Enter FileName", "to re print", "", -1, -1);

            CodeCallService.UploadEOC upload = new CodeCallService.UploadEOC();
            label1.Text = upload.uploadData_ReprintBatch(input);
        }

        private void button17_Click(object sender, EventArgs e)
        {
            CodeCallService.ReportsBCBS reports = new CodeCallService.ReportsBCBS();
            reports.pritnSummary();
        }
    }
}
