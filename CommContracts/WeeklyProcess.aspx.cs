using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace CommContracts
{
    public partial class WeeklyProcess : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!Page.IsPostBack)
            {
                string result = listFiles();
                if (result != "")
                    LabelM.InnerHtml = result;
            }
        }
        protected void UploadFiles(object sender, EventArgs e)
        {

        }
        protected string listFiles()
        {
            string result = "";
            string OutputStatus = "";
            try
            {


            }
            catch (Exception ex)
            {
                result = ex.Message;

            }
            return result;
        }
    }
}