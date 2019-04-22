using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data;
using System.Data.SqlClient;
using System.Web.Security;
using System.Configuration;

namespace CommContracts
{
    public class BaseClass : System.Web.UI.Page
    {
        DBUtility dbU;
        public BaseClass()
        {


        }
        public bool checkLogin()
        {
            GlobalVar.isLocal = HttpContext.Current.Request.IsLocal;
            
            //return true;
            string uid = (this.Context.Request.QueryString["uid"] != "" 
                && this.Context.Request.QueryString["uid"] != null) ? this.Context.Request.QueryString["uid"] : "";

            if (uid == "")
            {
                if (HttpContext.Current != null)
                {

                    if (HttpContext.Current.Session["HzUserID"] != null)
                    {
                        uid = HttpContext.Current.Session["HzUserID"].ToString();
                    }
                }
            }

            if (uid == null || uid == "")
                return false;
            else
            {
                GlobalVar.UserUpd = HttpContext.Current.Session["HzUser"].ToString();
                GlobalVar.UserLoc = Environment.MachineName;
                return true;
            }
            
        }
        //override protected void OnInit(EventArgs e)
        //{
        //GlobalVar.dbaseName = "WoltersK";
        //dbU = new DBUtility(cVars.connectionKey, DBUtility.ConnectionStringType.Configured);


        //SqlParameter[] sqlParams = new SqlParameter[] { new SqlParameter("@UN", LoginUser.UserName), new SqlParameter("@PWD", LoginUser.Password) };

        //if (Convert.ToInt32(dbU.ExecuteScalar("WK_R_IsValidUser", sqlParams)).Equals(1))
        //{
        //    Session["WKUser"] = LoginUser.UserName;

        //    SqlParameter[] sqlParams2 = new SqlParameter[] { new SqlParameter("@UN", LoginUser.UserName) };

        //    Session["WKUserRole"] = dbU.ExecuteScalar("WK_R_GetUserRoleByUserName", sqlParams2);

        //    FormsAuthentication.SetAuthCookie(WKL LoginUser.UserName, true);

        //    e.Authenticated = true;

        //    SqlParameter[] sqlParams3 = new SqlParameter[] { new SqlParameter("@UN", LoginUser.UserName) };

        //    int UserID = Convert.ToInt32(dbU.ExecuteScalar("Select UserID From Users WHERE UserName=@UN", sqlParams3));

        //    Session["WKUserID"] = UserID;

        //}
        //else
        //{

        //}
        //}
    }
}