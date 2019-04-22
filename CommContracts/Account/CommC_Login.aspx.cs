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

namespace CommContracts.Account
{
    public partial class CommC_Login : System.Web.UI.Page
    {
        DBUtility dbU;

        protected void Page_Load(object sender, EventArgs e)
        {

        }
        protected void LoginUser_Authenticate(object sender, AuthenticateEventArgs e)
        {

           
            GlobalVar.dbaseName = "BCBS_MA";
            dbU = new DBUtility(cVars.connectionKey, DBUtility.ConnectionStringType.Configured);

            GlobalVar.isLocal = HttpContext.Current.Request.IsLocal;

            SqlParameter[] sqlParams = new SqlParameter[] { new SqlParameter("@UN", LoginUser.UserName), new SqlParameter("@PWD", LoginUser.Password) };

            if (Convert.ToInt32(dbU.ExecuteScalar("IsValidUser", sqlParams)).Equals(1))
            {
                Session["CCUser"] = LoginUser.UserName;

                SqlParameter[] sqlParams2 = new SqlParameter[] { new SqlParameter("@email", LoginUser.UserName) };

                //Session["WKUserRole"] = dbU.ExecuteScalar("WK_R_GetUserRoleByUserName", sqlParams2);
                Session["CCUserRole"] = dbU.ExecuteScalar("CommOne_GetUserRolesByEmail", sqlParams2);

                FormsAuthentication.SetAuthCookie(LoginUser.UserName, true);

                e.Authenticated = true;

                SqlParameter[] sqlParams3 = new SqlParameter[] { new SqlParameter("@UN", LoginUser.UserName) };

                int UserID = Convert.ToInt32(dbU.ExecuteScalar("Select UserID From Users WHERE UserName=@UN", sqlParams3));

                Session["CCUserID"] = UserID;

            }
            else
            {

            }

        }
    }
}