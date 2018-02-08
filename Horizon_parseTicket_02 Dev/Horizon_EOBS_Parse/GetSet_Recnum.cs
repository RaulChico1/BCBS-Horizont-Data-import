using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.Data.SqlClient;

namespace Horizon_EOBS_Parse
{
    public class GetSet_Recnum
    {
        DBUtility dbU;
        public int Get_Recnum()
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            int GRecnum;
            var recnum = dbU.ExecuteScalar("select max(recnum) from HOR_parse_SEQ");

            if (recnum.ToString() == "")
                GRecnum = 1;
            else
                GRecnum = Convert.ToInt32(recnum.ToString()) + 1;

            return GRecnum;
        }
        public void Set_Recnum(int Recnum, string TableName)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            dbU.ExecuteScalar("Insert into HOR_parse_SEQ (Recnum, TableName, datetime) values(" + Recnum + ",'" + TableName + "', GETDATE())");

        }
        public string getNextCycle(string dataTable, string Importdate)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

            string GRecnum;
            var recnum = dbU.ExecuteScalar("select cycle from " + dataTable + " where ImportDate = '" + Importdate + "'");
            if (recnum == null)
                GRecnum = "00";
            else
            {
                if (recnum.ToString() == "" || recnum.ToString() == "0")
                    GRecnum = "00";
                else
                    GRecnum = (Convert.ToInt16(recnum) + 1).ToString("00");
            }
            return GRecnum;

        }
    }
}
