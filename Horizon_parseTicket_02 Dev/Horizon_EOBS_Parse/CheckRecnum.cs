using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Horizon_EOBS_Parse
{
    public class CheckRecnum
    {
        DBUtility dbU;

        public bool canProcess(string procName)
        {
            bool result = false;
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);



            return result;

        }
    }
}
