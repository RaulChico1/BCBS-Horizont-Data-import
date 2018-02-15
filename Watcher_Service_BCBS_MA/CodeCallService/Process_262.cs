using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CodeCallService
{
    public class Process_262
    {
        CodeCallService.DBUtility dbU;
        string directoryName = @"C:\CierantProjects_dataLocal\Horizon_Parse\DailyFiles\BCBS_MA\Blue";
        public string processKits262()
        {
            CodeCallService.GlobalVar.dbaseName = "BCBS_MA";
            dbU = new CodeCallService.DBUtility(CodeCallService.GlobalVar.connectionKey, CodeCallService.DBUtility.ConnectionStringType.Configured);


            string result = "";
            string strsql = "Update BCBS_MA_parse_262 set Search_EOC = d.filename + '~' + CONVERT(varchar(15), D.Recnum) " +
                             "FROM[BCBS_MA_parse_262] R left join BCBS_MA_parse_eoc D on d.sin = r.sin + '0000'";

            // select* from BCBS_MA_parse_262

            dbU.ExecuteScalar(strsql);




            return result;
        }
    }
}
