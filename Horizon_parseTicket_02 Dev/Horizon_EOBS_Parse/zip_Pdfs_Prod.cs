using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Configuration;
using WinSCP;
using System.IO;
using System.IO.Compression;
using System.Net;


namespace Horizon_EOBS_Parse
{
    //public class fFile
    //{
    //    public string Name { get; set; }
    //    public string Ext { get; set; }
    //}
    public class zip_Pdfs_Prod
    {
        DBUtility dbU;
        string workingDir = ProcessVars.InputDirectory + @"from_FTP\FilesToZip\";
        

        DataTable filesToZip = null;
        public string select_to_zip()
        {
            string result = "";
            appSets appsets = new appSets();
            downloadpdfs();

            return result;
        }
        public string downloadpdfs()
        {
            string result = "";
            Directory.CreateDirectory(workingDir);

            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            DataTable sources = dbU.ExecuteDataTable("Select distinct sourcedir as sourceDir from HOR_parse_Category_Master_specialProcess where code = 'Zip_Only' order by sourcedir");
            if (sources.Rows.Count > 0)
            {
                foreach (DataRow rowS in sources.Rows)
                {
                    var fileToImport = getFilesImport("pdf", rowS["sourceDir"].ToString());
                    string resultd = downloadFiles(fileToImport, rowS["sourceDir"].ToString());
                }
            }
            return result;
        }
        public IEnumerable<fFile> getFilesImport(String option, string sourceDir)
        {
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            filesToZip = dbU.ExecuteDataTable("Select * from HOR_parse_Category_Master_specialProcess where code = 'Zip_Only' and SourceDir = '" + sourceDir + "' order by filename");
            
            IEnumerable<fFile> iFiles =
                    from fToZip in filesToZip.AsEnumerable()
                    select new fFile()
                    {
                        Name = fToZip.Field<String>("Filename"),
                        Ext = option
                    };
            return iFiles;
        }

        public string downloadFiles(IEnumerable<fFile> ffileszip, string sourceDir)
        {
            string result = "";
            string HOST = sourceDir.Substring(0, sourceDir.IndexOf("/") - 1);
            GlobalVar.dbaseName = "BCBS_Horizon";
            dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);
            string error = "";
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "Horizon";
            info.Pass = "CyRyk1al";
            info.Host = HOST;  //"ftp://sftp.cierant.com";
            int steps = 0;
            //string tarFiles = "filesautomaticallyconverted";
            string ftpSubDir = sourceDir;//"sftp.cierant.com/HorizonBCBS/IN/";
            FtpWebRequest request = (FtpWebRequest)FtpWebRequest.Create(new Uri(info.Host + "/IN"));
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            request.Credentials = new NetworkCredential(info.User, info.Pass);
            string listFiles = "";
            int countFiles = 0;
            try
            {

            }
            catch(Exception ex)
            {}
            return result;

        }
    }
}
