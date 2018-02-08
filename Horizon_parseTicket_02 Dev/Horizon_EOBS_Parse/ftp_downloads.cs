using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.IO;

namespace Horizon_EOBS_Parse
{
    public class ftp_downloads
    {

        public void checkFile()
        {
            SshConnectionInfo info = new SshConnectionInfo();
            info.User = "Horizon";
            info.Pass = "CyRyk1al";
            info.Host = "ftp://sftp.cierant.com";

            string ftpSubDir = "ftp.cierant.com/HorizonBCBS/IN/";
            string FTPFullFileName = info.Host + "/IN/HLGS_12062017.zip";

            string ftpFileSize = Convert.ToString(FileSize(FTPFullFileName, info.User, info.Pass));
            string DirLocal = ProcessVars.InputDirectory + "From_FTP";
            FileInfo fLocal = new FileInfo(DirLocal + "\\__HLGS_12062017.zip");
            if (fLocal.Exists)
            {
                string localFileSize = fLocal.Length.ToString();
            }


        }

        public static long FileSize(string FTPFullFileName, string UserName, string Password)
        {
            try
            {
                //sftp://Horizon@sftp.cierant.com/HorizonBCBS/IN/HLGS_12062017.zip
                FtpWebRequest FTP = (FtpWebRequest)FtpWebRequest.Create(new Uri(FTPFullFileName));
                FTP.Method = WebRequestMethods.Ftp.GetFileSize;
                FTP.UseBinary = true;
                FTP.Credentials = new NetworkCredential(UserName, Password);
                FtpWebResponse Response = (FtpWebResponse)FTP.GetResponse();
                Stream FtpStream = Response.GetResponseStream();
                long FileSize = Response.ContentLength;

                FtpStream.Close();
                Response.Close();
                return FileSize;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
            //https://www.codeproject.com/Questions/738766/How-to-detect-if-a-file-downloaded-by-FTP-is-compl

        }
    }
}
