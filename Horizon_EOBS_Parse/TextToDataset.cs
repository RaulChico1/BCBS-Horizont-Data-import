using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;

namespace Horizon_EOBS_Parse
{
    public class TextToDataSet
    {

        public TextToDataSet()
        { }

        /// <summary>
        /// Converts a given delimited file into a dataset. 
        /// Assumes that the first line    
        /// of the text file contains the column names.
        /// </summary>
        /// <param name="File">The name of the file to open</param>    
        /// <param name="TableName">The name of the 
        /// Table to be made within the DataSet returned</param>
        /// <param name="delimiter">The string to delimit by</param>
        /// <returns></returns>  
        public static DataSet Convert(string File,
         string TableName, string delimiter)
        {

            int errors = 0;
            //The DataSet to Return
            DataSet result = new DataSet();

            //Open the file in a stream reader.
            StreamReader s = new StreamReader(File, Encoding.Default, true);

            //Split the first line into the columns       
            string[] columns = s.ReadLine().Split(delimiter.ToCharArray());

            //Add the new DataTable to the RecordSet
            result.Tables.Add(TableName);

            //Cycle the colums, adding those that don't exist yet 
            //and sequencing the one that do.
            foreach (string col in columns)
            {
                bool added = false;
                string next = "";
                int i = 0;
                while (!added)
                {
                    //Build the column name and remove any unwanted characters.
                    string columnname = col + next;
                    columnname = columnname.Replace("#", "");
                    columnname = columnname.Replace("'", "");
                    columnname = columnname.Replace("&", "");

                    //See if the column already exists
                    if (!result.Tables[TableName].Columns.Contains(columnname))
                    {
                        //if it doesn't then we add it here and mark it as added
                        result.Tables[TableName].Columns.Add(columnname);
                        added = true;
                    }
                    else
                    {
                        //if it did exist then we increment the sequencer and try again.
                        i++;
                        next = "_" + i.ToString();
                    }
                }
            }
            
           
                //Read the rest of the data in the file.        
                string AllData = s.ReadToEnd();
               
         
           
            //Split off each row at the Carriage Return/Line Feed
            //Default line ending in most windows exports.  
            //You may have to edit this to match your particular file.
            //This will work for Excel, Access, etc. default exports.
            string[] rows = AllData.Split("\n".ToCharArray());
            string row="";
            //Now add each row to the DataSet        
            foreach (string r in rows)
            {
                try
                {
                    //Split the row at the delimiter.
                    string[] items = r.Split(delimiter.ToCharArray());


                    //Add the item
                    result.Tables[TableName].Rows.Add(items);

                    System.Diagnostics.Debug.WriteLine(r);
                }
                catch(Exception ex)
                {
                    errors = errors + 1;
                    row = r;
                    

                }



            }
            if (errors == 0)
            {  
                //Return the imported data.        
                return result;
            }
            else {

                SendMails sendmail = new SendMails();
                sendmail.SendMail("NJHId Cards " + DateTime.Now.ToString("yyyy-MM-dd"), "rchico@apps.cierant.com,tkarintholil@apps.cierant.com,sshrivastava@apps.cierant.com", "noreply@apps.cierant.com", "Njhid cards Failed -FileNot in correct Format at " + row + DateTime.Now.ToString("yyyy-mm-dd"));
                
                return null; 
            }
        }



        public static DataSet Convertwithoutheader(string File, string TableName, string delimiter, string[] colHeadings)
        {
            //The DataSet to Return
            DataSet result = new DataSet();

            //Open the file in a stream reader.
            StreamReader s = new StreamReader(File);

            //Add the new DataTable to the RecordSet
            result.Tables.Add(TableName);

            string[] columns;

            if (colHeadings == null)
            {

                //Split the first line into the columns
                columns = s.ReadLine().Split(delimiter.ToCharArray());
            }
            else
            {
                columns = colHeadings;
            }


            //Cycle the colums, adding those that don't exist yet
            //and sequencing the one that do.
            foreach (string col in columns)
            {
                bool added = false;
                string next = "";
                int i = 0;
                while (!added)
                {
                    //Build the column name and remove any unwanted characters.
                    string columnname = col + next;
                    columnname = columnname.Replace("#", "");
                    columnname = columnname.Replace("'", "");
                    columnname = columnname.Replace("&", "");

                    //See if the column already exists
                    if (!result.Tables[TableName].Columns.Contains(columnname))
                    {
                        //if it doesn't then we add it here and mark it as added
                        result.Tables[TableName].Columns.Add(columnname);
                        added = true;
                    }
                    else
                    {
                        //if it did exist then we increment the sequencer and try again.
                        i++;
                        next = "_" + i.ToString();
                    }
                }
            }

            s.ReadLine();
            //Read the rest of the data in the file.        
            string AllData = s.ReadToEnd();

            //Split off each row at the Carriage Return/Line Feed
            //Default line ending in most windows exports.  
            //You may have to edit this to match your particular file.
            //This will work for Excel, Access, etc. default exports.
           // string[] rows = AllData.Split("\r".ToCharArray());
            string[] rows = AllData.Split("\r\n".ToCharArray());
            //Now add each row to the DataSet        
            //foreach (string r in rows)
            //{
            //    //Split the row at the delimiter.
            //    string[] items = r.Split(delimiter.ToCharArray());

            //    //Add the item
            //    result.Tables[TableName].Rows.Add(items);
            //}

            foreach (string r in rows)
            {
                //Split the row at the delimiter.
                string[] items = r.Split(delimiter.ToCharArray());

                //Add the item
                result.Tables[TableName].Rows.Add(items);
            }





            //Return the imported data.        
            return result;





        }









    }
}
