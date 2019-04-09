using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml;
using System.Xml.Linq;
using System.Data;

namespace Horizon_EOBS_Parse
{
    public static class CreateXML
    {
        
        public static string ToXml(this DataTable table, int metaIndex = 0 )
        {
            try
            {

                XDocument xdoc = new XDocument(
                    new XElement("sample",
                        from column in table.Columns.Cast<DataColumn>()
                        where column != table.Columns[metaIndex]
                        select new XElement(column.ColumnName,
                            from row in table.AsEnumerable()
                            select new XElement(row.Field<string>(metaIndex), row[column])
                            )
                        )
                    );

               

                return "";
            }
            catch (Exception ex)
            {
                string error = ex.Message;
                return error;
            }
            
        }
       
       
    }
}
