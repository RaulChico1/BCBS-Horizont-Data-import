using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Reflection;
using System.IO;
using System.Data;
using Excel = Microsoft.Office.Interop.Excel;
using System.Runtime.InteropServices;

namespace Horizon_EOBS_Parse
{
    public class ClassExcel
    {


        public void createExcelFile(DataSet ds,string filepath)
        {
            
            try
           
            {
            Microsoft.Office.Interop.Excel.Application ExcelObj = new Microsoft.Office.Interop.Excel.Application();
            Excel.Application xlApp;
            Excel.Workbook xlWorkBook;
            Excel.Worksheet xlWorkSheet;
            

        
            object misValue = System.Reflection.Missing.Value;
            xlApp = new Excel.Application();
           
            xlWorkBook = xlApp.Workbooks.Add(misValue);
           
            foreach (DataTable table in ds.Tables)
            {
                
                xlWorkSheet = xlWorkBook.Sheets.Add();
               // xlWorkSheet.Name = table.TableName;
                xlWorkSheet.Name = "Non-Deliverable";
                for (int i = 1; i < table.Columns.Count + 1; i++)
                {
                    xlWorkSheet.Cells[1, i] = table.Columns[i - 1].ColumnName;
                }

                for (int j = 0; j < table.Rows.Count; j++)
                {
                    for (int k = 0; k < table.Columns.Count; k++)
                    {
                        xlWorkSheet.Cells[j + 2, k + 1] = table.Rows[j].ItemArray[k].ToString();
                    }
                }
            }
        xlWorkBook.SaveAs(filepath, Excel.XlFileFormat.xlWorkbookDefault, misValue, misValue, false, false, Excel.XlSaveAsAccessMode.xlNoChange, misValue, misValue, misValue, misValue, misValue);
           


            xlWorkBook.Close();
            xlApp.Quit();
            releaseObject(xlWorkBook);
            releaseObject(xlApp);
        }
            catch(Exception ex)
            {
              
            }
          
          

            
        }

        private void releaseObject(object obj)
        {
            try
            {
                System.Runtime.InteropServices.Marshal.ReleaseComObject(obj);
                obj = null;
            }
            catch (Exception ex)
            {
                obj = null;
             
            }
            finally
            {
                GC.Collect();
            }
        }

           
        }
      
    }
