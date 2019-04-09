using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Data;
using System.Reflection;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using DocumentFormat.OpenXml;
using System.Globalization;
using System.Text.RegularExpressions;
using System.IO;

namespace Horizon_EOBS_Parse
{
    public class Export_XLSX
    {
        public void CreateExcelFile(DataTable dataToTicket, string location, string ticketNO)
        {
            {
                int totalRecs = dataToTicket.Rows.Count;
                int xx = 0;
            anothernum:
                xx++;
                string docName = location + "Job Ticket_" + ticketNO + "_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd__") + xx + ".xls";
                string FileNAME_network = @"\\freenas\clients\Horizon BCBS\NoticeLetters\JobTickets\" + "Job Ticket_" + ticketNO + "_" + GlobalVar.DateofProcess.ToString("yyyy_MM_dd__") + xx + ".xlsx";
                if (File.Exists(FileNAME_network))
                    goto anothernum;
                if (File.Exists(docName))
                    File.Delete(docName);
                if (File.Exists(FileNAME_network))
                    File.Delete(FileNAME_network);
                // Create a Wordprocessing document. 
                using (SpreadsheetDocument spreadsheetDocument = SpreadsheetDocument.Create(docName, SpreadsheetDocumentType.Workbook))
                {
                    // Add a WorkbookPart to the document.
                    WorkbookPart workbookpart = spreadsheetDocument.AddWorkbookPart();
                    workbookpart.Workbook = new Workbook();

                    // Add a WorksheetPart to the WorkbookPart.
                    WorksheetPart worksheetPart = workbookpart.AddNewPart<WorksheetPart>();
                    worksheetPart.Worksheet = new Worksheet();

                    // ####################################
                    //   IMPORTANT STUFF
                    // ####################################

                    //string strText = "This is some really, really long text to display.";
                    //double width = GetWidth("Calibri", 11, strText);

                    //string strText2 = "123";
                    //double width2 = GetWidth("Calibri", 11, strText2);

                    Columns columns = new Columns();
                    //columns.Append(CreateColumnData(2, 2, width));
                    //columns.Append(CreateColumnData(3, 3, width2));

                    columns.Append(CreateColumnData(1, 1, 19));
                    columns.Append(CreateColumnData(2, 2, 11));
                    columns.Append(CreateColumnData(3, 3, 11));
                    columns.Append(CreateColumnData(4, 4, 45));
                    columns.Append(CreateColumnData(5, 5, 15));
                    columns.Append(CreateColumnData(6, 6, 15));
                    worksheetPart.Worksheet.Append(columns);

                    // ####################################
                    //   END OF IMPORTANT STUFF
                    // ####################################

                    SheetData sd = new SheetData();
                    worksheetPart.Worksheet.Append(sd);

                    // Add Sheets to the Workbook.
                    Sheets sheets = spreadsheetDocument.WorkbookPart.Workbook.AppendChild<Sheets>(new Sheets());


                    // Append a new worksheet and associate it with the workbook.
                    Sheet sheet = new Sheet()
                    {
                        Id = spreadsheetDocument.WorkbookPart.GetIdOfPart(worksheetPart),
                        SheetId = 1,
                        Name = "Ticket_" + ticketNO,
                    };
                    sheets.Append(sheet);



                    // Add Data
                    Row row = new Row();

                    Cell cell;
                    string cellVal, cellLoc;

                    cellVal = "Customer"; cellLoc = "A1";
                    SetCell(worksheetPart.Worksheet, cellVal, cellLoc);

                    cellVal = "Horizon Blue Cross Blue Shield of NJ"; cellLoc = "B1";
                    SetCell(worksheetPart.Worksheet, cellVal, cellLoc);

                    cellVal = "Process Date:"; cellLoc = "A2";
                    SetCell(worksheetPart.Worksheet,  cellVal, cellLoc);

                    cellVal = GlobalVar.DateofProcess.ToString("MM/dd/yyyy"); cellLoc = "B2";
                    SetCell(worksheetPart.Worksheet,  cellVal, cellLoc);

                    cellVal = "Process Time:"; cellLoc = "A3";
                    SetCell(worksheetPart.Worksheet,  cellVal, cellLoc);

                    cellVal = DateTime.Now.ToString("HH:mm:ss"); cellLoc = "B3";
                    SetCell(worksheetPart.Worksheet,  cellVal, cellLoc);

                    cellVal = "Total Files Processed:"; cellLoc = "A5";
                    SetCell(worksheetPart.Worksheet,  cellVal, cellLoc);

                    cellVal = totalRecs.ToString(); cellLoc = "B5";
                    SetCellN(worksheetPart.Worksheet, cellVal, cellLoc);

                    cellVal = "Files Destination:"; cellLoc = "A6";
                    SetCell(worksheetPart.Worksheet,  cellVal, cellLoc);

                    int xRow = 8;
                    int bkColumn = 0;
                    char[] alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".ToCharArray();
                    for (int index = 0; index < dataToTicket.Columns.Count; index++)
                    {
                        cellVal = dataToTicket.Columns[index].ColumnName;
                        cellLoc = alpha[index].ToString() + xRow;
                        SetCell(worksheetPart.Worksheet, cellVal, cellLoc);
                        bkColumn = index;
                    }


                    xRow = 9;
                    foreach (DataRow rowd in dataToTicket.Rows)
                    {
                        for (int index = 0; index < dataToTicket.Columns.Count; index++)
                        {

                            if (index == 0)
                            {
                                //document.Cell(xRow, index).Value = row[index].ToString().Substring(0, 10);
                                cellVal = rowd[index].ToString().Substring(0, 10);
                                cellLoc = alpha[index].ToString() + xRow;
                                SetCell(worksheetPart.Worksheet, cellVal, cellLoc);

                            }
                            else if (index == 4 || index == 5)
                            {
                                if (ticketNO == "01_EPBs" && index == 4)
                                {
                                    cellVal = rowd[index].ToString();
                                    cellLoc = alpha[index].ToString() + xRow;
                                    SetCell(worksheetPart.Worksheet, cellVal, cellLoc);
                                }
                                else
                                {
                                    //document.Cell(xRow, index).Value = Convert.ToUInt32(row[index].ToString());
                                    cellVal = rowd[index].ToString();
                                    cellLoc = alpha[index].ToString() + xRow;
                                    SetCellN(worksheetPart.Worksheet, cellVal, cellLoc);
                                }
                            }
                            else
                            {
                                //document.Cell(xRow, index).Value = row[index].ToString();
                                cellVal = rowd[index].ToString();
                                cellLoc = alpha[index].ToString() + xRow;
                                SetCell(worksheetPart.Worksheet, cellVal, cellLoc);
                            }
                        }
                        xRow++;
                    }





                    //// String
                    //cell = CreateSpreadsheetCellIfNotExist(worksheetPart.Worksheet, "B2");
                    //cell.CellValue = new CellValue(strText);
                    //cell.DataType = CellValues.String;


                    //// Number
                    //int count = 123;
                    //cell = CreateSpreadsheetCellIfNotExist(worksheetPart.Worksheet, "C2");
                    //CellValue cellValue = new CellValue(count.ToString());
                    //cell.CellValue = cellValue;
                    //cell.DataType = CellValues.Number;

                    workbookpart.Workbook.Save();

                    // Close the document.
                    spreadsheetDocument.Close();

                    File.Copy(docName, FileNAME_network, true);

                    //if (ticketNO == "01_EPBs" || ticketNO == "01_Test")
                    //{
                    //    SendMails sendmail = new SendMails();
                    //    sendmail.SendMail("Horizon BCBS Daily Ticket EPB " + ticketNO + " Ready  EOM", "rchico@apps.cierant.com,cgaytan@apps.cierant.com",
                    //        //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                    //                                "noreply@apps.cierant.com", "\n\n" +
                    //                                 "Ticket " + ticketNO + " ready");  //tkrompinger@apps.cierant.com
                    //}
                    //else
                    //{
                    //    SendMails sendmail = new SendMails();
                    //    sendmail.SendMail("Horizon BCBS Daily Ticket " + ticketNO + " Ready  EOM", "rchico@apps.cierant.com,suz@apps.cierant.com,2038086157@tmomail.net",
                    //        //sendmail.SendMail("Pre Sales Kits Upload", "rchico@apps.cierant.com",
                    //                                "noreply@apps.cierant.com", "\n\n" +
                    //                                 "Ticket " + ticketNO + " ready");  //tkrompinger@apps.cierant.com

                    //}
                }
            }
        }
        public Cell SetCell(Worksheet worksheet,  string cellVal, string cellLoc)
        {
            Cell cell;
            cell = CreateSpreadsheetCellIfNotExist(worksheet.WorksheetPart.Worksheet, cellLoc);
            cell.CellValue = new CellValue(cellVal);
            cell.DataType = CellValues.String;
            
            return cell;
        }
        public Cell SetCellN(Worksheet worksheet, string cellVal, string cellLoc)
        {
            Cell cell;
            cell = CreateSpreadsheetCellIfNotExist(worksheet.WorksheetPart.Worksheet, cellLoc);
            cell.CellValue = new CellValue(cellVal);
            cell.DataType = CellValues.Number;
            return cell;
        }
        private static double GetWidth(string font, int fontSize, string text)
        {
            System.Drawing.Font stringFont = new System.Drawing.Font(font, fontSize);
            return GetWidth(stringFont, text);
        }

        private static double GetWidth(System.Drawing.Font stringFont, string text)
        {
            // This formula is based on this article plus a nudge ( + 0.2M )
            // http://msdn.microsoft.com/en-us/library/documentformat.openxml.spreadsheet.column.width.aspx
            // Truncate(((256 * Solve_For_This + Truncate(128 / 7)) / 256) * 7) = DeterminePixelsOfString

            //Size textSize = TextRenderer.MeasureText(text, stringFont);
            //double width = (double)(((textSize.Width / (double)7) * 256) - (128 / 7)) / 256;
            //width = (double)decimal.Round((decimal)width + 0.2M, 2);

            //return width;
            return 100;
        }

        private static Column CreateColumnData(UInt32 StartColumnIndex, UInt32 EndColumnIndex, double ColumnWidth)
        {
            Column column;
            column = new Column();
            column.Min = StartColumnIndex;
            column.Max = EndColumnIndex;
            column.Width = ColumnWidth;
            column.CustomWidth = true;
            return column;
        }

        // Given a Worksheet and a cell name, verifies that the specified cell exists.
        // If it does not exist, creates a new cell. 
        private static Cell CreateSpreadsheetCellIfNotExist(Worksheet worksheet, string cellName)
        {
            string columnName = GetColumnName(cellName);
            uint rowIndex = GetRowIndex(cellName);

            IEnumerable<Row> rows = worksheet.Descendants<Row>().Where(r => r.RowIndex.Value == rowIndex);
            Cell cell;

            // If the Worksheet does not contain the specified row, create the specified row.
            // Create the specified cell in that row, and insert the row into the Worksheet.
            if (rows.Count() == 0)
            {
                Row row = new Row() { RowIndex = new UInt32Value(rowIndex) };
                cell = new Cell() { CellReference = new StringValue(cellName) };
                row.Append(cell);
                worksheet.Descendants<SheetData>().First().Append(row);
                worksheet.Save();
            }
            else
            {
                Row row = rows.First();

                IEnumerable<Cell> cells = row.Elements<Cell>().Where(c => c.CellReference.Value == cellName);

                // If the row does not contain the specified cell, create the specified cell.
                if (cells.Count() == 0)
                {
                    cell = new Cell() { CellReference = new StringValue(cellName) };
                    row.Append(cell);
                    worksheet.Save();
                }
                else
                    cell = cells.First();
            }

            return cell;
        }

        // Given a cell name, parses the specified cell to get the column name.
        private static string GetColumnName(string cellName)
        {
            // Create a regular expression to match the column name portion of the cell name.
            Regex regex = new Regex("[A-Za-z]+");
            Match match = regex.Match(cellName);

            return match.Value;
        }

        // Given a cell name, parses the specified cell to get the row index.
        private static uint GetRowIndex(string cellName)
        {
            // Create a regular expression to match the row index portion the cell name.
            Regex regex = new Regex(@"\d+");
            Match match = regex.Match(cellName);

            return uint.Parse(match.Value);
        }


    }
}