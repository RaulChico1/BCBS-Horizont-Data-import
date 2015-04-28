using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;

/// <summary>
/// Summary description for Inventory_upd
/// </summary>
public class Inventory_upd
{
    DBUtility dbU;

    public string uploadtoSQL(DataTable csvData)
    {
        GlobalVar.dbaseName = "BCBS_Horizon";
        dbU = new DBUtility(GlobalVar.connectionKey, DBUtility.ConnectionStringType.Configured);

        var result = csvData.AsEnumerable()
                        .Select(row => new
                        {
                            FacilityID = row.Field<string>("FacilityID"),
                            FacilityName = row.Field<string>("FacilityName"),
                            CustomerID = row.Field<string>("CustomerID"),
                            C_Description = row.Field<string>("C_Description"),
                            Address1 = row.Field<string>("Address1"),
                            City = row.Field<string>("City"),
                            State = row.Field<string>("State"),
                            Zip = row.Field<string>("Zip")
                        })
                        .Distinct();
        foreach (var grp in result)
        {
            string id = grp.FacilityID;
            int ExistFacility = Convert.ToInt32(dbU.ExecuteScalar("select count(FacilityID) from [3PL_Facilities] where " +
                            "facilityID = '" + grp.FacilityID + "' and " +
                            "FacilityName = '" + grp.FacilityName + "' and " +
                            "CustomerID = '" + grp.CustomerID + "'"));
            if (ExistFacility == 0)
            {
                string strsql = "insert into [3PL_Facilities] (facilityID,FacilityName,CustomerID,C_Description,Address1,City,State,Zip ) values ('" +
                                grp.FacilityID + "','" + grp.FacilityName.Replace("'", " ") + "','" +
                                grp.CustomerID + "','" +
                                grp.C_Description.Replace("'", " ") + "','" +
                                grp.Address1.Replace("'", " ") + "','" +
                                grp.City.Replace("'", " ") + "','" +
                                grp.State + "','" +
                                grp.Zip + "')";


                dbU.ExecuteScalar(strsql);


            }
        }

        int errors = 0;
        string results = "ok";

        dbU.ExecuteScalar("delete from [3PL_Stock_Status]");

        //DataTable distinctTable = csvData.DefaultView.ToTable(true, "FacilityID", "SKU", "ItemID", "I_Description", "Description2", "SumOFOnHand", "SumOFAllocated", "SumOfAvailable");


        var groupQuery = from table in csvData.AsEnumerable()
                         group table by new
                         {
                             FacilityID = table["FacilityID"],
                             SKU = table["SKU"],
                             ItemID = table["ItemID"],
                             I_Description = table["I_Description"],
                             Location = table["Description2"],
                             Description2 = table["Description2"]
                         }
                             into grp
                             select new
                             {
                                 x = grp.Key,
                                 Shand = grp.Sum(t => double.Parse(t.Field<string>("SumOFOnHand"))),
                                 Sall = grp.Sum(t => double.Parse(t.Field<string>("SumOFAllocated"))),
                                 Sav = grp.Sum(t => double.Parse(t.Field<string>("SumOfAvailable")))
                             };

        try
        {
            foreach (var row in groupQuery)
            {
                string strsql = "insert into [3PL_Stock_Status] (FacilityID,SKU,ItemID,I_Description,Description2,SumOnHand,SumAllocated,SumAvailable,ImportDate ) values ('" +
                                 row.x.FacilityID.ToString().Replace("'", " ") + "', '" +
                                  row.x.SKU.ToString().Replace("'", " ") + "', '" +
                                  row.x.ItemID.ToString().Replace("'", " ") + "', '" +
                                  row.x.I_Description.ToString().Replace("'", " ") + "', '" +
                                  row.x.Description2.ToString().Replace("'", " ") + "', " +
                                  row.Shand + "," +
                                  row.Sall + "," +
                                  row.Sav + ",GETDATE())";
                dbU.ExecuteScalar(strsql);

            }
        }
        catch (Exception ex)
        {
            errors++;
            results = ex.Message;
        }


        return results;
    }
}