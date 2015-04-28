<%@ Page Title="" Language="C#" MasterPageFile="~/Site.master" AutoEventWireup="true" CodeFile="Queries.aspx.cs" Inherits="Queries" %>

<asp:Content ID="Content1" ContentPlaceHolderID="MainContent" Runat="Server">
  <div>
        Queries Source <font color="#0066cc">
            <label id="LabelLocation" runat="server" />
            
        </font></>
    </div>
    <br>
    <div id="rpt1" style="width: 42%; height: 300px; display: inline-block;">
        <div id="filesXML" style="overflow: scroll; width: 100%; height: 259px">
            <div>
                <asp:Label ID="Label3" AssociatedControlID="CheckBoxListFilesP" runat="server"
                    Text="Files to Upload" CssClass="CheckBoxLabel"></asp:Label>
                <asp:Button ID="All1" BackColor="YellowGreen" runat="server" Text="All" />
                <asp:Button ID="Button1" runat="server" Text="  Upload Query list"
                    OnClick="UploadFiles"
                    class="dropdown2" />
                <asp:CheckBoxList ID="CheckBoxListFilesP"
                    runat="server" CssClass="wk_DataTable">
                </asp:CheckBoxList>
            </div>
        </div>

        <div>
            <span>
                <label id="queriesTOMSG" runat="server" />
            </span>
        </div>
    </div>
</asp:Content>

