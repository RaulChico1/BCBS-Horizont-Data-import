<%@ Page Title="" Language="C#" MasterPageFile="~/Site.master" AutoEventWireup="true" CodeFile="NL_UploadFiles.aspx.cs" Inherits="NL_UploadFiles" %>


<asp:Content ID="Content1" ContentPlaceHolderID="MainContent" Runat="Server">
     <div>
        Network Source <font color="#0066cc">
            <label id="LabelLocation" runat="server" />
            
        </font></>
    </div>
<br>
    <div id="rpt1" style="width: 42%; height: 300px; display: inline-block;">
        <div id="filesPDF" style="overflow: scroll; width: 100%; height: 259px">
            <div>
                <asp:Label ID="Label3" AssociatedControlID="CheckBoxListFilesP" runat="server"
                    Text="Days to Unzip" CssClass="CheckBoxLabel"></asp:Label>
                <asp:Button ID="All1" BackColor="YellowGreen" runat="server" Text="All" OnClick="selectAllP" />
                <asp:Button ID="Button1" runat="server" Text="  Unzip to tmp"
                    OnClick="ExtractFiles"
                    class="dropdown2" OnClientClick="StartProgressBar()" />
                <asp:CheckBoxList ID="CheckBoxListFilesP"
                    runat="server" CssClass="wk_DataTable" 
                    OnSelectedIndexChanged="CheckBoxListFilesP_SelectedIndexChanged" AutoPostBack="true" >
                </asp:CheckBoxList>
            </div>
        </div>

        <div>
            <span>
                <label id="CopyFilesTOMSG" runat="server" />
            </span>
        </div>
    </div>
    <div style="float: right; width: 52%; height: 300px; display: inline-block;">
        <div id="filesdata" style="overflow: scroll; width: 100%; height: 259px">
            <div>
                <asp:Label ID="Label1" AssociatedControlID="CheckBoxListFilesD" runat="server"
                    Text="Data" CssClass="CheckBoxLabel"></asp:Label>
                
                <asp:Button ID="Button2" runat="server" Text="Get counts only"
                    OnClick="getCounts"
                    class="dropdown2" />
                <asp:CheckBoxList ID="CheckBoxListFilesD"
                    runat="server" CssClass="wk_DataTable">
                </asp:CheckBoxList>
            </div>
        </div>
        <div>
            <span>
                <label id="Label2" runat="server" />
            </span>
        </div>
    </div>
</asp:Content>

