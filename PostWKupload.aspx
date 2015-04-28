<%@ Page Title="" Language="C#" MasterPageFile="~/Site.master" AutoEventWireup="true" CodeFile="PostWKupload.aspx.cs" Inherits="PostWKupload" %>

<asp:Content ID="Content1" ContentPlaceHolderID="MainContent" runat="Server">

    <div>
        Commercial Network Source <font color="#0066cc">
            <label id="LabelLocation" runat="server" />
            
        </font></>
    </div>
    <br>
    <div id="rpt1" style="width: 42%; height: 300px; display: inline-block;">
        <div id="filesPDF" style="overflow: scroll; width: 100%; height: 259px">
            <asp:UpdatePanel ID="Updatepanel1" runat="server" UpdateMode="Conditional">
                <ContentTemplate>
                    <div>
                        <asp:Label ID="Label3" AssociatedControlID="CheckBoxListFilesP" runat="server"
                            Text="Files to Upload" CssClass="CheckBoxLabel"></asp:Label>
                        <asp:Button ID="All1" BackColor="YellowGreen" runat="server" Text="All" OnClick="selectAllP" />
                        <asp:Button ID="Button1" runat="server" Text="  Upload"
                            OnClick="UploadFiles"
                            class="dropdown2" />

                        <asp:CheckBoxList ID="CheckBoxListFilesP"
                            runat="server" CssClass="wk_DataTable">
                        </asp:CheckBoxList>
                    </div>
                </ContentTemplate>
            </asp:UpdatePanel>
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
                <asp:Label ID="Label1" AssociatedControlID="CheckBoxListFilesU" runat="server"
                    Text="Data" CssClass="CheckBoxLabel"></asp:Label>

                <asp:Button ID="Button2" runat="server" Text=" Output CSV"
                    OnClick="ExportCSV"
                    class="dropdown2" />
                <asp:CheckBoxList ID="CheckBoxListFilesU"
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