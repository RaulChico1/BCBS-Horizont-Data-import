<%@ Page Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="WeeklyProcess.aspx.cs" Inherits="CommContracts.WeeklyProcess" %>


<asp:Content ID="Content1" ContentPlaceHolderID="MainContent" runat="server">
    <div>
        Network Source <font color="#0066cc">
            <label id="LabelLocation" runat="server" />
            <p><font size="3" color="red">
                <label id="LabelApp" runat="server" />
            </font></p>
        </font></>
    </div>
    <br>
    <div id="rpt1" style="width: 42%; height: 300px; display: inline-block;">
        <div id="filesPDF" style="overflow: scroll; width: 100%; height: 259px">
           
                <ContentTemplate>
                    <div>
                        <asp:Label ID="Label3"  runat="server"
                            Text="Master Files" CssClass="CheckBoxLabel"></asp:Label>
                       
                        <asp:Button ID="Button1"  BackColor="YellowGreen" runat="server" Text="  Upload"
                            OnClick="UploadFiles"
                            class="dropdown2"
                            OnClientClick="return do_totals1();" />
                        <label id="LabelM" runat="server" />
                       
                    </div>
                </ContentTemplate>
           
        </div>

        <div>
            <span>
                <label id="CopyFilesTOMSG" runat="server" />
            </span>
        </div>
    </div>
    <div style="float: right; width: 58%; height: 300px; display: inline-block;">
        <div id="filesdata" style="overflow: scroll; width: 120%; height: 259px">
            <div>
                <asp:Label ID="Label1"  runat="server"
                    Text="Files Uploaded " CssClass="CheckBoxLabel"></asp:Label>
              
            </div>
        </div>

        <span>
            <label id="Label2" runat="server" />
        </span>
    </div>

</asp:Content>
