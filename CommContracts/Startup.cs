using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(CommContracts.Startup))]
namespace CommContracts
{
    public partial class Startup {
        public void Configuration(IAppBuilder app) {
            ConfigureAuth(app);
        }
    }
}
