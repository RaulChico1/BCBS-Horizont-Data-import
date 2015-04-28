using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(Horizon_Services.Startup))]
namespace Horizon_Services
{
    public partial class Startup {
        public void Configuration(IAppBuilder app) {
            ConfigureAuth(app);
        }
    }
}
