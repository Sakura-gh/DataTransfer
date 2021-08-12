using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace DataTransfer
{
    public static class DataTransfer
    {
        [FunctionName("DataTransfer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            TimeLogger.setLogger(log);
            var reader = new myReader();
            var writer = new myWriter();
            DataTransferFramework<TenantAsn> dataTransfer = new DataTransferFramework<TenantAsn>(reader, writer);
            log.LogInformation("data transfer begin");
            dataTransfer.start();
            log.LogInformation("data transfer end");
            return (ActionResult)new OkObjectResult(new { Result = "Success" });
        }
    }
}
