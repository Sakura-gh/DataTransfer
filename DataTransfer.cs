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
            DataTransferFramework<TenantAsn, myReader, myWriter> dataTransfer = new DataTransferFramework<TenantAsn, myReader, myWriter>();
            log.LogInformation("data transfer begin");
            //dataTransfer.start(); // 异步并行，含channel
            dataTransfer.sequentialExecute(); // 纯串行
            //dataTransfer.parallelExecute(); // 纯同步并行，无channel
            //dataTransfer.parallelAsyncWithoutChannel(); // 异步并行，无channel
            log.LogInformation("data transfer end");
            return (ActionResult)new OkObjectResult(new { Result = "Success" });
        }
    }
}
