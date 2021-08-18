using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Linq;

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
            dataTransfer.start(); // 异步并行，含channel
            //dataTransfer.sequentialExecute(); // 纯串行
            //dataTransfer.parallelExecute(); // 纯同步并行，无channel
            //dataTransfer.parallelAsyncWithoutChannel(); // 异步并行，无channel
            log.LogInformation("data transfer end");
            return (ActionResult)new OkObjectResult(new { Result = "Success" });
        }
    }

    // should be implemeted by developer
    public class TenantAsn : Message
    {
        public string tenantId { get; set; }
        public string asn { get; set; }
        public string requestCount { get; set; }
        public string requestBytes { get; set; }
        public string responseBytes { get; set; }
    }

    public class myReader : Reader<TenantAsn>
    {
        // 用于保存数据分片时只截取了一半的记录
        private Hashtable partialRecordTable = Hashtable.Synchronized(new Hashtable());

        public override List<TenantAsn> readData(MemoryStream memoryStream)
        {
            string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
            //TimeLogger.Log(stream2string);

            List<TenantAsn> tenantAsnList = new List<TenantAsn>();

            var lines = stream2string.Split("\n").ToList();

            lines.RemoveAt(0);
            lines.RemoveAt(lines.Count() - 1);
            foreach (var line in lines)
            {
                string[] splitArray = line.Split(",");
                try
                {
                    tenantAsnList.Add(
                        new TenantAsn
                        {
                            tenantId = splitArray[0],
                            asn = splitArray[1],
                            requestCount = splitArray[2],
                            requestBytes = splitArray[3],
                            responseBytes = splitArray[4]
                        }
                    );
                }
                catch (Exception e)
                {
                    TimeLogger.Log(line + ": " + e.Message);
                }
            }

            return tenantAsnList;
        }

    }

    public class myWriter : Writer<TenantAsn>
    {
        public override void writeData(List<TenantAsn> tenantAsnList)
        {
            //String s = JsonConvert.SerializeObject(tenantAsnList);
            //CloudBlobUtil.getOutputBlob().UploadTextAsync(s);

            // try to load data into db
            InfrastructureUnitsUtil.TenantMapping(tenantAsnList);
            
            tenantAsnList = null;
            GC.Collect();
        }

    }
}
