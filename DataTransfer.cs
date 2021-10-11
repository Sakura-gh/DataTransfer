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

            //await InfrastructureUnitsUtil.EFZTopology();
            //await InfrastructureUnitsUtil.EFZTenantMapping();
            //await InfrastructureUnitsUtil.AsnTopology();

            dataTransfer.start(); // parallel async, with channel
            //dataTransfer.sequentialExecute(); // serial
            //dataTransfer.parallelExecute(); // parallel sync, without channel
            //dataTransfer.parallelAsyncWithoutChannel(); // parallel async, without channel

            // commit, full refresh, just once
            //await InfrastructureUnitsUtil.AsnTenantMappingFinished(myWriter.guid);

            //log.LogInformation("guid: " + myWriter.guid);
            
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
        // store the tmp partial record
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
                            tenantId = splitArray[0].Trim().Trim('"'),
                            asn = splitArray[1].Trim(),
                            requestCount = splitArray[2].Trim(),
                            requestBytes = splitArray[3].Trim(),
                            responseBytes = splitArray[4].Trim()
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
        public static Guid guid = Guid.NewGuid();

        public override void writeData(List<TenantAsn> tenantAsnList)
        {
            //String s = JsonConvert.SerializeObject(tenantAsnList);
            //CloudBlobUtil.getOutputBlob().UploadTextAsync(s);

            // try to load data into db
            InfrastructureUnitsUtil.AsnTenantMapping(tenantAsnList, guid).Wait();

            //tenantAsnList = null;
            //GC.Collect();
        }

    }
}
