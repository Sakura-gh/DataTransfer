using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;

namespace DataTransfer
{
    public static class TestForDowload
    {
        [FunctionName("TestForDowload")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobContainer container = null;

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            container = client.GetContainerReference("shdprod");

            string input = "TenantMapping/TenantAsnMapping_2021-07-31.csv";
            CloudBlockBlob inputBlob = container.GetBlockBlobReference(input);
            string output = "TenantMapping/TenantAsnMapping_2021-07-31.json";
            CloudBlockBlob outputBlob = container.GetBlockBlobReference(output);

            TimeLogger.setLogger(log);

            try
            {
                log.LogInformation("ParallelDownloadBlob begin.");
                await ParallelDownloadBlob(inputBlob);
                log.LogInformation("ParallelDownloadBlob end.");
            }
            catch (Exception e)
            {
                log.LogInformation("ParallelDownloadBlob fail: " + e.Message);
            }

            try
            {
                log.LogInformation("DownloadBlob begin.");
                await DownloadBlob(inputBlob);
                log.LogInformation("DownloadBlob end.");
            }
            catch (Exception e)
            {
                log.LogInformation("DownloadBlob fail: " + e.Message);
            }

            return (ActionResult)new OkObjectResult(new { Result = "Success" });
        }

        public static async Task DownloadBlob(CloudBlockBlob inputBlob, Stream outputStream = null)
        {
            using (var memoryStream = new MemoryStream())
            {
                TimeLogger.startTimeLogger();
                await inputBlob.DownloadToStreamAsync(memoryStream);
                TimeLogger.stopTimeLogger("DownloadBlob");
                await memoryStream.FlushAsync();

            }
        }

        public static async Task ParallelDownloadBlob(CloudBlockBlob inputBlob, Stream outputStream = null)
        {
            // 填充blob的属性值和元数据
            await inputBlob.FetchAttributesAsync();
            // 16 MB chunk
            int BUFFER_SIZE = 16 * 1024 * 1024;
            // blob还剩下多少data
            long blobRemainingSize = inputBlob.Properties.Length;
            // taskQueue里保存着给每个线程分配的任务，blob读取的起始位置offset和读取的大小chunkSize
            Queue<KeyValuePair<long, long>> taskQueue = new Queue<KeyValuePair<long, long>>();
            long offset = 0;
            // 往queue里塞初始化好的任务配置
            while (blobRemainingSize > 0)
            {
                long chunkSize = (long)Math.Min(BUFFER_SIZE, blobRemainingSize);
                taskQueue.Enqueue(new KeyValuePair<long, long>(offset, chunkSize));
                offset += chunkSize;
                blobRemainingSize -= chunkSize;
            }

            TimeLogger.startTimeLogger();
            Parallel.ForEach(taskQueue,
                new ParallelOptions()
                {
                    // 最大的并发度
                    MaxDegreeOfParallelism = 10
                },
                async (task) =>
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        await inputBlob.DownloadRangeToStreamAsync(memoryStream, task.Key, task.Value);
                    }
                }
            );
            TimeLogger.stopTimeLogger("ParallelDownloadBlob");

        }
    }
}
