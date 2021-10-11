注册生产环境资源：[Office365 Cosmos Management (cosman.azurewebsites.net)](https://cosman.azurewebsites.net/cost/servicetree?viewType=Service&id=13f41f0f-32ed-4cc7-bf72-760d92e705a8&dimension=processing)

job name：[SHD_asn_tenant](https://cosman.azurewebsites.net/search?searchType=job&cluster=cosmos14-prod-cy2&virtualCluster=exchange.storage.prod&keywords=SHD_asn_tenant&comparativeMethod=equals&caseSensitive=true)

在cosmos环境上：

- script存在[COSMOS cosmos/exchange.storage.prod/local/Resources/M365Networking/script/t-haoge/ (osdinfra.net)](https://aad.cosmos14.osdinfra.net/cosmos/exchange.storage.prod/local/Resources/M365Networking/script/t-haoge/)上
- output存在[COSMOS cosmos/exchange.storage.prod/local/Aggregated/Datasets/Public/M365Networking/SHD/TenantMapping/TenantAsnMapping/ (osdinfra.net)](https://aad.cosmos14.osdinfra.net/cosmos/exchange.storage.prod/local/Aggregated/Datasets/Public/M365Networking/SHD/TenantMapping/TenantAsnMapping/)上

在Azure的Data Factory上，新创建一个Scope，选择生产环境ExchangeStorageProd，填上job name



分块操作并行下载：[c# - CloudBlockBlob.DownloadToStream vs DownloadRangeToStream - Stack Overflow](https://stackoverflow.com/questions/41810485/cloudblockblob-downloadtostream-vs-downloadrangetostream)

- [Asynchronous Parallel Block Blob Transfers with Progress Change Notification | Microsoft Docs](https://docs.microsoft.com/zh-cn/archive/blogs/kwill/asynchronous-parallel-block-blob-transfers-with-progress-change-notification)

使用c#的channel来完成生产者和消费者的解耦：[An Introduction to System.Threading.Channels | .NET Blog (microsoft.com)](https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/)

- [多线程并发如何高效实现生产者/消费者？ | 码农家园 (codenong.com)](https://www.codenong.com/cs110251191/)
- [在.NET Core中使用Channel（一） - 码农译站 - 博客园 (cnblogs.com)](https://www.cnblogs.com/hhhnicvscs/p/14249842.html)
- 



#### 结果对比

##### test1

result：

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210806190055152.png" alt="image-20210806190055152" style="zoom:50%;" />

code：

~~~c#
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Globalization;
using System.Linq;
using System.Diagnostics;

namespace DataTransfer
{
    public static class DataTransfer
    {
        [FunctionName("DataTransfer")]
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

    public static class TimeLogger
    {
        private static Stopwatch stopWatch = new Stopwatch();
        private static ILogger logger = null;

        public static void setLogger(ILogger log)
        {
            logger = log;   
        }
        
        public static bool startTimeLogger()
        {
            if (logger == null)
            {
                return false;
            }
            stopWatch.Start();
            return true;
        }

        public static bool stopTimeLogger(string msg = null)
        {
            if (logger == null)
            {
                return false;
            }
            stopWatch.Stop();
            logger.LogInformation(msg + ": spends " + stopWatch.ElapsedMilliseconds + " ms.");
            return true;
        }
    }
}
~~~





##### async, await的用法

什么时候触发新线程执行异步操作：https://blog.csdn.net/sinolover/article/details/104254408

