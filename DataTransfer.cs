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
using System.Threading.Channels;
using System.Text;
using System.Linq;
using Newtonsoft.Json;

namespace DataTransfer
{
    public static class DataTransfer
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

    //public abstract class Message { }
    public interface Message { }

    public class TenantAsn : Message
    {
        public string tenantId { get; set; }
        public string asn { get; set; }
        public string requestCount { get; set; }
        public string requestBytes { get; set; }
        public string responseBytes { get; set; }
    }

    public class Producer
    {
        private readonly ChannelWriter<List<Message>> channelWriter;

        public Producer(ChannelWriter<List<Message>> channelWriter)
        {
            this.channelWriter = channelWriter;
        }

        public async Task PushAsync(List<Message> messages)
        {
            await channelWriter.WriteAsync(messages);
        }
    }

    public class Consumer
    {
        private readonly ChannelReader<List<Message>> channelReader;

        public Consumer(ChannelReader<List<Message>> channelReader)
        {
            this.channelReader = channelReader;
        }

        public async ValueTask<List<Message>> PullAsync()
        {
            return await channelReader.ReadAsync();
        }
    }

    public abstract class Reader
    {
        protected Producer producer { get;  set; }
        // need to be implemented
        public abstract void readAndProduce(MemoryStream memoryStream);
    }

    public abstract class Writer
    {
        protected Consumer consumer { get; set; }
        // need to be implemented
        public abstract void consumeAndWrite();
    }

    public class myReader : Reader
    {
        public override async void readAndProduce(MemoryStream memoryStream)
        {
            // 1. parse the memoryStream to messages
            string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
            var lines = stream2string.Split("\n").ToList();
            lines.RemoveAt(0);
            lines.RemoveAt(lines.Count() - 1);
            var tenantAsnList = new List<TenantAsn>();
            foreach (var line in lines)
            {
                string[] splitArray = line.Split(",");
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
            // 2. produce each message to channel
            await producer.PushAsync(tenantAsnList);
        }

    }

    public class myWriter : Writer
    {
        public override void consumeAndWrite()
        {
            // 1. consume the messageList from channel
            List<TenantAsn> tenantAsnList = consumer.PullAsync();
        }
    }
}
