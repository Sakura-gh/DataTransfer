using System;
using System.IO;
using System.Threading;
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
using System.Collections.Concurrent;
using System.Reflection;
using System.Collections;

namespace DataTransfer
{
    public class DataTransferFramework<T, R, W>
        where T : Message
        where R : Reader<T>, new()
        where W : Writer<T>, new()
    {
        private Channel<List<T>> channel;
        private Producer<T> producer;
        private Consumer<T> consumer;

        private long sliceNum;

        public DataTransferFramework() { }

        public void start()
        {
            // 1. 创建channel，获取producer和consumer
            initChannel();
            this.producer = new Producer<T>(this.channel.Writer);
            this.consumer = new Consumer<T>(this.channel.Reader);
            // 2. 执行并发读和写的线程池(起多个线程分别执行reader和writer的核心业务)
            // 每个线程新建一个reader/writer，避免多线程在reader/writer上竞争导致死锁
            Task read = parallelReadAsync();
            Task write = parallelWriteAsync();
            Task.WhenAll(new List<Task> { read, write }).Wait();
        }

        public void testSequentialExecute()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = getBlob("TenantMapping/TenantAsnMapping_2021-07-31.csv");

            // 2. get output blob
            CloudBlockBlob outputBlob = getBlob("TenantMapping/TenantAsnMapping_2021-07-31_test.txt");

            // 2. get data slices
            Queue<DataSlice> sliceQueue = getDataSlices(inputBlob);

            while (sliceQueue.Count() > 0)
            {
                using (var memoryStream = new MemoryStream())
                {
                    DataSlice slice = sliceQueue.Dequeue();
                    inputBlob.DownloadRangeToStreamAsync(memoryStream, slice.offset, slice.realSize).Wait();
                    string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
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
                    String s = JsonConvert.SerializeObject(tenantAsnList);
                    outputBlob.UploadTextAsync(s).Wait();
                    TimeLogger.Log("slice " + slice.id);
                }
            }
        }

        public void testSequentialExecuteParallel()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = getBlob("TenantMapping/TenantAsnMapping_2021-07-31.csv");

            // 2. get output blob
            CloudBlockBlob outputBlob = getBlob("TenantMapping/TenantAsnMapping_2021-07-31_test.txt");

            // 2. get data slices
            Queue<DataSlice> sliceQueue = getDataSlices(inputBlob);

            Parallel.ForEach(sliceQueue,
                new ParallelOptions()
                {
                    // 最大的并发度
                    MaxDegreeOfParallelism = 10
                },
                (slice) =>
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        try
                        {
                            getBlob("TenantMapping/TenantAsnMapping_2021-07-31.csv").DownloadRangeToStreamAsync(memoryStream, slice.offset, slice.realSize).Wait();
                            string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
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
                            String s = JsonConvert.SerializeObject(tenantAsnList);
                            getBlob("TenantMapping/TenantAsnMapping_2021-07-31_test.txt").UploadTextAsync(s).Wait();
                            TimeLogger.Log("slice " + slice.id);
                        }
                        catch (Exception e)
                        {
                            TimeLogger.Log("slice " + slice.id + ": " + e.Message);
                        }
                    }
                }
            );
        }

        private void initChannel()
        {
            //var channelOptions = new BoundedChannelOptions(100)
            //{
            //    FullMode = BoundedChannelFullMode.Wait
            //};
            //this.channel = Channel.CreateBounded<List<T>>(channelOptions);

            this.channel = Channel.CreateUnbounded<List<T>>();
        }

        private R getNewReader()
        {
            var r = new R();
            r.setProducer(producer);
            return r;
        }

        private W getNewWriter()
        {
            var w = new W();
            w.setConsumer(this.consumer);
            return w;
        }

        private void parallelRead()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = getInputBlob();

            // 2. get data slices
            Queue<DataSlice> sliceQueue = getDataSlices(inputBlob);

            // 3. start read data slices and put them into channel, parallel
            readSlices(sliceQueue);
        }

        // parallel + async => read
        private async Task parallelReadAsync()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = getInputBlob();

            // 2. get data slices
            Queue<DataSlice> sliceQueue = getDataSlices(inputBlob);

            // 3. start read data slices and put them into channel, parallel and async
            await readSlicesAsync(sliceQueue);
        }

        private CloudBlockBlob getBlob(string name)
        {
            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("shdprod");

            CloudBlockBlob inputBlob = container.GetBlockBlobReference(name);

            return inputBlob;
        }

        private CloudBlockBlob getInputBlob()
        {
            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("shdprod");

            string input = "TenantMapping/TenantAsnMapping_2021-07-31.csv";
            //string input = "TenantMapping/TenantAsnMapping_test.csv";
            CloudBlockBlob inputBlob = container.GetBlockBlobReference(input);

            return inputBlob;
        }

        private Queue<DataSlice> getDataSlices(CloudBlockBlob inputBlob)
        {
            inputBlob.FetchAttributesAsync().Wait();
            // 16 MB chunk
            int BUFFER_SIZE = 1 * 1024 * 1024;
            //int BUFFER_SIZE = 1024;
            // blob还剩下多少data
            long blobRemainingSize = inputBlob.Properties.Length;
            // sliceQueue里保存着给每个线程分配的任务，blob读取的起始位置offset和读取的大小realSize
            Queue<DataSlice> sliceQueue = new Queue<DataSlice>();
            // 初始offset为0
            long offset = 0;
            // 初始id为0
            long id = 0;
            // 往queue里塞初始化好的任务配置
            while (blobRemainingSize > 0)
            {
                long realSize = (long)Math.Min(BUFFER_SIZE, blobRemainingSize);
                sliceQueue.Enqueue(new DataSlice(offset, realSize, BUFFER_SIZE, id++));
                offset += BUFFER_SIZE;
                blobRemainingSize -= BUFFER_SIZE;
            }
            sliceNum = sliceQueue.Count();
            return sliceQueue;
        }

        private void readSlices(Queue<DataSlice> dataSlices)
        {
            TimeLogger.startTimeLogger();
            Parallel.ForEach(dataSlices,
                new ParallelOptions()
                {
                    // 最大的并发度
                    MaxDegreeOfParallelism = 10
                },
                (dataSlice) =>
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        TimeLogger.startTimeLogger();
                        getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize).Wait();
                        TimeLogger.Log("download slice: " + dataSlice.id);
                        getNewReader().readAndProduceAsync(memoryStream).Wait();
                        TimeLogger.stopTimeLogger("poduce slice: " + dataSlice.id);
                    }
                }
            );
            TimeLogger.stopTimeLogger("ParallelDownloadBlob");
        }

        public void testNoChannel()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = getInputBlob();

            // 2. get data slices
            Queue<DataSlice> dataSlices = getDataSlices(inputBlob);

            // 3. test without channel
            parallelForEachAsync<DataSlice>(dataSlices, readAndWriteTask, 10).Wait();
        }

        private async Task readAndWriteTask(DataSlice dataSlice)
        {
            using (var memoryStream = new MemoryStream())
            {
                TimeLogger.startTimeLogger();
                await getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize);
                List<T> dataList = getNewReader().readData(memoryStream);
                String s = JsonConvert.SerializeObject(dataList);
                await getBlob("TenantMapping/TenantAsnMapping_2021-07-31_test.txt").UploadTextAsync(s);
                TimeLogger.stopTimeLogger("slice " + dataSlice.id + " completed");
            }
        }

        private async Task readSlicesAsync(Queue<DataSlice> dataSlices)
        {
            await parallelForEachAsync<DataSlice>(dataSlices, readTaskAsync, 5);

        }

        private async Task readTaskAsync(DataSlice dataSlice)
        {
            using (var memoryStream = new MemoryStream())
            {
                TimeLogger.startTimeLogger();
                // 1. get the memory stream from input blob
                // 为了避免由于竞争给inputBlob加锁导致的时间损耗，这里每个slice都单独创建一个新的inputBlob
                await getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize);
                // 2. read and parse memory stream, put it into the channel
                TimeLogger.Log("download slice: " + dataSlice.id);
                //await reader.readAndProduceAsync(memoryStream);
                //await getNewReader().readAndProduceAsync(memoryStream);
                await getNewReader().readAndProduceAsync(memoryStream);
                //TimeLogger.Log("parse and produce slice: " + dataSlice.id);
                TimeLogger.stopTimeLogger("poduce slice: " + dataSlice.id);
            }
        }

        // parallel + async => write
        private async Task parallelWriteAsync()
        {
            List<int> a = new List<int>();
            for (int i = 0; i < sliceNum; i++)
            {
                a.Add(i);
            }
            await parallelForEachAsync<int>(a, async (i) =>
            {
                //await writer.consumeAndWriteAsync();
                await getNewWriter().consumeAndWriteAsync();
                //await getNewWriter().consumeAndWriteAsync();
                TimeLogger.Log("upload slice: " + i);
            }, 10);
            //W writer = getNewWriter();
            //int i = 0;
            //try
            //{
            //    while (true)
            //    {
            //        await writer.consumeAndWriteAsync();
            //        TimeLogger.Log("upload slice: " + ++i);
            //    }
            //} catch (Exception e)
            //{
            //    TimeLogger.Log("upload slice complete: " + e.Message);
            //}
        }

        public Task parallelForEachAsync<U>(IEnumerable<U> source, Func<U, Task> funcBody, int maxDoP = 10)
        {
            async Task AwaitPartition(IEnumerator<U> partition)
            {
                using (partition)
                {
                    while (partition.MoveNext())
                    {
                        await Task.Yield(); // prevents a sync/hot thread hangup
                        await funcBody(partition.Current);
                    }
                }
            }

            return Task.WhenAll(
                Partitioner
                .Create(source)
                .GetPartitions(maxDoP)
                .AsParallel()
                .Select(p => AwaitPartition(p)));
        }


        private class DataSlice
        {
            public long offset;
            public long realSize;
            public long sliceSize;
            public long id;

            public DataSlice() { }

            public DataSlice(long offset, long realSize, long sliceSize, long id)
            {
                this.offset = offset;
                this.realSize = realSize;
                this.sliceSize = sliceSize;
                this.id = id;
            }
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

    public class Producer<T> where T : Message
    {
        private readonly ChannelWriter<List<T>> channelWriter;

        public Producer(ChannelWriter<List<T>> channelWriter)
        {
            this.channelWriter = channelWriter;
        }

        public async Task PushAsync(List<T> messages)
        {
            await channelWriter.WriteAsync(messages);
        }
    }

    public class Consumer<T> where T : Message
    {
        private readonly ChannelReader<List<T>> channelReader;

        public Consumer(ChannelReader<List<T>> channelReader)
        {
            this.channelReader = channelReader;
        }

        public async Task<List<T>> PullAsync()
        {
            return await channelReader.ReadAsync();
        }
    }

    public abstract class Reader<T> where T : Message
    {
        private Producer<T> producer;
        public void setProducer(Producer<T> producer)
        {
            this.producer = producer;
        }

        public async Task sendMessagesAsync(List<T> messages)
        {
            await producer.PushAsync(messages);
        }

        public async Task readAndProduceAsync(MemoryStream memoryStream)
        {
            // 1. parse the memoryStream to messages
            List<T> dataList = readData(memoryStream);
            // 2. produce each message to channel
            // 消息队列是一个典型的异步方式，无需加await，本质上就是本线程通知
            // 要发消息了，然后本线程的任务就完成结束了，接下来具体消息的发送是
            // 异步的，由新起的一个线程来完成
            await sendMessagesAsync(dataList);
        }

        // need to be implemented
        public abstract List<T> readData(MemoryStream memoryStream);
    }

    public abstract class Writer<T> where T : Message
    {
        private Consumer<T> consumer;
        public void setConsumer(Consumer<T> consumer)
        {
            this.consumer = consumer;
        }

        public async Task<List<T>> getMessages()
        {
            List<T> messages = await consumer.PullAsync();
            return messages;
        }
        // need to be implemented
        public abstract Task consumeAndWriteAsync();
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
        public override async Task consumeAndWriteAsync()
        {
            // 1. consume the messageList from channel
            List<TenantAsn> tenantAsnList = await getMessages();
            // 2. upload
            CloudBlockBlob outputBlob = getOutputBlob();
            String s = JsonConvert.SerializeObject(tenantAsnList);
            await outputBlob.UploadTextAsync(s);
            tenantAsnList = null;
            GC.Collect();
            //TimeLogger.Log("upload content : " + s);
        }

        private CloudBlockBlob getOutputBlob()
        {
            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("shdprod");

            string output = "TenantMapping/TenantAsnMapping_2021-07-31_test.txt";
            //string output = "TenantMapping/TenantAsnMapping_test.txt";
            CloudBlockBlob outputBlob = container.GetBlockBlobReference(output);

            return outputBlob;
        }
    }
}
