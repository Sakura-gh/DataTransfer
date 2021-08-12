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

namespace DataTransfer
{
    public class DataTransferFramework<T, R, W> 
        where T : Message
        where R : Reader<T>, new()
        where W : Writer<T>, new()
    {
        private Reader<T> reader;
        private Writer<T> writer;
        private Channel<List<T>> channel;
        private Producer<T> producer;
        private Consumer<T> consumer;

        private long sliceNum;
        // 用于保存数据分片时只截取了一半的记录
        private List<PartialRecord> partialRecords = new List<PartialRecord>();

        public DataTransferFramework(Reader<T> reader, Writer<T> writer)
        {
            this.reader = reader;
            this.writer = writer;
            
        }

        public void start()
        {
            // 1. 创建channel，获取producer和consumer
            initChannel();
            this.producer = new Producer<T>(this.channel.Writer);
            this.consumer = new Consumer<T>(this.channel.Reader);
            // 2. 将producer和consumer赋值给reader和writer，使其变为可执行代码
            this.reader.setProducer(this.producer);
            this.writer.setConsumer(this.consumer);
            // 3. 执行并发读和写的线程池(起多个线程分别执行reader和writer的核心业务)
            Task read = parallelReadAsync();
            Task write = parallelWriteAsync();
            //Task.WaitAll(new Task[] { read, write });
            Task.WhenAll(new List<Task> { read, write }).Wait();
        }

        private void initChannel()
        {
            var channelOptions = new BoundedChannelOptions(20)
            {
                FullMode = BoundedChannelFullMode.Wait
            };
            this.channel = Channel.CreateBounded<List<T>>(channelOptions);
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

        //private R getNewReader()
        //{
        //    return DeepClone<R>((R)this.reader);
        //}

        //private W getNewWriter()
        //{
        //    return DeepClone<W>((W)this.writer);
        //}

        //private C DeepClone<C>(C source)
        //{
        //    String s = JsonConvert.SerializeObject(source);
        //    TimeLogger.Log("object: " + s);
        //    return JsonConvert.DeserializeObject<C>(s);
        //}

        // parallel + async => read
        private async Task parallelReadAsync()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = getInputBlob();

            // 2. get data slices
            Queue<DataSlice> sliceQueue= getDataSlices(inputBlob);

            // 3. start read data slices and put them into channel, parallel and async
            await readSlices(sliceQueue);
        }

        private CloudBlockBlob getInputBlob()
        {
            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("shdprod");

            string input = "TenantMapping/TenantAsnMapping_test.csv";
            CloudBlockBlob inputBlob = container.GetBlockBlobReference(input);

            return inputBlob;
        }

        private Queue<DataSlice> getDataSlices(CloudBlockBlob inputBlob)
        {
            inputBlob.FetchAttributesAsync().Wait();
            // 16 MB chunk
            //int BUFFER_SIZE = 16 * 1024 * 1024;
            int BUFFER_SIZE = 1024;
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

        private async Task readSlices(Queue<DataSlice> dataSlices)
        {
            await parallelForEachAsync<DataSlice>(dataSlices, readTask, 4);
       
        }

        private async Task readTask(DataSlice dataSlice)
        {
            using (var memoryStream = new MemoryStream())
            {
                //TimeLogger.startTimeLogger();
                // 1. get the memory stream from input blob
                // 为了避免由于竞争给inputBlob加锁导致的时间损耗，这里每个slice都单独创建一个新的inputBlob
                await getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize);
                // 2. read and parse memory stream, put it into the channel
                TimeLogger.Log("download slice: " + dataSlice.id);
                //await reader.readAndProduceAsync(memoryStream);
                //await getNewReader().readAndProduceAsync(memoryStream);
                await getNewReader().readAndProduceAsync(memoryStream);
                TimeLogger.Log("parse and produce slice: " + dataSlice.id);
                //TimeLogger.stopTimeLogger("data slice complete: " + dataSlice.id);
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
            await parallelForEachAsync<int>(a, async (i) => {
                //await writer.consumeAndWriteAsync();
                await getNewWriter().consumeAndWriteAsync();
                //await getNewWriter().consumeAndWriteAsync();
                TimeLogger.Log("upload times: " + i);
            }, 2);
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

        private class PartialRecord
        {
            private string front;
            private string end;

            public void setFront(String front)
            {
                this.front = front;
            }

            public void setEnd(String end)
            {
                this.end = end;
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

        // need to be implemented
        public abstract Task readAndProduceAsync(MemoryStream memoryStream);
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

    public class myReader: Reader<TenantAsn>
    {
        public override async Task readAndProduceAsync(MemoryStream memoryStream)
        {
            // 1. parse the memoryStream to messages
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
                } catch (Exception e)
                {
                    TimeLogger.Log(line + ": " + e.Message);
                }
            }
            // 2. produce each message to channel
            // 消息队列是一个典型的异步方式，无需加await，本质上就是本线程通知
            // 要发消息了，然后本线程的任务就完成结束了，接下来具体消息的发送是
            // 异步的，由新起的一个线程来完成
            await sendMessagesAsync(tenantAsnList);
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
            TimeLogger.Log("upload content : " + s);
        }

        private CloudBlockBlob getOutputBlob()
        {
            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference("shdprod");

            string output = "TenantMapping/TenantAsnMapping_test.txt";
            CloudBlockBlob outputBlob = container.GetBlockBlobReference(output);

            return outputBlob;
        }
    }
}
