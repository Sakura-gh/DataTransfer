//using System;
//using System.IO;
//using System.Threading;
//using System.Threading.Tasks;
//using Microsoft.AspNetCore.Mvc;
//using Microsoft.Azure.WebJobs;
//using Microsoft.Azure.WebJobs.Extensions.Http;
//using Microsoft.AspNetCore.Http;
//using Microsoft.Extensions.Logging;
//using Microsoft.WindowsAzure.Storage;
//using Microsoft.WindowsAzure.Storage.Blob;
//using System.Collections.Generic;
//using System.Threading.Channels;
//using System.Text;
//using System.Linq;
//using Newtonsoft.Json;
//using System.Collections.Concurrent;
//using System.Reflection;
//using System.Collections;
//using DataTransfer;

//namespace DataTransferTest
//{
//    public class DataTransferFrameworkTest<T, R, W> 
//        where T : Message
//        where R : Reader<T>, new()
//        where W : Writer<T>, new()
//    {
//        private Channel<List<T>> channel;
//        private Producer<T> producer;
//        private Consumer<T> consumer;

//        private long sliceNum;

//        public DataTransferFrameworkTest() { }

//        public void start()
//        {
//            this.run();
//        }

//        private CloudBlockBlob getBlob(string name)
//        {
//            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
//            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

//            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
//            CloudBlobContainer container = client.GetContainerReference("shdprod");

//            CloudBlockBlob inputBlob = container.GetBlockBlobReference(name);

//            return inputBlob;
//        }

//        private CloudBlockBlob getInputBlob()
//        {
//            return getBlob("TenantMapping/TenantAsnMapping_2021-07-31.csv"); 
//        }

//        private CloudBlockBlob getOutputBlob()
//        {
//            return getBlob("TenantMapping/TenantAsnMapping_2021-07-31_test.txt");
//        }

//        private Queue<DataSlice> getDataSlices(CloudBlockBlob inputBlob)
//        {
//            inputBlob.FetchAttributesAsync().Wait();
//            // 16 MB chunk
//            int BUFFER_SIZE = 1 * 1024 * 1024;
//            //int BUFFER_SIZE = 1024;
//            // blob还剩下多少data
//            long blobRemainingSize = inputBlob.Properties.Length;
//            // sliceQueue里保存着给每个线程分配的任务，blob读取的起始位置offset和读取的大小realSize
//            Queue<DataSlice> sliceQueue = new Queue<DataSlice>();
//            // 初始offset为0
//            long offset = 0;
//            // 初始id为0
//            long id = 0;
//            // 往queue里塞初始化好的任务配置
//            while (blobRemainingSize > 0)
//            {
//                long realSize = (long)Math.Min(BUFFER_SIZE, blobRemainingSize);
//                sliceQueue.Enqueue(new DataSlice(offset, realSize, BUFFER_SIZE, id++));
//                offset += BUFFER_SIZE;
//                blobRemainingSize -= BUFFER_SIZE;
//            }
//            sliceNum = sliceQueue.Count();
//            return sliceQueue;
//        }

//        private void run()
//        {
//            // 1. get input blob
//            CloudBlockBlob inputBlob = getInputBlob();

//            // 2. get data slices
//            Queue<DataSlice> dataSlices = getDataSlices(inputBlob);

//            // 3. test without channel
//            parallelForEachAsync<DataSlice>(dataSlices, readAndWriteTask, 10).Wait();
//        }

//        public class TaskExecutor
//        {
//            public R reader;
//            public W writer;

//            public TaskExecutor()
//            {
//                Channel<T> channel = getNewChannel();
//                Producer<T> producer = new Producer<T>(channel.Writer);
//                Consumer<T> consumer = new Consumer<T>(channel.Reader);
//                this.reader = getNewReader();
//                this.reader.setProducer(producer);
//                this.writer = getNewWriter();
//                this.writer.setConsumer(consumer);
//            }

//            private Channel<T> getNewChannel()
//            {
//                var channelOptions = new BoundedChannelOptions(80)
//                {
//                    FullMode = BoundedChannelFullMode.Wait
//                };
//                return Channel.CreateBounded<T>(channelOptions);
//            }

//            private R getNewReader()
//            {
//                var r = new R();
//                return r;
//            }

//            private W getNewWriter()
//            {
//                var w = new W();
//                return w;
//            }
//        }

//        private async Task readAndWriteTask(DataSlice dataSlice)
//        {
//            using (var memoryStream = new MemoryStream())
//            {
//                TimeLogger.startTimeLogger();
//                await getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize);
//                TaskExecutor taskExecutor = new TaskExecutor();
//                await taskExecutor.reader.process(memoryStream);
//                await taskExecutor.writer.process();
//                TimeLogger.stopTimeLogger("slice " + dataSlice.id + " completed");
//            }
//        }

//        public Task parallelForEachAsync<U>(IEnumerable<U> source, Func<U, Task> funcBody, int maxDoP = 10)
//        {
//            async Task AwaitPartition(IEnumerator<U> partition)
//            {
//                using (partition)
//                {
//                    while (partition.MoveNext())
//                    {
//                        await Task.Yield(); // prevents a sync/hot thread hangup
//                        await funcBody(partition.Current);
//                    }
//                }
//            }

//            return Task.WhenAll(
//                Partitioner
//                .Create(source)
//                .GetPartitions(maxDoP)
//                .AsParallel()
//                .Select(p => AwaitPartition(p)));
//        }


//        private class DataSlice
//        {
//            public long offset;
//            public long realSize;
//            public long sliceSize;
//            public long id;

//            public DataSlice() { }

//            public DataSlice(long offset, long realSize, long sliceSize, long id)
//            {
//                this.offset = offset;
//                this.realSize = realSize;
//                this.sliceSize = sliceSize;
//                this.id = id;
//            }
//        }

//    }

//    //public abstract class Message { }
//    public interface Message { }

//    public class TenantAsn : Message
//    {
//        public string tenantId { get; set; }
//        public string asn { get; set; }
//        public string requestCount { get; set; }
//        public string requestBytes { get; set; }
//        public string responseBytes { get; set; }
//    }

//    public class Producer<T> where T : Message
//    {
//        private readonly ChannelWriter<T> channelWriter;

//        public Producer(ChannelWriter<T> channelWriter)
//        {
//            this.channelWriter = channelWriter;
//        }

//        public async Task PushAsync(T message)
//        {
//            await channelWriter.WriteAsync(message);
//        }

//        public void complete()
//        {
//            channelWriter.Complete();
//        }
//    }

//    public class Consumer<T> where T : Message
//    {
//        private readonly ChannelReader<T> channelReader;

//        public Consumer(ChannelReader<T> channelReader)
//        {
//            this.channelReader = channelReader;
//        }

//        public IAsyncEnumerable<T> PullAllAsync()
//        {
//            return channelReader.ReadAllAsync();
//        }
//    }

//    public abstract class Reader<T> where T : Message
//    {
//        private Producer<T> producer;
//        public void setProducer(Producer<T> producer)
//        {
//            this.producer = producer;
//        }

//        public async Task sendMessageAsync(T message)
//        {
//            await producer.PushAsync(message);
//        }

//        public void sendComplete()
//        {
//            producer.complete();
//        }

//        // main process
//        public async Task process(MemoryStream memoryStream)
//        {
//            await readAndProduce(memoryStream);
//        }

//        // need to be implemented
//        public abstract Task readAndProduce(MemoryStream memoryStream);
//    }

//    public abstract class Writer<T> where T : Message
//    {
//        private Consumer<T> consumer;
//        public void setConsumer(Consumer<T> consumer)
//        {
//            this.consumer = consumer;
//        }

//        // main process
//        public async Task process()
//        {
//            await foreach (var message in consumer.PullAllAsync())
//            {
//                consumeAndWrite(message);
//            }
//        }
//        // need to be implemented
//        public abstract void consumeAndWrite(T message);
//    }

//    public class myReader: Reader<TenantAsn>
//    {
//        // 用于保存数据分片时只截取了一半的记录
//        private Hashtable partialRecordTable = Hashtable.Synchronized(new Hashtable());

//        public override async Task readAndProduce(MemoryStream memoryStream)
//        {
//            string stream2string = Encoding.ASCII.GetString(memoryStream.ToArray());
//            //TimeLogger.Log(stream2string);

//            var lines = stream2string.Split("\n").ToList();

//            lines.RemoveAt(0);
//            lines.RemoveAt(lines.Count() - 1);
//            foreach (var line in lines)
//            {
//                string[] splitArray = line.Split(",");
//                try
//                {
//                    await sendMessageAsync(
//                        new TenantAsn
//                        {
//                            tenantId = splitArray[0],
//                            asn = splitArray[1],
//                            requestCount = splitArray[2],
//                            requestBytes = splitArray[3],
//                            responseBytes = splitArray[4]
//                        }
//                    );
//                } catch (Exception e)
//                {
//                    TimeLogger.Log(line + ": " + e.Message);
//                }
//                //sendComplete();
//            }
//        }

//    }

//    public class myWriter : Writer<TenantAsn>
//    {
//        public override void consumeAndWrite(TenantAsn tenantAsn)
//        {
//            CloudBlockBlob outputBlob = getOutputBlob();
//            String s = JsonConvert.SerializeObject(tenantAsn);
//            outputBlob.UploadTextAsync(s);
//        }

//        private CloudBlockBlob getOutputBlob()
//        {
//            string storageConnectionString = System.Environment.GetEnvironmentVariable("AzureWebJobsStorage");
//            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

//            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
//            CloudBlobContainer container = client.GetContainerReference("shdprod");

//            string output = "TenantMapping/TenantAsnMapping_2021-07-31_test.txt";
//            //string output = "TenantMapping/TenantAsnMapping_test.txt";
//            CloudBlockBlob outputBlob = container.GetBlockBlobReference(output);

//            return outputBlob;
//        }
//    }
//}
