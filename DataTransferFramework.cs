using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Text;
using System.Linq;
using Newtonsoft.Json;
using System.Collections.Concurrent;
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

        private int sliceNum;

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

        // 使用同一个channel(producer/consumer)传输数据
        private void initChannel()
        {
            bool isBounded = Convert.ToBoolean(Environment.GetEnvironmentVariable("ChannelBounded"));
            if (isBounded)
            {
                int boundLimit = Convert.ToInt32(Environment.GetEnvironmentVariable("ChannelLimit"));
                var channelOptions = new BoundedChannelOptions(boundLimit)
                {
                    FullMode = BoundedChannelFullMode.Wait
                };
                this.channel = Channel.CreateBounded<List<T>>(channelOptions);
            } 
            else
            {
                this.channel = Channel.CreateUnbounded<List<T>>();
            }
        }

        // 每个独立的线程都要申请一个独立的reader，避免加锁抢占资源
        private R getNewReader()
        {
            var r = new R();
            r.setProducer(producer);
            return r;
        }

        // 每个独立的线程都要申请一个独立的writer，避免加锁抢占资源
        private W getNewWriter()
        {
            var w = new W();
            w.setConsumer(this.consumer);
            return w;
        }

        // 任务分片
        private Queue<DataSlice> getDataSlices(CloudBlockBlob inputBlob)
        {
            inputBlob.FetchAttributesAsync().Wait();
            // Buffer Size: buffer MB chunk, recommand to set 1 MB
            int buffer = Convert.ToInt32(Environment.GetEnvironmentVariable("Buffer"));
            int BUFFER_SIZE = buffer * 1024 * 1024;
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
            this.sliceNum = sliceQueue.Count();
            return sliceQueue;
        }

        // parallel + async foreach execute
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

        // main code:
        // parallel + async => read
        private async Task parallelReadAsync()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = CloudBlobUtil.getInputBlob();

            // 2. get data slices
            Queue<DataSlice> sliceQueue = getDataSlices(inputBlob);

            // 3. start read data slices and put them into channel, parallel and async
            await readSlicesAsync(sliceQueue);
        }

        private async Task readSlicesAsync(Queue<DataSlice> dataSlices)
        {
            int readerNum = Convert.ToInt32(Environment.GetEnvironmentVariable("ReaderNumParallelAsync"));
            //TimeLogger.Log("readerNum: " + readerNum);
            await parallelForEachAsync<DataSlice>(dataSlices, readTaskAsync, readerNum);

        }

        private async Task readTaskAsync(DataSlice dataSlice)
        {
            using (var memoryStream = new MemoryStream())
            {
                TimeLogger.startTimeLogger();
                // 1. get the memory stream from input blob
                // 为了避免由于竞争给inputBlob加锁导致的时间损耗，这里每个slice都单独创建一个新的inputBlob
                await CloudBlobUtil.getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize);
                // 2. read and parse memory stream, put it into the channel
                TimeLogger.Log("download slice: " + dataSlice.id);
                //await reader.readAndProduceAsync(memoryStream);
                await getNewReader().readAndProduceAsync(memoryStream);
                TimeLogger.stopTimeLogger("poduce slice: " + dataSlice.id);
            }
        }

        // main code:
        // parallel + async => write
        private async Task parallelWriteAsync()
        {
            int writerNum = Convert.ToInt32(Environment.GetEnvironmentVariable("WriterNumParallelAsync"));
            //TimeLogger.Log("writerNum: " + writerNum);
            await parallelForEachAsync<int>(Enumerable.Range(1, this.sliceNum).ToList(), async (i) =>
            {
                await getNewWriter().consumeAndWriteAsync();
                TimeLogger.Log("upload slice: " + i);
            }, writerNum);
        }

        // test1: 纯串行
        public void sequentialExecute()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = CloudBlobUtil.getInputBlob();

            // 2. get data slices
            Queue<DataSlice> sliceQueue = getDataSlices(inputBlob);

            // 3. get a reader and a writer
            R reader = getNewReader();
            W writer = getNewWriter();

            // 4. sequential read and write data slice
            while (sliceQueue.Count() > 0)
            {
                using (var memoryStream = new MemoryStream())
                {
                    // download data slice stream
                    DataSlice slice = sliceQueue.Dequeue();
                    inputBlob.DownloadRangeToStreamAsync(memoryStream, slice.offset, slice.realSize).Wait();

                    // parse the stream to List and send data to target
                    List<T> messageList = reader.readData(memoryStream);
                    writer.writeData(messageList);

                    // log
                    TimeLogger.Log("slice " + slice.id);
                }
            }
        }

        // test2: 纯并行，无channel
        // 每个线程都需要独立的reader和writer
        public void parallelExecute()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = CloudBlobUtil.getInputBlob();

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
                            // download data slice stream
                            CloudBlobUtil.getInputBlob().DownloadRangeToStreamAsync(memoryStream, slice.offset, slice.realSize).Wait();

                            // parse the stream to List and send data to target
                            List<T> messageList = getNewReader().readData(memoryStream);
                            getNewWriter().writeData(messageList);

                            // log
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

        // test3: 异步并行，无channel
        public void parallelAsyncWithoutChannel()
        {
            // 1. get input blob
            CloudBlockBlob inputBlob = CloudBlobUtil.getInputBlob();

            // 2. get data slices
            Queue<DataSlice> dataSlices = getDataSlices(inputBlob);

            // 3. parallel + async without channel
            parallelForEachAsync<DataSlice>(dataSlices, readAndWriteTask, 10).Wait();
        }

        private async Task readAndWriteTask(DataSlice dataSlice)
        {
            using (var memoryStream = new MemoryStream())
            {
                //TimeLogger.startTimeLogger();
                
                await CloudBlobUtil.getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize);

                // parse the stream to List and send data to target
                List<T> messageList = getNewReader().readData(memoryStream);
                getNewWriter().writeData(messageList);

                // log
                TimeLogger.Log("slice " + dataSlice.id);

                //TimeLogger.stopTimeLogger("slice " + dataSlice.id + " completed");
            }
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

        public async Task consumeAndWriteAsync()
        {
            // 1. consume the messageList from channel
            List<T> messageList = await getMessages();
            // 2. upload
            writeData(messageList);
        }

        // need to be implemented
        public abstract void writeData(List<T> messageList);
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
            String s = JsonConvert.SerializeObject(tenantAsnList);
            CloudBlobUtil.getOutputBlob().UploadTextAsync(s);
            tenantAsnList = null;
            GC.Collect();
        }

    }
}
