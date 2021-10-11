# Design Doc

> DataTransfer：tiny framework：
>
> - 多线程并发
> - 消息队列channel
> - reader/writer解耦
> - 插件化代码复用

Destination：

~~~c#
// 1. realise the self-define reader and writer
var reader = new myReader();
var writer = new myWriter();
// 2. create the dataTransfer framework, using reader and writer
DataTransfer dataTransfer = new DataTransfer(reader, writer);
// 3. start the data transfer process
dataTransfer.start();
~~~

All you need to do: 

- extend the two abstract class called Reader and Writer, implement the function `readAndProduce()` and `ConsumeAndWrite()`
  - `readAndProduce()`: read from the data source `MemoryStream`, parse the serialized string to different `Message` objects, put them in a `List<Message>`, and send it into channel by `producer.PushAsync(List<Message>)`
  - `ConsumeAndWrite()`: read the `List<Message>` from the channel by `consumer.PullAsync()`, and transform the `Message` objects on specific format, load them into the target data source finally. 
- the reader and writer are business-related, depends on the specific develop senario, so it should be realized by the developer. All other things, like how to deal with the OOM problem, how to use the high-concurrency, will be solved by the framework.

你只需要做：

- 继承并实现两个抽象类`Reader`和`Writer`，其中`Reader`用来从数据源读取数据并传送到channel中，而`Writer`则是用来从channel中读取数据，进行业务逻辑上的处理再写入目标数据源中
- 实际上，`Reader`和`Writer`是业务相关的，需要由业务开发人员进行实现，而其余部分，包括怎么处理大数据量，怎么避免OOM问题，怎么使用高并发来读和写，这些都由框架完成，开发者无需关心



理一下整个过程：

- 我们使用channel在生产者线程和消费者线程之间传输数据，业务方需要传输的具体对象肯定不同，但我们可以定义一个消息父类`Message`，所有的消息对象都必须继承自`Message`，此时整个框架就可以以`Message`作为数据传输的对象(即channel的Producer和Consumer读写的对象)，而不必考虑具体的对象属性

- 开发者继承抽象类`Reader`和`Writer`，并分别实现了两个抽象方法

  -  `readAndProduce()`：Reader从数据源读取`Message`消息数据并借助producer写入channel中
  - `consumeAndWrite()`：Writer借助consumer从channel中读取`Message`消息数据，进行业务逻辑相关的处理，并写入目标数据源中
  - 在`Reader`中创建的每个`Message`对象都被定义为表中的一行，或者源数据的一个基本单元，所有的逻辑处理和转换都放到`Writer`中进行处理
  - 其中producer和consumer是channel的生产者和消费者，它们在抽象类Reader和Writer中被定义，初始化为null，只有当framework启动时才会被真正赋值；注意，你可能会问，开发者在实现Reader和Writer代码时，使用的producer和consumer是null啊，这是不是有问题，但实际上，开发者实现的仅仅是一个代码片段，这个代码片段不会在实现时被执行，因此这个时候它们为null完全没有问题，producer和consumer作为从成员对象只是告诉你将来执行这个代码片段的时候，会有一个已经赋好值的producer和consumer来做消息传递

- 创建`DataTransfer`的framework，并将reader和writer赋值给该框架的成员变量，此时核心业务逻辑代码有了

- 启动`DataTransfer`，此时会先创建channel，然后从channel中得到真正的producer和consumer，赋值到reader和writer中，此时这个核心业务逻辑代码已经具备真正执行的能力了

- 然后再调用`ParallelRead()`和`ParallelWrite()`函数，启动并发读(并发调用reader)和并发写(并发调用writer)，数据传输正式开始

  注意，DataTransfer类本身就有reader和writer这两个成员变量，只不过初始化为null，它们已经被写入`ParallelRead()`和`ParallelWrite()`函数中，用于读和写的核心逻辑，只不过对于还未被具体赋值reader和writer的`DataTransfer`来说，`ParallelRead()`和`ParallelWrite()`函数中的读和写本身是空壳子，这两个函数也只是代码片段而已，只有当用户自定义好了Reader和Writer，并传入`DataTransfer`中了，这两个代码片段才真正具备了可执行的能力

#### Code Sample

##### 核心启动流程

开发者只需要执行下面几步，即可启动整个数据传输框架

~~~c#
// 1. realise the self-define reader and writer
var reader = new myReader();
var writer = new myWriter();
// 2. create the dataTransfer framework, using reader and writer
DataTransfer dataTransfer = new DataTransfer(reader, writer);
// 3. start the data transfer process
dataTransfer.start();
~~~

##### 消息对象设计

框架已经定义好的父类`Message`

~~~c#
public abstract class Message { }
~~~

开发者需要继承`Message`并定义符合自身业务逻辑的消息对象(建议是一行数据表示为一个对象)

~~~c#
public class TenantAsn : Message
{
    public string tenantId { get; set; }
    public string asn { get; set; }
    public string requestCount { get; set; }
    public string requestBytes { get; set; }
    public string responseBytes { get; set; }
}
~~~

##### 消息的生产者和消费者设计

生产者和消费者主要是通过channel来进行`Message`的生产和消费，注意每次push和pull的对象都是List，主要是为了让单个线程拿到时间片后能一次性处理多条消息，避免上下文切换太频繁导致性能消耗，其中生产者和消费者各为业务独立的线程池

生产者：

~~~c#
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
~~~

消费者：

~~~c#
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
~~~

注意：本质上对泛型来说，`List<parent>`和`List<child>`并不存在父类指向子类的关系，而是平级，都是`List<T>`类型，因此我们无法使用`List<Message>`变量参数来接受真正的`List<TenantAsn>`变量

##### Reader/Writer核心业务逻辑设计

框架已经定义好的Reader/Writer抽象类

~~~c#
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
~~~

开发者需要继承Reader/Writer抽象类，并实现具体读和写的业务逻辑

Reader:

~~~c#
public class myReader : Reader {
    public override void readAndProduce(MemoryStream memoryStream) {
        // 1. parse the memoryStream to messages
        // 2. produce the messageList into channel
        // ...
    }
}
~~~

Writer:

~~~c#
public class myWriter : Writer {
    public override void consumeAndWrite() {
        // 1. consume the messageList from channel
        // 2. deal the messages depends on the business-related senario
        // 3. write the dealed objects into target data source
        // ...
    }
}
~~~

##### DataTransfer架构设计

~~~c#
public class DataTransfer {
    private Reader reader;
    private Writer writer;
    private Channel channel;
    private Producer producer;
    private Consumer consumer;
    
    public DataTransfer(reader, writer) {
        this.reader = reader;
        this.writer = writer;
    }
    
    public void start() {
        // 1. 创建channel，获取producer和consumer
        this.channel = initChannel();
        this.producer = new Producer(this.channel.Writer);
        this.consumer = new Consumer(this.channel.Reader);
        // 2. 将producer和consumer赋值给reader和writer，使其变为可执行代码
        this.reader.setProducer(this.producer);
        this.writer.setConsumer(this.consumer);
        // 3. 执行并发读和写的线程池(起多个线程分别执行reader和writer的核心业务)
        await ParallelRead();
        await ParallelWrite();
    }
    
    private async Task ParallelRead() {
        // 1. init configuration
        // 2. parallel execute reader.readAndProduce()
        // ...
    }
    
    private async Task ParallelWrite() {
        // 1. init configuration
		// 2. parallel execute writer.consumeAndWrite()
        // ...
    }
}
~~~





关于`List<Parent>`接收`List<Child>`的问题，有如下几个方案：

- 使用本身就有的协变`IEnumerable<Parent>`，C#提供了**协变**，我们只需要将Producer和Consumer中所有`List<Message>`的函数参数替换成`IEnumerable<Message>`即可，但此时仅能解决参数传入channel的问题，还需要考虑怎么在从channel取出变量时，把它从`IEnumerable<Message>`转换回`List<Child>`

- 自己分别实现用于写入channel的协变`InList<T>`和从channel读出的逆变`OutList<T>`

  当时是这么写的，但感觉有很多问题，放弃了...

  ~~~c#
  public interface IListIn<in T>
  {
      void Add(T t);
  }
  
  public interface IListOut<out T>
  {
      T this[int index] { get; }
      int Count();
  }
  
  public class DTList<T> : IListIn<T>, IListOut<T>
  {
      private List<T> list;
      public T this[int index]
      {
          get
          {
              if (index < 0 || index >= list.Count())
              {
                  throw new ArgumentOutOfRangeException("index out of range.");
              }
              return list[index];
          }
      }
  
      public void Add(T t)
      {
          list.Add(t);
      }
  
      public int Count()
      {
          return list.Count();
      }
  }
  ~~~

- 协变逆变不能同时存在，前面这些方法的转换太烦了，于是换个思路，我确实不能使用`List<Parent>`的指针接收`List<Child>`对象，因为`List<Parent>`和`List<Child>`之间是平级的，不会存在父亲指向子类的情况；但`Parent`是可以接收`Child`对象的啊，我完全可以让`List<Parent>`指针来接收`List<Parent>`对象，只需要将

  ~~~c#
  var tenantAsnList = new List<TenantAsn>();
  ~~~

  改成

  ~~~c#
  var tenantAsnList = new List<Message>();
  ~~~

  然后我的`tenantAsnList.Add(new TenantAsn{...})`照样是可以行得通的

  这个方案应该是性能上代价最低的了，但它在用户体验上代价却很高昂，因为你必须要求用户去创建一个`List<Message>`，而这仅仅是某种口头上的通知，并没有任何强制性措施去保证这件事能够被很好的执行，因而用户很有可能因为不注意直接创建了一个`List<tenantAsn>`，然后就往channel里传了，ok，此时框架就报错了

  为了屏蔽掉这个本身不应该由用户保障的细节，一个折中的方案是，我直接把`List<Message>`以成员变量`MessageList`的形式写在`Reader`中，并告诉用户，不管你创建的对象是啥，你必须往我指定的`MessageList`里塞

##### 异步--Task.WhenAll and Parallel.ForeachAsync

异步函数本身就会去线程池里申请线程执行

case 1: 不会同时并行执行两个操作，它将先执行第一个，然后等待其完成，然后再执行第二个

~~~c#
var a = await FuncAsync();
var b = await FuncAsync();
~~~

case 2: 同时执行这两个操作，但会同步**阻塞**直到它们完成

~~~c#
var a = FuncAsync();
var b = FuncAsync();
Task.WaitAll(new List[] {a, b});
~~~

case 3: 同时执行这两个操作，且异步**等待**这两个操作

~~~c#
var a = FuncAsync();
var b = FuncAsync();
await Task.WhenAll(new List[] {a, b});
~~~

如何理解 **阻塞** 和 **等待** 的区别？

阻塞就是在另外两个任务没有完成之前，当前线程一直cpu空转等在这里

等待就是在另外两个任务没有完成之前，当前线程会放弃cpu资源，从而让其他需要用到执行的线程得到cpu资源执行，直到另外两个任务完成，当前线程会重新进入等待队列进行执行

##### 异步IO不能用parallel

This leads to bugs. Async delegates return `Task` and the Parallel construct doesn't await that task. This means exceptions are unobserved and you can't be sure after the `Parallel.For` invocation has run that all the work has actually been completed（异步返回的是Task，Parallel并不会等待该Task完成，这意味着异常没法被观测，也就没法验证执行parallel.foreach最终执行是否成功）

https://stackoverflow.com/questions/55187460/increase-performance-async-parallel-foreach

需要使用Task.WhenAll() -- 缺点：需要限制并行度

- https://stackoverflow.com/questions/38634376/running-async-methods-in-parallel
- https://devblogs.microsoft.com/pfxteam/implementing-a-simple-foreachasync-part-2/
- https://stackoverflow.com/questions/15136542/parallel-foreach-with-asynchronous-lambda
- https://stackoverflow.com/questions/19284202/how-to-correctly-write-parallel-for-with-async-methods

Task.WhenAll的用法：对于非async/await的函数`Ff()`，Task.WhenAll()可以保证Task t1和t2均完成后再返回，它相当于是起了一个新的Task去监听t1, t2，并且当t1, t2还在运行时，线程并不会阻塞在Ff()上，而是立即返回

~~~c#
public static void Main(string[] args)
{
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1: " + tid);
    // 也可以在这里把Ff()直接展开，效果是一样的
    // Task t1 = f1();
    // Task t2 = f2();
    // return Task.WhenAll(new List<Task> { t1, t2 });
    Ff();
    string tid2 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 2: " + tid2);
    Thread.Sleep(10000);
}

public static Task Ff()
{
    Task t1 = f1();
    Task t2 = f2();
    return Task.WhenAll(new List<Task> { t1, t2 });
}

public static async Task f1()
{
    await Task.Delay(2000);
    Console.WriteLine("f1");
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 3: " + tid);
}

public static async Task f2()
{
    await Task.Delay(2000);
    Console.WriteLine("f2");
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 4: " + tid);
}
// output：从输出中可以看出来，f1和f2确实是在并行地执行异步任务
thread id 1: 1
thread id 2: 1
f2
f1
thread id 3: 5
thread id 4: 4
~~~

后来有一个想法，如果我们在某个异步函数Ff0()里分别用两个await调用执行两个异步线程f1()和f2()，那这个时候线程的执行顺序是怎么样的？-- 首先main线程进入Ff0()，一直到`await f1()`都是main线程在执行，一旦遇到`await f1()`，main线程会一直执行到最底层的await，直到异步函数真正启动，main线程立即返回，接下来由f1的异步线程1开始执行，f1()执行完后，回到Ff0()，在`await f1()`到`await f2()`之间的所有事情都由刚刚f1的异步线程1来完成，一直到`await f2()`，f1创建的异步线程会一直执行到最底层的await，直到异步函数真正启动，此时f1创建的异步函数**结束执行**，由f2创建的异步函数开始接手，它处理完f2()后，回到Ff0()，在`await f2()`后的所有事情都由f2的异步线程来完成

下面给出了每个block是由哪个线程执行的情况，并用字母abcd...标注了执行顺序

~~~
Main() {
	// block 0  -- thread 1  (a)
	F();        -- thread 1  (b)
	// block 00 -- thread 1  (e)
}

async Task F() {
	// block1     -- thread 1        (c)
	await f1();   -- thread 1 and 2
	// block2     -- thread 2        (h)
	await f2();   -- thread 2 and 3
	// block3     -- thread 3        (l)
}

async Task f1() {
	// block before execute async            -- thread 1    (d)
	await Task.Delay(300); // execute async  -- thread 2 start, thread 1 return e (f)
	// block after execute async             -- thread 2    (g)
}

async Task f2() {
	// block before execute async            -- thread 2    (i)
	await Task.Delay(300); // execute async  -- thread 3 start, thread 2 end (j)
	// block after execute async             -- thread 3    (k)
}
~~~

具体测试代码：

~~~C#
public static void Main(string[] args)
{
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1: " + tid);
    Ff0();
    //Ff();
    //Task t1 = f1();
    //Task t2 = f2();
    //Task.WhenAll(new List<Task> { t1, t2 });
    string tid2 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 2: " + tid2);
    Thread.Sleep(3000);
}

public async static Task Ff0()
{
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id a: " + tid);
    await f1();
    string tid2 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id b: " + tid2);
    await f2();
    string tid3 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id c: " + tid3);
}

public static Task Ff()
{
    Task t1 = f1();
    Task t2 = f2();
    return Task.WhenAll(new List<Task> { t1, t2 });
}

public static async Task f1()
{
    string tid0 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 3-0: " + tid0);
    await Task.Delay(300);
    Console.WriteLine("f1");
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 3: " + tid);
}

public static async Task f2()
{
    string tid0 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 4-0: " + tid0);
    await Task.Delay(300);
    Console.WriteLine("f2");
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 4: " + tid);
}
~~~

执行结果：

![image-20210812105946488](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210812105946488.png)

**最终可以采用的方案**可参考博客：https://medium.com/@alex.puiu/parallel-foreach-async-in-c-36756f8ebe62

- 其实我们一直在希望C#能够支持async foreach的特性

##### 一篇还不错的博客

https://zhuanlan.zhihu.com/p/242142417

##### 普通函数调用异步函数的方式

1.在普通函数(比如main函数)里调用异步函数，如果要接收异步函数的返回值，就相当于加了一个wait，必须等到异步函数执行完毕，同步函数才会接着执行，如

~~~c#
string s = Test().Result;
_ = Test().Result;
~~~

具体代码：

~~~c#
public static void Main(String[] args) {
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1: " + tid);
    string s = Test().Result;
    string tid2 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1_1: " + tid2);
    string tid3 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1_2: " + tid3);
}

public static async Task<String> Test()
{
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 4: " + tid);
    string s = await Task.Run(f);
    string tid2 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 5: " + tid2);
    return s;
}

public static string f()
{
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 6: " + tid);
    Thread.Sleep(5000);
    return "test";
}
~~~

![image-20210810202209845](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810202209845.png)



可以看到，在一开始一直到进入Test()函数之后，遇到await之前，都是main线程在执行，遇到await之后，开一个新的线程进入执行f()，注意此时main线程立即返回了，但是它要用到新线程执行完Test()之后返回的值，相当于要wait新线程，而新线程又要sleep(5000)，因此在6输出完之后会卡顿一段时间，知道新线程继续执行完f()和Test()，main线程拿到返回值，才能继续执行后面的语句

2.当然我们会想把任务丢给异步函数之后，main函数自己接着往下走，那么这个时候只要不用异步函数的返回值就好了，一旦启动新线程执行异步函数，main线程就回去继续执行之前的逻辑

~~~c#
Task s = Test();
_ = Test();
Test();
~~~

具体代码：

~~~c#
public static void Main(String[] args) {
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1: " + tid);
    _ = Test();
    string tid2 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1_1: " + tid2);
    string tid3 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 1_2: " + tid3);
}

public static async Task<String> Test()
{
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 4: " + tid);
    string s = await Task.Run(f);
    string tid2 = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 5: " + tid2);
    return s;
}

public static string f()
{
    string tid = Thread.CurrentThread.ManagedThreadId.ToString();
    Console.WriteLine("thread id 6: " + tid);
    Thread.Sleep(5000);
    return "test";
}
~~~

![image-20210810201604927](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810201604927.png)

可以看到，在一开始一直到进入Test()函数之后，遇到await之前，都是main线程在执行，遇到await之后，开一个新的线程进入执行f()，此时main线程不管新线程有没有执行好，当新线程创建好之后就直接立马返回到`_ = Test()`之后继续执行Main函数里的代码了，注意由于新线程被sleep了，而Main线程又被执行完了，因此thread id 5还没有输出main线程就结束了，于是控制台上就没有显示



##### 理解async和await

一般来说在main函数里调用一个普通函数，普通函数再去调用async函数，进入async函数一直到遇到await之前，都是由main线程执行的，一旦遇到最底层的那个await(如果await嵌套，那么main会一直执行到最后一个await之前)，它后面会跟着原生IO的Task Run或者你自己写的Task.Run()，接下来会启动一个新的线程执行await之后的代码，而原来的线程会立即返回到调用链上从后往前第一个非async的函数，继续往下执行，而调用链上所有含await的async函数，理论上await后面的代码都是由新线程来执行了

- https://blog.csdn.net/sinolover/article/details/104254408

##### Task底层：线程池

Task底层使用的是线程池，而不是每new一个task直接new一个线程：

假设我们

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810132754407.png" alt="image-20210810132754407" style="zoom:80%;" />

cpu占用率100%

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810132958233.png" alt="image-20210810132958233" style="zoom:80%;" />

总共8核，使用了20个线程（底层的线程池预先分配好的数量？），基本cpu就跑不动了

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810133125544.png" alt="image-20210810133125544" style="zoom:80%;" />



直接使用Thread起50个线程

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810134304490.png" alt="image-20210810134304490" style="zoom:80%;" />

成功创建了

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810134338265.png" alt="image-20210810134338265" style="zoom:80%;" />

CPU占用率100%

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810134207864.png" alt="image-20210810134207864" style="zoom:80%;" />





C#的task底层就是使用了线程池ThreadPool，我们可以通过设置线程池的最大最小数量来控制Task所能动用的线程数

设置为16(不能小于逻辑处理器数，而逻辑处理器数等于16)

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810140416146.png" alt="image-20210810140416146" style="zoom:80%;" />

果然最终Task能够创建的线程数减少到了16个

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210810140324226.png" alt="image-20210810140324226" style="zoom:80%;" />

task相关的一些文章：

- https://cxyzjd.com/article/WPwalter/85222818
- https://www.cxyzjd.com/article/ljason1993/80696448
- https://www.twblogs.net/a/5ee8202c91b2f851f930513c/?lang=zh-cn

Parallel本质上就是使用Task.Run实现的

##### blob多线程上传

http://www.kangry.net/blog/?type=article&article_id=380

##### 泛型解决参数传递

利用`where T : Message`使Producer/Consumer和Reader/Writer都变成泛型模板类，且指定接收继承自Message的对象，而最终用户实现的myReader/myWriter则是继承自`Reader<TenantAsn>`和`Writer<TenantAsn>`，这样用户只需要在重载的函数里自行创建一个所需对象的List再传输即可，也不用说一定要传一个`List<Message>`，灵活性大大增加

这里主要解决了f(`List<Father>`)不能接收`List<Child>`参数的苦恼

#### problems

超时？是否发送死锁？

<img src="C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210812153429536.png" alt="image-20210812153429536" style="zoom:80%;" />

可以看到，总共四个数据块，download4次都完成了，但parse只完成了其中的两次，仔细观察parse部分的代码：第一部分是下载slice，每次都用新的inputBlob，没有问题；第二部分使用公用的reader，我怀疑是这里产生了竞争和死锁

~~~c#
await getInputBlob().DownloadRangeToStreamAsync(memoryStream, dataSlice.offset, dataSlice.realSize);
await reader.readAndProduceAsync(memoryStream);
~~~



之前的做法是：所有生产者消费者本质上都需要借助用户new出来的reader和writer来实现数据的传输和读取，显然多线程会对reader和writer产生竞争，进而导致死锁，这可能是超时的原因

~~~c#
public DataTransferFramework(Reader<T> reader, Writer<T> writer)
{
    this.reader = reader;
    this.writer = writer;
}
~~~

因此目前的想法是给每个读/写任务都创建一个新的生产者/消费者，那么问题来了：

myReader/myWriter是由用户实现的类，必须继承我提供的Reader/Writer，而每个任务都需要创建新的生产者/消费者，每个生产者都要创建一个新的myReader，然而我并不知道这个myReader具体是什么，该怎么去创建呢？

使用泛型解决问题：

~~~c#
public class DataTransferFramework<T, R, W> 
    where T : Message
    where R : Reader<T>, new()
    where W : Writer<T>, new() 
{
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
}

--- 
    
DataTransferFramework<TenantAsn, myReader, myWriter> dataTransfer = new DataTransferFramework<TenantAsn, myReader, myWriter>();
dataTransfer.start();
~~~

根据实验结果，确实是由于线程竞争reader/writer导致的死锁超时，对每个task新建一个reader/writer后，运行正常

![image-20210812194238453](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210812194238453.png)

并行性能速度低于串行：受到memory限制

- 思考：当数据量达到几十GB乃至几十TB的时候，我们还能用原来的方法吗？脆弱的Azure Function的内存，处理这样的case需要非常长的时间
- 如何一劳永逸地解决memory的限制？不用Azure Function的内存，而是使用消息队列！将所有的存储都放在外部的消息队列存储上！此时我的并行程序理论上就不会再受到memory的限制



数据分片导致部分记录的情况：

- 分片重叠 -- 保证重叠部分大于等于一条记录即可



#### test

纯串行：1min13s左右，cpu利用率低

![image-20210817094738877](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817094738877.png)

![image-20210817095045391](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817095045391.png)

纯并行(10)，无channel：32s左右，cpu利用率高

![image-20210817093925289](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817093925289.png)

![image-20210817094152467](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817094152467.png)

异步并行：无channel，30s左右

![image-20210817102953975](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817102953975.png)

![image-20210817103447667](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817103447667.png)

异步并行：有channel(100)，read 8，write 10，2min26s

![image-20210817104339126](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817104339126.png)

为什么加了channel使读写线程分离之后性能跌了这么多？可以看到，过程中cpu利用率并没有像之前的纯并行和无channel的异步并行一样，持续不断地高，显然由于channel的使用导致某些资源某些线程被阻塞住了

![image-20210817105019220](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817105019220.png)

设置并发度为read=1，write=1，等同于串行，竟然要花费4min14s的时间

![image-20210817105730550](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817105730550.png)

尝试过将writer写成一个同步程序循环读reader的数据，结果更加慢了，明显消费的速度跟不上生产的速度，导致资源被阻塞

调参：read=5，write=10，花费了1min左右

![image-20210817112314698](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817112314698.png)

read=8, write=15，花了58s左右

![image-20210817121144120](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817121144120.png)

整体cpu和memory的占用都比较均衡

![image-20210817121259972](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817121259972.png)

channel无限制(之前都是限制100)，54s

![image-20210817123421936](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817123421936.png)

有channel虽然比纯并行or无channel异步并行的耗时长，但利用channel将reader和writer解耦能够更有利于扩展、读写分离，最重要的是可以独立分别地控制读线程和写线程的数量，进而达到控制内存占用（带channel的内存占用基本在300M左右，不带channel的会上升到1-2GB）

思考：channel的影响其实有两点：

- reader -> channel -> writer的流程耗时肯定会比直接reader->writer要长，但这个性能损耗换来解耦和扩展性是值得的

- 当channel满时，writer消费能力跟不上reader的生产能力，此时reader的异步线程被阻塞住，无法往channel里push数据，那该任务就无法结束，假设10个并行线程都没有被释放，也就无法再去读新的io数据，这也是图中cpu占用率突然降低的原因

  解决方案：1. 提高writer线程数 2. 使用外部消息队列存储，此时channel本身就不会再受到内存的限制

