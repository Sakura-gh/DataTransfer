[TOC]

# test result

##### 纯串行

1min13s左右，cpu利用率低

![image-20210817094738877](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817094738877.png)

![image-20210817095045391](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817095045391.png)

##### 纯并行(10)，无channel

32s左右，cpu利用率高

![image-20210817093925289](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817093925289.png)

![image-20210817094152467](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817094152467.png)

##### 异步并行(10)，无channel

30s左右，cpu利用率高

![image-20210817102953975](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817102953975.png)

![image-20210817103447667](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817103447667.png)

##### 异步并行(multi)，有/无channel(100)

有channel，read=8，write=10，2min26s

![image-20210817104339126](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210817104339126.png)

为什么加了channel使读写线程分离之后性能跌了这么多？可以看到，过程中cpu利用率并没有像之前的纯并行和无channel的异步并行一样，持续不断地保持高占用，显然由于channel的使用导致某些资源某些线程被阻塞住了

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

##### 小结和思考

小结：有channel虽然比纯并行、无channel异步并行的耗时长，但利用channel将reader和writer解耦能够更有利于扩展、读写分离，最重要的是可以独立分别地控制读线程和写线程的数量，进而达到控制内存占用（带channel的内存占用基本在300M左右，不带channel的会上升到1-2GB）

思考：channel的影响其实有两点：

- reader -> channel -> writer的流程耗时肯定会比直接reader->writer要长，但这个性能损耗换来解耦和扩展性是值得的，从最终调参结果中可以看出，它还是可以比串行快的

- 当channel满时，writer消费能力跟不上reader的生产能力，此时reader的异步线程被阻塞住，无法往channel里push数据，那该任务就无法结束，假设10个并行线程都没有被释放，也就无法再去读新的io数据，这也是图中cpu占用率突然降低的原因

  解决方案：1. 提高writer线程数 2. 使用外部消息队列存储，此时channel本身就不会再受到内存的限制





串行，30MB，24min

![image-20210824153250638](C:\Users\t-haoge\AppData\Roaming\Typora\typora-user-images\image-20210824153250638.png)
