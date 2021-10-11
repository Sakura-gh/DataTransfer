#### DataTransfer

##### Introduction

The DataTransfer Framework is used to **transfer data between any two data sources** and is typically suitable for transferring large amounts of data when your machine has buzus in memory.

It uses the pattern: `framework` + `plugin`

- **Plug-ins only care about reading or writing to the data itself**. Specifically, you need to define a message class that inherits the Reader and Writer classes and implements the specific interfaces for reading and writing data. 
- **The framework will handles all cpu issues and memory issues**. Specifically, cpu issues like multi-thread, concurrent, parallel, asynchronous, and memory issues like data sharding, and message queue.

DataTransfer will provide you a Framework, and you just need to implements the Plugins.

##### Appendix

[Performance-Test-Doc](./docs/Performance-Test-Doc.md)

