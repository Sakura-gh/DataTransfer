using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataTransfer
{
    class test
    {
        public void main(string []args)
        {
            Console.WriteLine("a");
            Task.Run(() =>
            {
                Thread.Sleep(2000);
                Console.WriteLine("1 begin");
            }).Wait();
            Console.WriteLine("b");
            Task.Run(() =>
            {
                Console.WriteLine("2 begin");
            }).Wait();
            Console.WriteLine("c");
        }
    }
}
