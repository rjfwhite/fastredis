// See https://aka.ms/new-console-template for more information

using System;
using ExampleApp;
using StackExchange.Redis;

class Hello {         
    static void Main(string[] args)
    {
        // new PerformanceTest();
        // new IndexTest();
        new FastRedisClientTest();
    }
}