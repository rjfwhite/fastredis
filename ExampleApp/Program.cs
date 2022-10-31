// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using Core;
using ExampleApp;
using StackExchange.Redis;

// ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("redis-12260.c284.us-east1-2.gce.cloud.redislabs.com:12260", options => options.Password = "vUh5W6oWj4DTyM7poN5CecTGP4BqVoGp");
ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");

Console.WriteLine(redis.IsConnected);

var database = redis.GetDatabase();

var sub = redis.GetSubscriber();

new IndexTest(database, sub);