using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace TestSyncAsync
{
    public static class Program
    {
        const string URL = "mongodb://10.10.10.170:27017";
        const string dbname = "LoadTests";
        const string col = "insertTest";
        const string chars = "1234567890qwertyuiopasdfghjkl;'zxcvnm,.QWERTYUIOPASDFGHJKL:ZCVBNM<>?!@#$%^&*()-=";
        const int count = 100000;
        const int parallel = 500;

        private static readonly Random _ra = new Random();

        private static IMongoCollection<BsonDocument> Collection => new MongoClient(URL).GetDatabase(dbname).GetCollection<BsonDocument>(col);

        public static void Main(string[] args)
        {
            if (args.Contains("sync"))
            {
                Console.WriteLine("Sync run");
                RunSync();
            }
            else if (args.Contains("async"))
            {
                Console.WriteLine("Async run");
                RunAsync().Wait();
            }
            else Console.WriteLine("No load method selected");

            Console.ReadLine();
        }

        private static BsonDocument GenerateDocument()
        {
            var res = new BsonDocument
            {
                ["d1"] = _ra.Next(),
                ["d2"] = _ra.NextDouble(),
                ["s1"] = new string(Enumerable.Repeat(0, _ra.Next(20, 500)).Select(v => chars[_ra.Next(chars.Length)]).ToArray()),
                ["s2"] = new string( Enumerable.Repeat(0, _ra.Next(20, 500)).Select(v => chars[_ra.Next(chars.Length)]).ToArray()),
                ["v3"] = DateTime.UtcNow.AddSeconds(_ra.Next(0, 10000000))
            };

            return res;
        }

        private static void RunSync()
        {
            var sww = new Stopwatch();
            sww.Start();

            Parallel.ForEach(Enumerable.Range(0, count), new ParallelOptions { MaxDegreeOfParallelism = parallel}, (i) =>
            {
                var cll = Collection;
                if (i % 1000 == 0) Console.WriteLine($"R:{i}");
                cll.InsertOne(GenerateDocument());
            });

            sww.Stop();
            Console.WriteLine($"done in {sww.Elapsed}");
        }

        private static async Task RunAsync()
        {
            var smm = new SemaphoreSlim(parallel, parallel);

            var sww = new Stopwatch();
            sww.Start();

            await Task.WhenAll(Enumerable
                .Range(0, count)
                .Select(async (i) =>
                {
                    await smm.WaitAsync();
                    var cll = Collection;
                    if (i % 1000 == 0) Console.WriteLine($"R:{i}");
                    try
                    {
                        await cll.InsertOneAsync(GenerateDocument());
                    }
                    finally
                    {
                        smm.Release();
                    }
                }).ToArray());

            sww.Stop();
            Console.WriteLine($"done in {sww.Elapsed}");
        }
    }
}
