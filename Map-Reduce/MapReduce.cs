using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;



public class MapReduce
{
    //note: consider when two keys map to same bucket
    private class Bucket : IComparable<Bucket>
    {
        public string key;
        public List<string> values;
        public int currValue = 0;
        public int CompareTo(Bucket other)
        {
            return this.key.CompareTo(other.key);
        }
    }

    private static List<List<Bucket>> reducers;
    private static Mutex pLock;
    private static Mutex bucketLock;
    private static List<Task> workers;
    private static Dictionary<string, string> dict;
    static int currFile = 0;
    private static Func<string, int, ulong> partitioner;
    private static int numPartitions;


 

    private static void mapperHelper(Action<string> map, string[] files) 
    {
        //while there is still work to be done map the next document
        //need a way to see when file is done being used

        while(true) 
        {

            //implement lock to grab this file
            int workersFile = -999;

            if (currFile < files.Length)
            { 
                pLock.WaitOne();
                workersFile = currFile;
                currFile++;
                pLock.ReleaseMutex();
                if (workersFile >= files.Length)
                    return;
                map(files[workersFile]); 
            }
            else 
            {
                return;   
            }

        }
    }

    public static long Run(string[] args, Action<string> map, Action<string, Func<string, int, string>, int> reduce, int nMapppers, int nReducers, Func<string, int, ulong> partitioner)
    {
        var mappers = splitWork(args, nMapppers, map);
        pLock = new Mutex();
        bucketLock = new Mutex();
        reducers = new List<List<Bucket>>();
        for (int i = 0; i < nReducers; i++)
            reducers.Add(new List<Bucket>());
        MapReduce.partitioner = partitioner;
        //TODO: Fix this assingment 
        MapReduce.numPartitions = nReducers;
        var mapWatch = Stopwatch.StartNew();
        foreach (var mapper in mappers)
        {
            mapper.Start();
        }
        foreach (var mapper in mappers)
        {
            mapper.Wait();
        }

        mapWatch.Stop();


        var reducerWork = ReducerWork(reducers, reduce);
        var watch = Stopwatch.StartNew();
        foreach (var work in reducerWork)
        {
            work.Start();
        }

        foreach (var work in reducerWork)
        {
            work.Wait();
        }
        watch.Stop();
        Console.WriteLine(nMapppers + " mappers finished in " + mapWatch.ElapsedMilliseconds + " milliseconds");
        Console.WriteLine(nReducers + " reducers finished in " + watch.ElapsedMilliseconds + " milliseconds");
        return mapWatch.ElapsedMilliseconds + watch.ElapsedMilliseconds;
    }

    public static List<Task> splitWork(string[] files, int nMappers, Action<string> map)
    {

        workers = new List<Task>();
        for (int i = 0; i < nMappers; i++) 
        {
            workers.Add( new Task(() => 
            {
                mapperHelper(map, files);         
            }));
        }

        return workers;

    }

    private static void ReduceHelper(List<Bucket> keyValueBucket, Action<string, Func<string, int, string>, int> reduce, int partitionIndex)
    {
        foreach(var key in keyValueBucket)
        {
            reduce(key.key, getNext, partitionIndex);
        }
    }

    private static List<Task> ReducerWork(List<List<Bucket>> reducers, Action<string, Func<string, int, string>, int> reduce)
    {

        workers = new List<Task>();
        foreach (var reducer in reducers)
        {
            workers.Add(new Task(() =>
            {
                ReduceHelper(reducer, reduce, reducers.IndexOf(reducer));
            }));
        }

        return workers;
    }

    public static void Emit(string key, string value)
    {
        // Find out which reducer to assign this key to
        int reducerIndex = (int)MapReduce.partitioner(key,MapReduce.numPartitions);

        bucketLock.WaitOne();
        var bucket = reducers.ElementAt(reducerIndex).Find(x => x.key == key);
        // If we are able to find a bucket that already has our key, just add our value to that bucket
        if (bucket != null)
        {
            bucket.values.Add(value);
        }
        else
        {
            // Since we could not find an existing key bucket for us to drop the value into, initialize a new bucket for that reducer to work on with our value.
            reducers.ElementAt(reducerIndex).Add(new Bucket() { key = key, values = new List<string>() { value } });
        }
        bucketLock.ReleaseMutex();
    }

    public static string getNext(string key, int pNum)
    {
        var bucket = reducers[pNum].Find(x => x.key == key);
        var nextValue = bucket.currValue >= bucket.values.Count ? null: bucket.values[bucket.currValue];
        bucket.currValue++;
        return nextValue;
    }

    public static ulong DefaultHashPartitioner(string key, int numPartitions)
    {
        var chars = key.ToCharArray();
        ulong hash = 5381;
        foreach (var c in chars)
        {
            hash = hash * 33 + c;
        }
        return hash % (ulong)numPartitions;
    }

}
