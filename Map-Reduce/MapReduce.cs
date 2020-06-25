using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;



public class MapReduce
{
    //note: consider when two keys map to same bucket
    private class Bucket
    {
        string key;
        List<string> values;
    }

    private static List<Bucket> buckets;
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
                map(files[workersFile]); 
            }
            else 
            {
                return;   
            }

        }
    }

    public static void Run(string[] args, Action<string> map, Action<string, Func<string, int, string>, int> reduce, int nMapppers, int nReducers, Func<string, int, ulong> partitioner)
    {
        var mappers = splitWork(args, nMapppers, map);
        buckets = new List<Bucket>();
        MapReduce.partitioner = partitioner;
        //TODO: Fix this assingment 
        MapReduce.numPartitions = nReducers;

        foreach (var mapper in mappers)
        {
            mapper.Start();
        }

        foreach (var mapper in mappers)
        {
            mapper.Wait();
        }

        Console.WriteLine("Mappers done working");
        
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

    public static void Emit(string key, string value)
    {
        int bucket = (int)MapReduce.partitioner(key,) ;
    }

    public static string getNext(string key, int pNum)
    {

        return "";
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
