using System;
using System.IO;

namespace WordCount
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Word Count");
            MapReduce.Run(args, Mapper, Reduce, 2, 2, MapReduce.DefaultHashPartitioner);
        }

        static void Mapper(string fileName)
        {
            string text = File.ReadAllText(fileName);
            foreach (var word in text.Split(' ', '\t', '\n', '\r'))
            {
                MapReduce.Emit(word, "1");
            }

        }

        static void Reduce(string key, Func<string, int, string> getNext, int pNum)
        {
            int count = 0;
            string value;
            while ((value = getNext(key, pNum)) != null)
            {
                count++;
            }
            Console.WriteLine("key: " + key + " count: " + count);
        }


    }
}
