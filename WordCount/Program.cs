using System;
using Map_Reduce;

namespace WordCount
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Word Count");
            MapReduce.Run();
        }
    }
}
