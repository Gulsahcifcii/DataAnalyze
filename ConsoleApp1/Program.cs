using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// APPROVED
/// CHANGES REQUIRED
/// I added Two methods; a method get this week of the year, other method summarizedSalesData according to week
/// </summary>
namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting...");
            SalesDataHelper salesData = new SalesDataHelper();
            Console.WriteLine("Getting data...");
            System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var items = salesData.GetStreamingSales(SAMPLE_SIZE);
            var summaryItems = Challenge1Solution(items);

            SaveToDatabase(summaryItems);

            stopwatch.Stop();
            Console.WriteLine($"Processing sales data completed. Total time: {stopwatch.Elapsed.TotalSeconds} seconds.");
            Console.ReadKey();
        }

        private const int SAMPLE_SIZE = 1000000;
        private static IEnumerable<SummarizedSalesData> Challenge1Solution(IEnumerable<SalesData> items)
        {
            var batchSize = 4;
            int j = 0, k = 0,n=0 ;
            IEnumerable<List<SalesData>> batched = items
                .Select((Value, Index) => new { Value, Index })
                .GroupBy(p => p.Index / batchSize)
                .Select(g => g.Select(p => p.Value).ToList());
            List<List<SalesData>> h = batched.ToList();
            Task<List<SummarizedSalesData>> task1 = null, task2 = null, task3 = null, task4 = null;

            for (int i = 0; i < 62500; i++)
            {
                task1 = Task.Run(() =>
                {
                    return getSummarizedeData(h[i]);
                });
                j = i;
            }
            for (j = 62500; j < 125000; j++)
            {
                task2 = Task.Run(() =>
               {
                   return getSummarizedeData(h[j]);
               });
                k = j;
            }
            for (k = 125000; k < 187500; k++)
            {
                task3 = Task.Run(() =>
                {
                    return getSummarizedeData(h[k]);
                });
                n = k;
            }
            for (n = 187500; n < 250000 - 1;n++)
                task4 = Task.Run(() =>
                     {
                         return getSummarizedeData(h[n]);
                     });


            List<SummarizedSalesData> results = new List<SummarizedSalesData>();

            for (int i = 0; i < task1.Result.Count - 1; i++)
            {
                results.Add(task1.Result[i]);
                results.Add(task2.Result[i]);
                results.Add(task3.Result[i]);
                results.Add(task4.Result[i]);
            }

            return results;
        }
        /// <summary>
        /// This method return weeek of the year.
        /// </summary>
        /// <param name="salesDate"></param>
        /// <returns></returns>
        private static int getSalesWeekOfYear(string salesDate)
        {
            var cultureInfo = CultureInfo.CurrentCulture;
            return cultureInfo.Calendar.GetWeekOfYear(
                       new DateTime(short.Parse(salesDate.Split("-")[0]),
                           short.Parse(salesDate.Split("-")[1]), short.Parse(salesDate.Split("-")[2])),
                           cultureInfo.DateTimeFormat.CalendarWeekRule,
                           cultureInfo.DateTimeFormat.FirstDayOfWeek + 1);

        }
        /// <summary>
        /// This method return summarizedSalesData according to week
        /// </summary>
        /// <param name="salesDatas"></param>
        /// <returns></returns>

        private static List<SummarizedSalesData> getSummarizedeData(List<SalesData> salesDatas)
        {
            var batchSize = 4;
            IEnumerable<List<SalesData>> batched = salesDatas
                  .Select((Value, Index) => new { Value, Index })
                  .GroupBy(p => p.Index / batchSize)
                  .Select(g => g.Select(p => p.Value).ToList());
            List<List<SalesData>> salesDatasDivide = batched.ToList();
            List<SummarizedSalesData> results = new List<SummarizedSalesData>();
            List<List<SummarizedSalesData>> result = new List<List<SummarizedSalesData>>();
            result.Add(salesDatas.GroupBy(x => new
            {
                x.BrandId,
                x.CompanyId,
                x.ProductId,
                x.StoreId,
                x.SalesDate
            }).Select(y =>
                          new SummarizedSalesData
                          {
                              ProductId = y.Key.ProductId,
                              CompanyId = y.Key.CompanyId,
                              StoreId = y.Key.StoreId,
                              BrandId = y.Key.BrandId,
                              TotalVolume = y.Sum(x => x.Volume),
                              TotalPrice = y.Sum(x => x.Price),
                              WeekNumber = getSalesWeekOfYear(y.Key.SalesDate)
                          }).ToList());
            foreach (var sublist in result)
            {
                foreach (var value in sublist)
                {
                    results.Add(value);
                }
            }
            return results;

        }
        private static void SaveToDatabase(IEnumerable<SummarizedSalesData> items)
        {
            // Assumed database fast insert code is implemented here.
            Console.WriteLine("Writing records to the database...");
            int recordCounter = 0;
            double totalVolume = 0;
            decimal totalPrice = 0;
            foreach (var item in items)
            {
                recordCounter++;
                totalVolume += item.TotalVolume;
                totalPrice += item.TotalPrice;
            }
            Console.WriteLine($"Records saved to the database. Total record count: {recordCounter}, Total Volume: {totalVolume}, Total Price: {totalPrice}");
        }
    }

    public sealed class SalesDataHelper
    {
        public SalesDataHelper()
        {
            _rnd = new Random();
            InitArray(ref _brands, ref _brandCodes, 1, "Brand Text", true);
            InitArray(ref _companies, ref _companyCodes, 1, "Company Name", true);
            InitArray(ref _stores, ref _storeCodes, 1, "Store Name", true);
            InitArray(ref _prodcuts, ref _prodcuts, 2, "Product description", false);
        }

        private Random _rnd;
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public IEnumerable<SalesData> GetStreamingSales(long maxCount)
        {
            long counter = 0;
            while (counter++ < maxCount)
            {
                int brandIndex = _rnd.Next(0, _brands.Length);
                int companyIndex = _rnd.Next(0, _companies.Length);
                int storeIndex = _rnd.Next(0, _stores.Length);
                int productIndex = _rnd.Next(0, _prodcuts.Length);
                var item = new SalesData()
                {
                    Id = counter,
                    BrandId = _brandCodes[brandIndex],
                    BrandName = _brands[brandIndex],
                    CompanyId = companyIndex,
                    CompanyName = _companies[companyIndex],
                    ProductId = productIndex,
                    ProductName = _prodcuts[productIndex],
                    StoreId = _storeCodes[storeIndex],
                    StoreName = _stores[storeIndex],
                    Price = (decimal)(_rnd.NextDouble() * 1000),
                    Volume = Math.Round(_rnd.NextDouble() * 1000, 2),
                    SalesDate = DateTime.Today.AddDays(-1 * _rnd.Next(1, 60)).ToString("yyyy-MM-dd"),
                    // OtherData = new byte[10 * 1024] // 10KB
                };
                //_rnd.NextBytes(item.OtherData);
                yield return item;
            }
        }

        private void InitArray(ref string[] array, ref string[] idArray, int itemCount, string prefix, bool fillIdArray)
        {
            array = new string[itemCount];
            if (fillIdArray)
                idArray = new string[itemCount];
            for (int i = 0; i < itemCount; i++)
            {
                array[i] = $"{prefix} - {i.ToString()}";
                if (fillIdArray)
                {
                    string tmpCode;
                    do
                    {
                        tmpCode = new string(Enumerable.Repeat(chars, 5)
                          .Select(s => s[_rnd.Next(s.Length)]).ToArray());
                    } while (idArray.Contains(tmpCode));
                    idArray[i] = tmpCode;
                }
            }
        }


        private string[] _brands;
        private string[] _brandCodes;
        private string[] _companies;
        private string[] _companyCodes;
        private string[] _stores;
        private string[] _storeCodes;
        private string[] _prodcuts;
    }

    public sealed class SalesData
    {
        public long Id { get; set; }
        public int ProductId { get; set; }
        public string ProductName { get; set; }
        public string StoreId { get; set; }
        public string StoreName { get; set; }
        public string BrandId { get; set; }
        public string BrandName { get; set; }
        public int CompanyId { get; set; }
        public string CompanyName { get; set; }
        public string SalesDate { get; set; }
        public double Volume { get; set; }
        public decimal Price { get; set; }
        public byte[] OtherData { get; set; }
    }

    public sealed class SummarizedSalesData
    {
        public int ProductId { get; set; }
        public int CompanyId { get; set; }
        public string StoreId { get; set; }
        public string BrandId { get; set; }
        public int WeekNumber { get; set; }
        public double TotalVolume { get; set; }
        public decimal TotalPrice { get; set; }
    }
}
