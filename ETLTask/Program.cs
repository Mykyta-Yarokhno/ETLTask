using CsvHelper;
using ETLTask.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

class Program
{
    public static void Main(string[] args)
    {
        string csvUrl = "https://drive.google.com/uc?export=download&id=1l2ARvh1-tJBqzomww45TrGtIh5j8Vud4"; 

        IEnumerable<TripRecord> records;
        using (var httpClient = new HttpClient())
        {
            var response = httpClient.GetAsync(csvUrl).Result;
            response.EnsureSuccessStatusCode();

            using (var stream = response.Content.ReadAsStream())
            using (var reader = new StreamReader(stream))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                records = csv.GetRecords<TripRecord>().Select(record =>
                {

                    if (string.IsNullOrWhiteSpace(record.passenger_count?.ToString()))
                    {
                        record.passenger_count = 0;
                    }

                    record.store_and_fwd_flag = record.store_and_fwd_flag == "N" ? "No" : "Yes";

                    TimeZoneInfo estTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                    record.tpep_pickup_datetime = TimeZoneInfo.ConvertTimeToUtc(record.tpep_pickup_datetime, estTimeZone);
                    record.tpep_dropoff_datetime = TimeZoneInfo.ConvertTimeToUtc(record.tpep_dropoff_datetime, estTimeZone);

                    return record;
                }).ToList();
            }
        }

        var uniqueRecords = RemoveDuplicatesAndExport(records);

        BulkInsertToSQL(uniqueRecords);
    }

    public static IEnumerable<TripRecord> RemoveDuplicatesAndExport(IEnumerable<TripRecord> records)
    {
        string duplicateFilePath = "CSVFile\\duplicates.csv";

        var groupedRecords = records
            .GroupBy(r => new { r.tpep_pickup_datetime, r.tpep_dropoff_datetime, r.passenger_count });

        var duplicateRecords = groupedRecords
            .Where(g => g.Count() > 1)
            .SelectMany(g => g.Skip(1));

        if (duplicateRecords.Any())
        {
            try
            {
                string directoryPath = Path.GetDirectoryName(duplicateFilePath);
                if (!Directory.Exists(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                }

                using (var writer = new StreamWriter(duplicateFilePath, false))
                using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
                {
                    csv.WriteRecords(duplicateRecords); 
                }

                Console.WriteLine($"Dublicates saved to {duplicateFilePath}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error writing to {duplicateFilePath}: {ex.Message}");
            }
        }
        else
        {
            Console.WriteLine("No duplicates found to write.");
        }

        var uniqueRecords = groupedRecords
            .Select(g => g.First());

        return uniqueRecords;
    }

    static void BulkInsertToSQL(IEnumerable<TripRecord> records)
    {
        string connectionString = "Data Source=DESKTOP-LH6V225\\SQLEXPRESS;Initial Catalog=ETLTask;User ID=sa2;Password=root;MultipleActiveResultSets=True;Trust Server Certificate=True";
        using (var connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (var bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = "TripData";

                bulkCopy.ColumnMappings.Add("tpep_pickup_datetime", "PickupDateTime");
                bulkCopy.ColumnMappings.Add("tpep_dropoff_datetime", "DropoffDateTime");
                bulkCopy.ColumnMappings.Add("passenger_count", "PassengerCount");
                bulkCopy.ColumnMappings.Add("trip_distance", "TripDistance");
                bulkCopy.ColumnMappings.Add("store_and_fwd_flag", "StoreAndFwdFlag");
                bulkCopy.ColumnMappings.Add("PULocationID", "PULocationID");
                bulkCopy.ColumnMappings.Add("DOLocationID", "DOLocationID");    
                bulkCopy.ColumnMappings.Add("fare_amount", "FareAmount");
                bulkCopy.ColumnMappings.Add("tip_amount", "TipAmount");

                var dataTable = ToDataTable(records);
                bulkCopy.WriteToServer(dataTable);
            }
        }
    }

    static DataTable ToDataTable(IEnumerable<TripRecord> data)
    {
        var table = new DataTable();
        table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
        table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
        table.Columns.Add("passenger_count", typeof(int));
        table.Columns.Add("trip_distance", typeof(float));
        table.Columns.Add("store_and_fwd_flag", typeof(string));
        table.Columns.Add("PULocationID", typeof(int));
        table.Columns.Add("DOLocationID", typeof(int));
        table.Columns.Add("fare_amount", typeof(decimal));
        table.Columns.Add("tip_amount", typeof(decimal));

        foreach (var record in data)
        {
            table.Rows.Add(
                record.tpep_pickup_datetime,
                record.tpep_dropoff_datetime,
                record.passenger_count,
                record.trip_distance,
                record.store_and_fwd_flag,
                record.PULocationID,
                record.DOLocationID,
                record.fare_amount,
                record.tip_amount
            );
        }

        return table;
    }
}
