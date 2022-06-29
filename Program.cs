using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Spark.Extensions.Delta.Tables;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Streaming;
using Microsoft.Spark.Sql.Types;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Delta_demo
{
    class Program
    {
        public static SparkSession Spark; 
        static void Main(string[] args)
        {
            var parquetStream = GetParquetStream("Expected_Patient.parquet");
            var fixture = new DeltaFixture();
            Spark = fixture.SparkFixture.Spark;
            var engine = new DeltaEngine(Spark, @"abfss://fhir@hacktrainagkae4foft2d2fy.dfs.core.windows.net/deltademo/");


            //engine.CreateDataFrame(JsonConvert.DeserializeObject<Dictionary<string, object>>(File.ReadAllText(@"D:\Projects\Delta-demo\Patient.json")));
            //engine.CreateDeltaLake("abfss://fhir@hacktrainagkae4foft2d2fy.dfs.core.windows.net/smallfilestest/");
            // Create
            engine.CreateDeltaLake(parquetStream);
            engine.ReadDeltaTable().Show();

            // Append
            engine.AppendResource(JsonConvert.DeserializeObject<Dictionary<string, object>>(File.ReadAllText(@"D:\Projects\Delta-demo\Patient.json")));
            engine.ReadDeltaTable().Show();

            // Update
            engine.UpdateOrAddResource(JsonConvert.DeserializeObject<Dictionary<string, object>>(File.ReadAllText(@"D:\Projects\Delta-demo\PatientNew.json")));
            engine.ReadDeltaTable().Show();

            // Delete
            engine.DeleteResource(JsonConvert.DeserializeObject<Dictionary<string, object>>(File.ReadAllText(@"D:\Projects\Delta-demo\PatientNew.json")));
            engine.ReadDeltaTable().Show();
        }



        static MemoryStream GetParquetStream(string filePath)
        {
            var expectedResult = new MemoryStream();
            using var file = new FileStream(filePath, FileMode.Open, FileAccess.Read);
            file.CopyTo(expectedResult);
            expectedResult.Position = 0;
            return expectedResult;
        }

        
    }
}
