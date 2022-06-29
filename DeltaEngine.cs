using Microsoft.Spark.Extensions.Delta.Tables;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Delta_demo
{
    public class DeltaEngine
    {
        private static SparkSession _spark;
        private string _deltaPath;
        private string _tmpParquetPath = @"./tmp.parquet";
        private string _tmpJsonPath = @"./tmp.json";

        public DeltaEngine(SparkSession spark, string deltaPath)
        {
            _spark = spark;
            _deltaPath = deltaPath;
        }

        public void CreateDeltaLake(MemoryStream parquetStream)
        {
            using (MemoryStream inStream = parquetStream)
            {
                using (Stream outStream = File.OpenWrite(_tmpParquetPath))
                {
                    inStream.CopyTo(outStream);

                }
            }

            var reader = _spark.Read();
            var parquet = reader.Parquet(_tmpParquetPath);
            parquet.Write().Format("delta").Mode("overwrite").Save(_deltaPath);
        }

        public void CreateDeltaLake(string parquetPath)
        {
            var reader = _spark.Read();
            var parquet = reader.Parquet(parquetPath);
            parquet.Write().Format("delta").Mode("overwrite").Save(_deltaPath);
        }

        public DataFrame ReadDeltaTable()
        {
            var table = DeltaTable.ForPath(_deltaPath);
            return table.ToDF();
        }


        public void UpdateOrAddResource(Dictionary<string, object> resource)
        {
            var deltaTable = DeltaTable.ForPath(_deltaPath);
            var schema = deltaTable.ToDF().Schema();
 
            DataFrame newData = CreateDataFrame(resource, schema).As("newData");

            var newdic = new Dictionary<string, string>();

            foreach (var filed in schema.Fields)
            {
                var name = filed.Name;
                newdic.Add(name, "newData." + name);
            }

            deltaTable.As("oldData")
                .Merge(newData, "oldData.id = newData.id")
                .WhenMatched()
                .UpdateExpr(newdic)
                .WhenNotMatched()
                .InsertExpr(newdic)
                .Execute();
        }

        public void DeleteResource(Dictionary<string, object> resource)
        {
            var idValue = resource["id"];
            var table = DeltaTable.ForPath(_deltaPath);
            table.Delete($"id = '{idValue}'");
        }

        public void DeleteData(string condition)
        {
            var table = DeltaTable.ForPath(_deltaPath);
            table.Delete(condition);
        }

        public void AppendResource(Dictionary<string, object> resource)
        {
            var table = DeltaTable.ForPath(_deltaPath);           
            var schema = table.ToDF().Schema();
            DataFrame df = CreateDataFrame(resource, schema);
            var tmp = df.Schema();
            df.Write().Format("delta").Mode("append").Save(_deltaPath);
        }

        public DataFrame CreateDataFrame(Dictionary<string, object> resource, StructType schema = null)
        {
            File.WriteAllText(_tmpJsonPath, JsonConvert.SerializeObject(resource));
            var df = _spark.Read().Schema(schema).Json(_tmpJsonPath);
            return df;

            // _spark.CreateDataFrame();
            /*
            var subschema = new StructType(new List<StructField>()
               {
                new StructField("Name", new StringType()),
                });

             schema = new StructType(new List<StructField>()
               {
                new StructField("Name", new StringType()),
                new StructField("Age", new IntegerType()),
                new StructField("testarray", new ArrayType(new IntegerType())),
                new StructField("teststruct", subschema)
                });
            var data = new List<GenericRow>();
            var subvalues = new Dictionary<string, string>();
            subvalues.Add("Name","name");
            //subvalues.Add(null);
            var array = new List<int> { 1, 2, 3 };
            //subvalues.Add("10");
            //var structValue = new GenericRow(subvalues.ToArray());

            var values = new List<object>();
            values.Add("name");
            values.Add(null);
            values.Add(array.ToArray());
            values.Add(subvalues);
            data.Add(new GenericRow(values.ToArray()));
            return _spark.CreateDataFrame(data, schema);
            
            
            /*
            var values = new List<object>();
            foreach (var field in schema.Fields)
            {
                var name = field.Name;
                var value = resource.GetValueOrDefault(name);

                if (field.DataType is StructType || field.DataType is ArrayType)
                {
                    values.Add(null);
                }
                else
                {
                    values.Add(value);
                }
            }


            var data = new List<GenericRow>
            {
                new GenericRow(values.ToArray())
            };

            return _spark.CreateDataFrame(data, schema);
            */
        }
    }
}
