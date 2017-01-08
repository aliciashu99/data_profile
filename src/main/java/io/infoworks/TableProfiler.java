package io.infoworks;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by ashu on 12/27/16.
 *
 * Data Profile a table one at a time.
 * Program takes 2 parameters: database name and table name
 * Table level stats is stored in xxx_table_profile Hive table where xxx is the table profiled
 * Column stats is stored in xxx_columns_profile Hive table where xxx is the table profiled
 */

public class TableProfiler
{
    public static void main( String[] args ) {
        String dbName = null, tblName = null;

        if (args.length != 2) {
            System.out.println("Need to provide database name and table name");
            System.out.println("Usage: TableProfiler dbName tblName");
            return;
        } else {
            dbName = args[0];
            tblName = args[1];
        }
/*
        ExecutorService executor = Executors.newFixedThreadPool(1);

        executor.execute(new DataProfile(dbName, tblName));
        executor.shutdown(); */
        //Future<Void> future = executor.submit(new DataProfile(dbName, tblName));
        dataProfile(dbName, tblName);
    }

        private static void dataProfile(String dbName, String tblName) {

            String warehouseLocation = "/Users/ashu/dev/data_profile";
            SparkSession spark = SparkSession
                    .builder()
                    .appName("Java Spark SQL basic example")
                    .config("spark.sql.warehouse.dir", warehouseLocation)
                    .enableHiveSupport()
                    .getOrCreate();

            tblName = dbName + "."+ tblName;
            String tbl_profile_name = tblName + "_table_profile";
            spark.sql("drop table IF EXISTS " + tbl_profile_name);
            spark.sql("CREATE TABLE IF NOT EXISTS " + tbl_profile_name + "(tablename string, rowcount bigint)");
            Dataset<Row> dp = spark.sql("select count(*) from " + tblName);
            Long totals = (Long)dp.first().get(0);

            // Insert table level stats into xxx_table_profile table where xxx is the table being profiled
            String insertTblProfileValues = String.format("insert into %s select \"%s\" as tablename, count(*) from %s",
                    tbl_profile_name, tblName, tblName);
            spark.sql(insertTblProfileValues);

            String col_profile_name = tblName + "_columns_profile";
            spark.sql("drop table IF EXISTS " + col_profile_name);
            spark.sql("CREATE TABLE IF NOT EXISTS " + col_profile_name + "(tablename string, columnname string, type string," +
                    " distinctcount bigint, minval double, maxval double, minlen int, maxlen int, nullcount bigint," +
                    " nullpercent double, emptycount bigint, top10percent double, toptenvalue array<string>)");

            Dataset<Row> describeTbl = spark.sql("describe " + tblName);

            Dataset<String> names = describeTbl.map(new MapFunction<Row, String>() {
                @Override
                public String call(Row row) throws Exception {
                    return row.getString(0);
                }
            }, Encoders.STRING());

            Dataset<String> types = describeTbl.map(new MapFunction<Row, String>() {
                @Override
                public String call(Row row) throws Exception {
                    return row.getString(1);
                }
            }, Encoders.STRING());
            List<Object> colProfileValueList = new ArrayList<>();
            String[] colList = (String[]) names.collect();
            String[] typeList = (String[])types.collect();

            for (int i = 0; i < names.count(); i++) {
                colProfileValueList.clear();
                String type = typeList[i];

                colProfileValueList.add(tblName);
                colProfileValueList.add(colList[i]);
                colProfileValueList.add(type);

                if (type.equalsIgnoreCase("string")) {
                    String selectMinMax = String.format("select min(length(%s)), max(length(%s)), count(distinct %s) from %s",
                            colList[i], colList[i], colList[i], tblName);
                    Dataset<Row> minmax = spark.sql(selectMinMax);

                    processStringTypeForMinMaxLen(minmax, colProfileValueList);
                } else if (type.equalsIgnoreCase("int")) {
                    String selectMinMax = String.format("select min(%s), max(%s), count(distinct %s) from %s",
                            colList[i], colList[i], colList[i], tblName);
                    Dataset<Row> minmax = spark.sql(selectMinMax);

                    processIntTypeForMinMaxVal(minmax, colProfileValueList);
                }

                String selectNullCount = String.format("select count(%s) from %s where %s is null", colList[i], tblName, colList[i]);
                Dataset<Row> nulls = spark.sql(selectNullCount);
                Long nullCount = (Long)nulls.first().get(0);
                double nullPercent = nullCount / totals * 100;
                colProfileValueList.add(nullCount);
                colProfileValueList.add(nullPercent);

                String selectEmptyCount = String.format("select count(%s) from %s where %s=''", colList[i], tblName, colList[i]);
                Dataset<Row> emptycounts = spark.sql(selectEmptyCount);
                Long empct = (Long)emptycounts.first().get(0);
                colProfileValueList.add(empct);

                String selectTop10 = String.format("select %s, count(%s) from %s group by %s order by %s desc limit 10",
                        colList[i], colList[i], tblName, colList[i], colList[i]);
                Dataset<Row> top10Values = spark.sql(selectTop10);

                Dataset<Long> top10Counts = top10Values.map(new MapFunction<Row, Long>() {
                    @Override
                    public Long call(Row row) throws Exception {
                        return row.getLong(1);
                    }
                }, Encoders.LONG());
                long totalTop10 = 0;
                for (Long val : top10Counts.collectAsList()) {
                    totalTop10 += val;
                }
                double top10Percent = totalTop10 / totals * 100;

                colProfileValueList.add(top10Percent);

                StringBuilder arrayStr = new StringBuilder("array(");
                buildTop10ValuesArray(top10Values, arrayStr);
                colProfileValueList.add(arrayStr);

                // Insert column stats into xxx_columns_profile table where xxx is the table being profiled
                String insertColProfileValues = String.format("insert into %s values('%s', '%s', '%s', %d, %f, %f, " +
                                "%d, %d, %d, %f, %d, %f, %s)",
                        col_profile_name, colProfileValueList.get(0), colProfileValueList.get(1),
                        colProfileValueList.get(2), colProfileValueList.get(3), colProfileValueList.get(4),
                        colProfileValueList.get(5), colProfileValueList.get(6), colProfileValueList.get(7),
                        colProfileValueList.get(8), colProfileValueList.get(9), colProfileValueList.get(10),
                        colProfileValueList.get(11), arrayStr);
                spark.sql(insertColProfileValues);
            }
        }

        private static void processIntTypeForMinMaxVal(Dataset<Row> minmax, List<Object> colProfileValueList) {
            Dataset<Double> minct = minmax.map(new MapFunction<Row, Double>() {
                @Override
                public Double call(Row row) throws Exception {
                    Object a = row.get(0);
                    if (a instanceof Integer)
                        return ((Integer) a).doubleValue();
                    else if (a instanceof Long)
                        return ((Long) a).doubleValue();
                    return row.getDouble(0);
                }
            }, Encoders.DOUBLE());
            Dataset<Double> maxct = minmax.map(new MapFunction<Row, Double>() {
                @Override
                public Double call(Row row) throws Exception {
                    Object a = row.get(1);
                    if (a instanceof Integer)
                        return ((Integer) a).doubleValue();
                    else if (a instanceof Long)
                        return ((Long) a).doubleValue();
                    return row.getDouble(1);
                }
            }, Encoders.DOUBLE());
            Long disct = (Long) minmax.first().get(2);
            colProfileValueList.add(disct);
            colProfileValueList.add((double)minct.collectAsList().get(0));
            colProfileValueList.add((double)maxct.collectAsList().get(0));
            colProfileValueList.add(0);
            colProfileValueList.add(0);
        }

        private static void processStringTypeForMinMaxLen(Dataset<Row> minmax, List<Object> colProfileValueList) {
            Integer minLen = (Integer)minmax.first().get(0);
            Integer maxLen = (Integer)minmax.first().get(1);
            Long disct = (Long)minmax.first().get(2);

            colProfileValueList.add(disct);
            colProfileValueList.add(0.0);
            colProfileValueList.add(0.0);
            colProfileValueList.add(minLen);
            colProfileValueList.add(maxLen);
        }

        private static void buildTop10ValuesArray(Dataset<Row> top10, StringBuilder arrayStr) {
            for (int j = 0; j < top10.collectAsList().size(); j++) {
                arrayStr.append("'").append(top10.collectAsList().get(j).get(0)).append("'");
                if (j < top10.collectAsList().size() - 1)
                    arrayStr.append(",");
            }
            arrayStr.append(")");
        }
}

