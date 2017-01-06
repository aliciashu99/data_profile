package io.infoworks;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapFunction;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;


public class App
{
    public static void main( String[] args ) {
        String dbName = null, tblName = null;

        if (args.length != 2) {
            System.out.println("Need to provide database name and table name");
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

            spark.sql("create database IF NOT EXISTS " + dbName);
            spark.sql("CREATE TABLE IF NOT EXISTS " + dbName + ".airline (name string, year int, month int, dayOfMonth int) \n" +
                    " ROW FORMAT DELIMITED   \n" +
                    " FIELDS TERMINATED BY ','  \n" +
                    " LINES TERMINATED BY '\n' \n" +
                    " STORED AS TEXTFILE");
            spark.sql("LOAD DATA INPATH '/user/hive/warehouse/alicia.db/airline.csv' OVERWRITE INTO TABLE " + dbName + ".airline");
            tblName = dbName + "."+ tblName;
            String tbl_profile_name = tblName + "_table_profile";
            spark.sql("drop table IF EXISTS " + tbl_profile_name);
            spark.sql("CREATE TABLE IF NOT EXISTS " + tbl_profile_name + "(tablename string, rowcount bigint)");
            Dataset<Row> dp = spark.sql("select count(*) from " + tblName);

            Dataset<Long> totals = dp.map(new MapFunction<Row, Long>() {
                @Override
                public Long call(Row row) throws Exception {
                    return row.getLong(0);
                }
            }, Encoders.LONG());

            spark.sql("insert into " + tbl_profile_name + " select \"" + tblName + "\" as tablename, count(*) from " + tblName);

            String col_profile_name = tblName + "_columns_profile";
            spark.sql("drop table IF EXISTS " + col_profile_name);
            spark.sql("CREATE TABLE IF NOT EXISTS " + col_profile_name + "(tablename string, columnname string, type string," +
                    " distinctcount bigint, minval double, maxval double, minlen int, maxlen int, nullcount bigint, nullpercent double," +
                    " emptycount bigint, top10percent double, toptenvalue array<string>)");

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
            List<Object> valueList = new ArrayList<>();
            String[] colList = (String[]) names.collect();
            String[] typeList = (String[])types.collect();

            for (int i = 0; i < names.count(); i++) {
                valueList.clear();
                String type = typeList[i];

                valueList.add(tblName);
                valueList.add(colList[i]);
                valueList.add(type);

                if (type.equalsIgnoreCase("string")) {
                    Dataset<Row> minmax = spark.sql("select min(length(" + colList[i] + ")), max(length(" + colList[i] + ")), count(distinct "
                            + colList[i] + ") from " + tblName);

                    Dataset<Integer> minct = minmax.map(new MapFunction<Row, Integer>() {
                        @Override
                        public Integer call(Row row) throws Exception {
                            return row.getInt(0);
                        }
                    }, Encoders.INT());
                    Dataset<Integer> maxct = minmax.map(new MapFunction<Row, Integer>() {
                        @Override
                        public Integer call(Row row) throws Exception {
                            return row.getInt(1);
                        }
                    }, Encoders.INT());
                    Dataset<Long> disct = minmax.map(new MapFunction<Row, Long>() {
                        @Override
                        public Long call(Row row) throws Exception {
                            return row.getLong(2);
                        }
                    }, Encoders.LONG());

                    valueList.add(disct.collectAsList().get(0));
                    valueList.add(0);
                    valueList.add(0);
                    valueList.add(minct.collectAsList().get(0));
                    valueList.add(maxct.collectAsList().get(0));
                } else if (type.equalsIgnoreCase("int")) {
                    Dataset<Row> minmax = spark.sql("select min(" + colList[i] + "), max(" + colList[i] + "), count(distinct "
                            + colList[i] + ") from " + tblName);

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
                    Dataset<Long> disct = minmax.map(new MapFunction<Row, Long>() {
                        @Override
                        public Long call(Row row) throws Exception {
                            return row.getLong(2);
                        }
                    }, Encoders.LONG());

                    valueList.add(disct.collectAsList().get(0));
                    valueList.add(minct.collectAsList().get(0));
                    valueList.add(maxct.collectAsList().get(0));
                    valueList.add(0);
                    valueList.add(0);
                }

                Dataset<Row> nulls = spark.sql("select count(" + colList[i] + ") from " + tblName + " where " + colList[i] + " is null");
                Dataset<Long> nullCount = nulls.map(new MapFunction<Row, Long>() {
                    @Override
                    public Long call(Row row) throws Exception {
                        return row.getLong(0);
                    }
                }, Encoders.LONG());
                double nullPercent = nullCount.collectAsList().get(0) / totals.collectAsList().get(0) * 100;

                valueList.add(nullCount.collectAsList().get(0));
                valueList.add(nullPercent);

                Dataset<Row> emptycounts = spark.sql("select count(" + colList[i] + ") from " + tblName + " where " + colList[i] + "=''");
                Dataset<Long> empct = emptycounts.map(new MapFunction<Row, Long>() {
                    @Override
                    public Long call(Row row) throws Exception {
                        return row.getLong(0);
                    }
                }, Encoders.LONG());
                valueList.add(empct.collectAsList().get(0));

                Dataset<Row> top10Values = spark.sql("select " + colList[i] + ", count(" + colList[i] + ") from "
                        + tblName + " group by " + colList[i] + " order by " + colList[i] + " desc limit 10");

                StringBuilder arrayStr = new StringBuilder("array(");
                if (type.equalsIgnoreCase("int")) {
                    Dataset<Integer> top10 = top10Values.map(new MapFunction<Row, Integer>() {
                        @Override
                        public Integer call(Row row) throws Exception {
                            return row.getInt(0);
                        }
                    }, Encoders.INT());

                    for (int j = 0; j < top10.collectAsList().size(); j++) {
                        arrayStr.append("'").append(top10.collectAsList().get(j)).append("'");
                        if (j < top10.collectAsList().size() - 1)
                            arrayStr.append(",");
                    }
                } else if (type.equalsIgnoreCase("string")) {
                    Dataset<String> top10Str = top10Values.map(new MapFunction<Row, String>() {
                        @Override
                        public String call(Row row) throws Exception {
                            return row.getString(0);
                        }
                    }, Encoders.STRING());
                    for (int j = 0; j < top10Str.collectAsList().size(); j++) {
                        arrayStr.append("'").append(top10Str.collectAsList().get(j)).append("'");
                        if (j < top10Str.collectAsList().size() - 1)
                            arrayStr.append(",");
                    }
                }
                arrayStr.append(")");
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
                double top10Percent = totalTop10 / totals.collectAsList().get(0) * 100;

                valueList.add(top10Percent);

                spark.sql("insert into " + col_profile_name + " values(\"" + valueList.get(0) + "\",\""
                        + valueList.get(1) + "\",\"" + valueList.get(2) + "\"," + valueList.get(3) + ","
                        + valueList.get(4) + "," + valueList.get(5) + "," + valueList.get(6) + ","
                        + valueList.get(7) + "," + valueList.get(8) + "," + valueList.get(9) + ","
                        + valueList.get(10) + "," + valueList.get(11) + "," + arrayStr + ")");
            }
        }
}

