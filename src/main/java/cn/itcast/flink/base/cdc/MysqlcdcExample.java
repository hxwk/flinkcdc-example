package cn.itcast.flink.base.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Author itcast
 * Date 2022/11/20 13:19
 * Desc TODO
 */
public class MysqlcdcExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sql = "CREATE TABLE source (\n" +
                " id INT NOT NULL,\n" +
                " name STRING,\n" +
                " description STRING,\n" +
                " weight DECIMAL(10,3),\n" +
                " primary key(id) not enforced\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'node1',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'test',\n" +
                " 'scan.incremental.snapshot.enabled' = 'false',\n" +
                " 'debezium.snapshot.mode' = 'schema_only',\n" +
                " 'server-time-zone' = 'Asia/Shanghai',\n" +
                " 'table-name' = 'products'\n" +
                ")";
        tableEnv.executeSql(sql);

        Table tableQuery = tableEnv.sqlQuery("select * from source");

        DataStream<Tuple2<Boolean, Row>> result = tableEnv.toRetractStream(tableQuery, Row.class);
        result.print();

        env.execute();

    }
}
