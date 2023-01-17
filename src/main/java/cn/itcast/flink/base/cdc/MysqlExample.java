package cn.itcast.flink.base.cdc;

/**
 * Author itcast
 * Date 2022/11/20 11:21
 * Desc TODO
 */

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MysqlExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_contracted_route (demand_id bigint,order_shipment int,customer_id bigint,create_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '7315', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'contracted_route', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_driver_intention (demand_id bigint,intention int,customer_id bigint,create_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '7316', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'driver_intention', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS  source_wx_msg_task (demand_id bigint,wl_id varchar(255),create_time timestamp) with(" +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = '172.27.0.48', " +
                " 'server-id' = '7317', " +
                "  'port' = '3306', " +
                "  'username' = 'flink_cdc_ad', " +
                "  'password' = 'jklsek52@=9H', " +
                "  'database-name' = 'ht_user', " +
                "  'table-name' = 'wx_msg_task', " +
                "  'server-time-zone' = 'Asia/Shanghai', " +
                "  'debezium.snapshot.locking.mode' = 'none', " +
                "  'scan.incremental.snapshot.enabled' = 'false', " +
                "  'debezium.snapshot.mode' = 'initial' " +
                ")");
        Table tableQuery = tableEnv.sqlQuery("select demand_id,push_driver_count,high_intent_driver_count,un_reply_driver_count \n" +
                "from \n" +
                "(select a.demand_id,b.wl_id_num as push_driver_count,if(c.high_intention_user is null,0,c.high_intention_user-1) as high_intent_driver_count,if(c.reply_user is null,0,c.reply_user-1) as un_reply_driver_count \n" +
                "from (select demand_id from source_contracted_route where demand_id > 0 and order_shipment in (0,2) and date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') group by demand_id) a \n" +
                "join (select demand_id,count(wl_id) as wl_id_num from source_wx_msg_task where demand_id > 0 and date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') group by demand_id) b \n" +
                "on a.demand_id = b.demand_id left join (select demand_id,count(distinct if(intention in (1,11),customer_id,0)) as high_intention_user,count(distinct if(intention=1,customer_id,0)) as reply_user from source_driver_intention where demand_id > 0 and date_format(create_time,'yyyy-MM-dd') = date_format(current_timestamp,'yyyy-MM-dd') group by demand_id) c \n" +
                "on a.demand_id = c.demand_id) right_table");
        tableEnv.toRetractStream(tableQuery, Row.class).filter(
                new FilterFunction<Tuple2<Boolean, Row>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                        return booleanRowTuple2.f0;
                    }
                }
        ).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
                return booleanRowTuple2.f1;
            }
        }).print();
//                addSink(new MysqlSink());
        env.execute("报价-高意向司机实时表");
    }

    public static class MysqlSink extends RichSinkFunction<Row> {
        PreparedStatement ps;
        private Connection conn;
        private String sql;

        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("初始化数据库连接池");
            /*conn = C3P0Util.getConnection();
            conn.setAutoCommit(false);
            boolean ret = false;
            sql = "update demand_heat_stat set push_driver_count=?,high_intent_driver_count=?,un_reply_driver_count=? " +
                    "where demand_id=?";
            ps = this.conn.prepareStatement(sql);*/
        }

        @Override
        public void close() throws Exception {
            super.close();
            //关闭连接和释放资源
            if (conn != null) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        /**
         * 每条数据的插入都要调用一次 invoke() 方法
         *
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(Row value, Context context) throws Exception {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String now_date= df.format(new Date());
            try {
                String[] value1 = new String[4];
                value1[0]=value.getField(1).toString();
                value1[1]=value.getField(2).toString();
                value1[2]=value.getField(3).toString();
                value1[3]=value.getField(0).toString();
//                C3P0Util.insertOrUpdateData(sql,value1);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            } finally {
                System.out.println("["+now_date+"] :成功地插入了1行数据, "+value.toString());
            }
        }
    }
}