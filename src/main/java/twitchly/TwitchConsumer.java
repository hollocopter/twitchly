package twitchly;


import com.typesafe.config.Config;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
import org.json.JSONObject;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import com.datastax.driver.core.Cluster;
import com.typesafe.config.ConfigFactory;


public class TwitchConsumer {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        // LOAD CONFIGURATIONS
        Config conf = ConfigFactory.load();
        final String DNS = conf.getString("CASSANDRA_DNS");
        final String Topic = conf.getString("CHAT_TOPIC");
        final Integer WindowSize = conf.getInt("WINDOW_SIZE");
        final Integer WindowSlide = conf.getInt("WINDOW_SLIDE");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        DataStream<String> text = env
                .addSource(new FlinkKafkaConsumer09<>(Topic, new SimpleStringSchema(), properties));


        DataStream<Tuple3<String, Long, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(new KeySelector<String,String>() {
                            public String getKey(String str) { return str; }
                        })
                        .timeWindow(Time.seconds(WindowSize), Time.seconds(WindowSlide))
                        .apply(new Tuple3<String, Long, Integer>("",0L, 0), new MyFoldFunction(), new MyWindowFunction());


        CassandraSink.addSink(counts)
                .setQuery("INSERT INTO test.values (channel, time, count) values (?, ?, ?);")
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    public Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint(DNS).build();
                    }
                })
                .build();


        // execute program
        env.execute("Twitch Chat Analysis");
    }

    private static class MyFoldFunction
            implements FoldFunction<String, Tuple3<String, Long, Integer> > {

        public Tuple3<String, Long, Integer> fold(Tuple3<String, Long, Integer> acc, String s) {
            String cur0 = acc.getField(0);
            Long cur1 = acc.getField(1);
            Integer cur2 = acc.getField(2);
            return new Tuple3<String,Long,Integer>(cur0, cur1,cur2+1);
        }
    }

    private static class MyWindowFunction
            implements WindowFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, String, TimeWindow> {

        public void apply(String key,
                          TimeWindow window,
                          Iterable<Tuple3<String, Long, Integer>> counts,
                          Collector<Tuple3<String, Long, Integer>> out) {
            Integer count = counts.iterator().next().getField(2);
            out.collect(new Tuple3<String, Long, Integer>(key, window.getEnd(),count));
        }
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class LineSplitter implements FlatMapFunction<String, String> {

        @Override
        public void flatMap(String value, Collector<String> out) {

            JSONObject obj = new JSONObject(value);
//			String message = obj.getString("Message");
            String channel = obj.getString("Channel");
//			String user = obj.getString("User");
            out.collect(channel);

        }
    }

    public static final class LineSplitterTuple implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String,Integer>> out) {

            JSONObject obj = new JSONObject(value);

            String channel = obj.getString("Channel");
            Tuple2<String,Integer> output = new Tuple2(channel, 1);

            out.collect(output);

        }
    }
}
