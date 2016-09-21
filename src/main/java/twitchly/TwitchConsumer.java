package twitchly;


import com.typesafe.config.Config;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
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


        DataStream<Tuple5<String, Long, Integer, Integer, String>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())
                        
                        .keyBy(new KeySelector<Tuple3<String,Integer,String>,String>() {
                            public String getKey(Tuple3<String,Integer,String> in) { return in.getField(0); }
                        })
                        .timeWindow(Time.seconds(WindowSize), Time.seconds(WindowSlide))
                        .apply(new Tuple5<String, Long, Integer, Integer, String>("",0L, 0, 0, ""), new MyFoldFunction(), new MyWindowFunction());


        CassandraSink.addSink(counts)
                .setQuery("INSERT INTO test.values2 (channel, time, count, viewers, game) values (?, ?, ?, ?, ?);")
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
            implements FoldFunction< Tuple3<String,Integer,String>, Tuple5<String, Long, Integer, Integer, String> > {

        public Tuple5<String, Long, Integer, Integer, String> fold(Tuple5<String, Long, Integer, Integer, String> acc, Tuple3<String,Integer,String> s) {
            String cur0 = acc.getField(0);
            Long cur1 = acc.getField(1);
            Integer cur2 = acc.getField(2);
            Integer cur3 = s.getField(1);
            String cur4 = s.getField(2);
            return new Tuple5<String,Long,Integer, Integer, String>(cur0, cur1,cur2+1,cur3,cur4);
        }
    }

    private static class MyWindowFunction
            implements WindowFunction<Tuple5<String, Long, Integer, Integer, String>, Tuple5<String, Long, Integer, Integer, String>, String, TimeWindow> {

        public void apply(String key,
                          TimeWindow window,
                          Iterable<Tuple5<String, Long, Integer, Integer, String>> counts,
                          Collector<Tuple5<String, Long, Integer, Integer, String>> out) {
            Tuple5<String,Long,Integer,Integer,String> next = counts.iterator().next();
            Integer count = next.getField(2);
            Integer viewers = next.getField(3);
            String game = next.getField(4);
            out.collect(new Tuple5<String, Long, Integer, Integer, String>(key, window.getEnd(),count, viewers, game));
        }
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple3<String,Integer,String>> {

        @Override
        public void flatMap(String value, Collector<Tuple3<String,Integer,String>> out) {

            JSONObject obj = new JSONObject(value);
            String channel = obj.getString("Channel");
            Integer viewers = obj.getInt("Viewers");
            String game = obj.getString("Game");
            out.collect(new Tuple3<String,Integer,String>(channel,viewers,game));

        }
    }
}
