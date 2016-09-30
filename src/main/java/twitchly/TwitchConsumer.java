package twitchly;


import com.typesafe.config.Config;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.text.SimpleDateFormat;
import java.util.*;

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


        DataStream<Tuple7<String, String, Long, Integer, Integer, String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new LineSplitter())

                        .keyBy(new KeySelector<Tuple4<String,Integer,String,String>,String>() {
                            public String getKey(Tuple4<String,Integer,String,String> in) { return in.getField(0); }
                        })
                        .timeWindow(Time.seconds(WindowSize), Time.seconds(WindowSlide))
                        .apply(new Tuple7<String, String, Long, Integer, Integer, String,Integer>("","",0L, 0, 0, "",0), new AggregateMessages(), new SlidingWindowFunction());


        CassandraSink.addSink(counts)
                .setQuery("INSERT INTO twitchly.streamStatus (channel, date, time, count, viewers, game, messageLength) values (?, ?, ?, ?, ?, ?, ?);")
                .setClusterBuilder(new ClusterBuilder() {
                    @Override
                    public Cluster buildCluster(Cluster.Builder builder) {
                        return builder.addContactPoint(DNS).build();
                    }
                })
                .build();


        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder().setHost(DNS).setPort(6379).build();

        counts.addSink(new RedisSink<Tuple7<String, String, Long, Integer, Integer, String, Integer>>(redisConf, new ViewerCountMapper()));
        counts.addSink(new RedisSink<Tuple7<String, String, Long, Integer, Integer, String, Integer>>(redisConf, new MessageCountMapper()));
        counts.addSink(new RedisSink<Tuple7<String, String, Long, Integer, Integer, String, Integer>>(redisConf, new EngagementMapper()));
        counts.addSink(new RedisSink<Tuple7<String, String, Long, Integer, Integer, String, Integer>>(redisConf, new SpamMapper()));

        // execute program
        env.execute("Twitch Chat Analysis");
    }

    private static class AggregateMessages
            implements FoldFunction< Tuple4<String,Integer,String,String>, Tuple7<String, String, Long, Integer, Integer, String, Integer> > {

        public Tuple7<String, String, Long, Integer, Integer, String, Integer> fold(Tuple7<String, String, Long, Integer, Integer, String, Integer> acc, Tuple4<String,Integer,String,String> s) {
            String cur0 = acc.getField(0);
            String cur1 = acc.getField(1);
            Long cur2 = acc.getField(2);
            Integer cur3 = acc.getField(3);
            Integer cur4 = s.getField(1);
            String cur5 = s.getField(2);
            Integer cur6 = acc.getField(6);
            Integer messageLength = ((String) s.getField(3)).split("\\s+").length;

            return new Tuple7<String,String,Long,Integer, Integer, String, Integer>(cur0, cur1,cur2,cur3+1,cur4,cur5,cur6+messageLength);
        }
    }

    private static class SlidingWindowFunction
            implements WindowFunction<Tuple7<String, String, Long, Integer, Integer, String, Integer>, Tuple7<String, String,Long, Integer, Integer, String, Integer>, String, TimeWindow> {

        public void apply(String key,
                          TimeWindow window,
                          Iterable<Tuple7<String, String, Long, Integer, Integer, String, Integer>> counts,
                          Collector<Tuple7<String, String, Long, Integer, Integer, String, Integer>> out) {
            Tuple7<String,String, Long,Integer,Integer,String, Integer> next = counts.iterator().next();
            Integer count = next.getField(3);
            Integer viewers = next.getField(4);
            String game = next.getField(5);
            Integer totalLength = next.getField(6);

            Calendar fromMidnight = Calendar.getInstance();
            fromMidnight.set(Calendar.HOUR, 0);
            fromMidnight.set(Calendar.MINUTE, 0);
            fromMidnight.set(Calendar.SECOND, 0);
            fromMidnight.set(Calendar.MILLISECOND, 0);

            out.collect(new Tuple7<String, String, Long, Integer, Integer, String, Integer>(key, new SimpleDateFormat("yyyyMMdd").format(new Date(window.getEnd())), window.getEnd()-fromMidnight.getTimeInMillis(),count, viewers, game, totalLength));
        }
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple4<String,Integer,String,String>> {

        @Override
        public void flatMap(String value, Collector<Tuple4<String,Integer,String,String>> out) {

            JSONObject obj = new JSONObject(value);
            Integer viewers;
            String game, message, channel;
            try
            {
                channel = obj.getString("Channel");
                viewers = obj.getInt("Viewers");
                game = obj.getString("Game");
                message = obj.getString("Message");

            }
            catch(Exception e){
                channel = "Error";
                viewers = 0;
                game = "Error";
                message = "";
            }
            out.collect(new Tuple4<String,Integer,String,String>(channel,viewers,game,message));

        }
    }

    public static class ViewerCountMapper implements RedisMapper<Tuple7<String, String, Long, Integer, Integer, String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD, "ViewerCount");
        }

        @Override
        public String getKeyFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            return data.getField(0);
        }

        @Override
        public String getValueFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            return data.getField(4).toString();
        }
    }

    public static class MessageCountMapper implements RedisMapper<Tuple7<String, String, Long, Integer, Integer, String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD, "MessageCount");
        }

        @Override
        public String getKeyFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            return data.getField(0);
        }

        @Override
        public String getValueFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            return data.getField(3).toString();
        }
    }

    public static class EngagementMapper implements RedisMapper<Tuple7<String, String, Long, Integer, Integer, String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD, "Engagment");
        }

        @Override
        public String getKeyFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            return data.getField(0);
        }

        @Override
        public String getValueFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            Double viewers =  ((Integer) data.getField(4)).doubleValue();
            Double messages = ((Integer) data.getField(3)).doubleValue();
            return Double.toString(Math.floor((messages*2/(viewers/1000))));
        }
    }

    public static class SpamMapper implements RedisMapper<Tuple7<String, String, Long, Integer, Integer, String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.ZADD, "Spam");
        }

        @Override
        public String getKeyFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            return data.getField(0);
        }

        @Override
        public String getValueFromData(Tuple7<String, String, Long, Integer, Integer, String, Integer> data) {
            Double messageLength =  ((Integer) data.getField(6)).doubleValue();
            Double messages = ((Integer) data.getField(3)).doubleValue();
            return Double.toString(Math.floor(messageLength/messages));
        }
    }
}

