package twitchly.Mappers;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


public class SpamMapper implements RedisMapper<Tuple7<String, String, Long, Integer, Integer, String, Integer>> {

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