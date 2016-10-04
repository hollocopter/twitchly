package twitchly.Mappers;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class MessageCountMapper implements RedisMapper<Tuple7<String, String, Long, Integer, Integer, String, Integer>> {

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