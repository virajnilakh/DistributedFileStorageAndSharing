package gash.router.server;

import java.util.Map;

import redis.clients.jedis.Jedis;

public class DataHandler {
	private static String key = "chunks";
	private static Jedis jedisHandler1 = new Jedis("localhost", 6379);

	public static String[] getChunks(String fileHash) {

		Map<String, String> map1 = jedisHandler1.hgetAll(fileHash);
		String temp1 = map1.get(key);
		return temp1.split(",");

	}
}
