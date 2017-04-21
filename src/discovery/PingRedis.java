package discovery;

import global.Constants;
import redis.clients.jedis.Jedis;

public class PingRedis {
	public static void main(String args[]){
		Jedis jedisHandler1 = new Jedis(Constants.jedis1, Constants.redisPort);
		Jedis jedisHandler2 = new Jedis(Constants.jedis2, Constants.redisPort);
		Jedis jedisHandler3 = new Jedis(Constants.jedis3, Constants.redisPort);
		try {
			System.out.println(jedisHandler1.ping());

		} catch (Exception e) {
			System.out.println("Connection to redis failed at "+Constants.jedis1);
		}
		try {
			System.out.println(jedisHandler2.ping());
		} catch (Exception e) {
			System.out.println("Connection to redis failed at "+Constants.jedis2);
		}
		try {
			System.out.println(jedisHandler3.ping());
		} catch (Exception e) {
			System.out.println("Connection to redis failed at "+Constants.jedis3);
		}
	}
}
