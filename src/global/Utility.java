package global;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utility {

	public static String getHashFileName(String name) {
		// TODO Auto-generated method stub

		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		digest.reset();

		digest.update(name.getBytes());
		byte[] bs = digest.digest();

		BigInteger bigInt = new BigInteger(1, bs);
		String hashText = bigInt.toString(16);

		// Zero pad until 32 chars
		while (hashText.length() < 32) {
			hashText = "0" + hashText;
		}

		return hashText;
	}

}
