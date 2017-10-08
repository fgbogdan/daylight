package ro.fagadar.daylight;

import com.googlecode.gwt.crypto.client.TripleDesCipher;

public class CryptUtils {

	public static String strMagic; // = 'USERPASS';

	public static String base1;

	public static String base2;

	public String char_A = "A";
	public static String[] PASS;
	public static int passlen = 8;
	


	public final static byte[] GWT_DES_KEY = new byte[] { (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1,
			(byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, };

	
	

	public static void initpass(String passwd) {

		// make sure password is 8 char long.

		if (passwd.length() < 8)
			passwd = passwd + CryptUtils.repeat("A", 8);

		base1 = "11011010";
		base2 = "11101010";
		PASS = new String[passlen];
		for (int i = 0; i < passlen; i++)
			PASS[i] = CryptUtils.asctobin(passwd.substring(i, i + 1));

		base1 = xor(base1, PASS[2]);
		base2 = xor(base2, PASS[4]);
	}

	public static String crypt1(String passwd) {

		initpass("USERPASS");
		String[] pass_temp = new String[passlen];
		String cur_chr;
		String new_chr;
		int cur_pass;
		for (int idx = 0; idx < PASS.length; ++idx)
			pass_temp[idx] = PASS[idx];
		int exp_len = passwd.length();
		String new_expr = "";
		for (int j = 0; j < exp_len; j++) {// encryption/decription loop
			cur_chr = passwd.substring(j, j + 1); // choose next character
			cur_chr = asctobin(cur_chr); // convert it to binary
			cur_pass = (j + 1) % 7 + 1; // select which password char to use

			new_chr = xor(cur_chr, pass_temp[cur_pass - 1]); // perform the
																				// en/de-crypt
			if (base1.substring(0, 1).equals("1")) { // a mechanism to change the
																	// password chars as we go
																	// along
				base1 = ror(base1);
				pass_temp[cur_pass - 1] = rol(pass_temp[cur_pass - 1]);
			}

			if (base2.substring(0, 1).equals("1")) {
				base2 = rol(base2);
				pass_temp[cur_pass - 1] = rol(pass_temp[cur_pass - 1]);
			}

			new_chr = bintoasc(new_chr);
			new_expr = new_expr + new_chr;
		}

		return new_expr;
	}

	public static String asctobin(String sCHAR) {
		char CHAR = sCHAR.charAt(0);
		String bin = "";
		int achar = (int) CHAR;
		int CNT = 128;
		int tmp, tmp1;
		while (CNT >= 1 && achar > 0) {

			tmp = achar % CNT;

			tmp1 = achar / CNT;
			if (tmp1 == 1)
				bin = bin + "1";
			else
				bin = bin + "0";
			achar = tmp;
			CNT = CNT / 2;
		}
		// padr
		if (bin.length() < 8)
			bin = bin + CryptUtils.repeat("0", 8 - bin.length());
		return bin;
	}

	public static String bintoasc(String bin_str) {
		int temp = 0;
		String stemp;
		char x;
		for (int a = 7; a >= 0; a--) {
			stemp = bin_str.substring(7 - a, 8 - a);
			x = stemp.charAt(0);
			temp = (int) (temp + java.lang.Math.pow(2, a) * ((int) x - 48));

		}
		return Character.toString((char) temp);
	}

	public static String xor(String BYTE1, String BYTE2) {
		String k, j;
		String XB = "";
		for (int i = 0; i < 8; i++) {
			j = BYTE1.substring(i, i + 1);
			k = BYTE2.substring(i, i + 1);
			if (!j.equalsIgnoreCase(k))
				XB = XB + "1";
			else
				XB = XB + "0";
		}
		return XB;
	}

	public static String rol(String BYTE1) {
		return BYTE1.substring(1, 8) + BYTE1.substring(0, 1);
	}

	public static String ror(String BYTE1) {
		return BYTE1.substring(7, 8) + BYTE1.substring(0, 7);
	}

	public static String repeat(String source, int occurences) {
		if (occurences == 0)
			return "";
		else
			return source + repeat(source, occurences - 1);
	}
	
	
	/* metoda noua de criptare */
	
	public static String crypt(String source) {
		TripleDesCipher cipher = new TripleDesCipher();
		String destination = "";
		cipher.setKey(GWT_DES_KEY);
		try {
			destination = cipher.encrypt(String.valueOf(source));
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		return destination;
	}

	public static String decrypt(String source) {
		TripleDesCipher cipher = new TripleDesCipher();
		String destination = "";
		cipher.setKey(GWT_DES_KEY);
		try {
			destination = cipher.decrypt(source);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return destination;
	}
	
	
}
