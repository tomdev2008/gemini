package com.gemini.util;

/**
 * @author Will Wang
 * @version 创建时间：Jun 17, 2014 10:39:33 PM
 * 
 */
public class NumberUtils {
	public static float toFloat(String value, float _default) {
		if (value == null || value.length() == 0)
			return _default;
		return Float.valueOf(value);
	}

	public static int toInt(String value, int _default) {
		if (value == null || value.length() == 0)
			return _default;
		return Integer.valueOf(value);
	}
	
	public static boolean toBoolean(String value, boolean _default) {
		if (value == null || value.length() == 0)
			return _default;
		return Boolean.valueOf(value);
	}

	public static int toInt4Trim(String value, int _default) {
		if (value == null || value.trim().length() == 0)
			return _default;
		try {
			return Integer.valueOf(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return _default;
	}

	public static long toLong(String value, long _default) {
		if (value == null || value.length() == 0)
			return _default;
		return Long.valueOf(value);
	}
}
