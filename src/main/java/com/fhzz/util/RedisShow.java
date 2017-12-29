package com.fhzz.util;

public class RedisShow {

	
	
	
	
	
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		String s1 = "test kafka";
		String s2 = "hello world";
		String s3 = "test spark streaming";
		
		String key = "streaming";
		
		RedisUtil.del(key);
		
		System.out.println(RedisUtil.get(key));
	}
}
