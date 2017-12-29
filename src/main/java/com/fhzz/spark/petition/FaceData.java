package com.fhzz.spark.petition;

import java.io.Serializable;

public class FaceData implements Serializable {

	private static final long serialVersionUID = 1165547964386045760L;

	private String name = null;
	
	private Integer age = 0;
	
	public FaceData() {}
	
	public FaceData(String name, Integer age) {
		this.name = name;
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}
}