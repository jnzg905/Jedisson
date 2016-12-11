package org.jedisson.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestObject {

	private String name;
	
	private int age;
	
	private List<String> friends = new ArrayList<>();
	
	private Map<String,TestObject> childen = new HashMap<>();

	public TestObject(){
		
	}
	
	public TestObject(final String name,final int age){
		this.name = name;
		this.age = age;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public List<String> getFriends() {
		return friends;
	}

	public void setFriends(List<String> friends) {
		this.friends = friends;
	}

	public Map<String, TestObject> getChilden() {
		return childen;
	}

	public void setChilden(Map<String, TestObject> childen) {
		this.childen = childen;
	}
	
	
}
