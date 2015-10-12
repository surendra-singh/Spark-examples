/**
 * 
 */
package org.surendra.spark.util.entity;

import java.io.Serializable;

/**
 * @author surendra.singh
 *
 */
public class User implements Serializable {

	private static final long serialVersionUID = 1L;

	private String name;
	
	private int age;
	
	private String location;

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

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}
}
