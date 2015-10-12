/**
 * 
 */
package org.surendra.spark.sql.cassandra.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * @author surendra.singh
 *
 */
public class Product {
	private long id;
	private String ordnr;
	private String classs;
	private String vendor;

	public String getVendor() {
		return vendor;
	}

	public void setVendor(String vendor) {
		this.vendor = vendor;
	}
	
	public static Map<String, String> getFieldMap() {
		Map<String, String> fieldMap = new HashMap<String, String>();
		fieldMap.put("id", "id");
		fieldMap.put("ordnr", "ordnr");
		fieldMap.put("class", "class");
		fieldMap.put("vendor", "vendor");
		return fieldMap;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getClasss() {
		return classs;
	}

	public void setClasss(String classs) {
		this.classs = classs;
	}

	public String getOrdnr() {
		return ordnr;
	}

	public void setOrdnr(String ordnr) {
		this.ordnr = ordnr;
	}
}
