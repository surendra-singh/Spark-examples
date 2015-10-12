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
public class Channel {
	private String ordnr;
	private String date;
	private String channel1;
	private String channel2;
	private String source;

	public String getOrdnr() {
		return ordnr;
	}

	public void setOrdnr(String ordnr) {
		this.ordnr = ordnr;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getChannel1() {
		return channel1;
	}

	public void setChannel1(String channel1) {
		this.channel1 = channel1;
	}

	public String getChannel2() {
		return channel2;
	}

	public void setChannel2(String channel2) {
		this.channel2 = channel2;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
	
	public static Map<String, String> getFieldMap() {
		Map<String, String> fieldMap = new HashMap<String, String>();
		fieldMap.put("ordnr", "ordnr");
		fieldMap.put("date", "date");
		fieldMap.put("channel1", "channel1");
		fieldMap.put("channel2", "channel2");
		fieldMap.put("source", "source");
		return fieldMap;
	}
}
