package com.bigdata.pinkelephant.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class JsonFieldAccess extends EvalFunc<String> {
	
	@Override
	public String exec(Tuple tuple) throws IOException {
		Object o = tuple.get(0);
		if (!(o instanceof String)) {
			throw new IOException("Got type " + o.getClass().getName() + " expected string");
		}
		String line = (String) o;
		Object objectName = tuple.get(1);
		String name = (String) objectName;
		line = line.replace("{", "").replace("}", "");
		// Not very defensive or elegant, but it works with our input
		String[] jsonFields = line.split(",");
		for (String field : jsonFields) {
			if (field.contains(name)) {
				return field.split(":")[1];
			}
		}
		// Not found, return default value
		if (tuple.size() != 3) {
			throw new IOException(
					String.format("Field %s specified as required but not found\n", name));
		} else {
			return (String) tuple.get(2);
		}
	}

}
