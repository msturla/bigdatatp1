package com.bigdata.pinkelephant.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class JsonSplitter extends EvalFunc<Tuple> {
	
	private static final Field[] EXPECTED_FIELDS = {
		new Field("box_id", DataType.LONG),
		new Field("power", "", DataType.CHARARRAY),
		new Field("channel", "", DataType.CHARARRAY),
		new Field("timestamp", DataType.LONG)
	};
	
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private Schema outputSchema = null;

	@Override
	public Tuple exec(Tuple tuple) throws IOException {
		Object o = tuple.get(0);
		if (!(o instanceof String)) {
			throw new IOException("Got type " + o.getClass().getName() + " expected string");
		}
		String line = (String) o;
		// remove json brackets
		line = line.replace("{", "").replace("}", "");
		// Not very defensive or elegant, but it works with our input
		String[] jsonFields = line.split(",");
		List<Object> tupleObjects = new ArrayList<Object>();
		for(Field field : EXPECTED_FIELDS) {
			boolean found = false;
			for (String jsonField : jsonFields) {
				if (jsonField.contains(field.name)) {
					String[] keyValuePair = jsonField.split(":");
					if (keyValuePair.length != 2) {
						throw new IOException("Malformed json line: " + line);
					} else {
						addField(tupleObjects, keyValuePair[1], field.type);
						found = true;
						break;
					}
				}
			}
			if (!found) {
				if (field.required) {
					throw new IOException(String.format(
							"hey! Required field %s was not found in line: %s.",
							field.name, line));
				} else {
					addField(tupleObjects, field.defaultValue, field.type);
				}
			}
		}
		return tupleFactory.newTuple(tupleObjects);
	}
	
	private void addField(List<Object> tupleObjects, String value, byte type) {
		switch (type) {
		case DataType.LONG: tupleObjects.add(Long.valueOf(value));
		break;
		case DataType.CHARARRAY: tupleObjects.add(value);
		break;
		case DataType.INTEGER: tupleObjects.add(Integer.valueOf(value));
		break;
		}
	}
	
	public Schema outputSchema(Schema input) {
		try {
			if (outputSchema == null) {
				//input schema is always a string, we dont care.
				List<Schema.FieldSchema> fieldSchemas = new ArrayList<Schema.FieldSchema>();
				for (Field field : EXPECTED_FIELDS) {
					fieldSchemas.add(new Schema.FieldSchema(field.name, field.type));
				}
				Schema tupleSchema = new Schema(fieldSchemas);
				outputSchema = tupleSchema;
				//outputSchema = new Schema(new Schema.FieldSchema("vals", tupleSchema, DataType.TUPLE));
			}
			return outputSchema;
		} catch (Exception e) {
			return null;
		}
	}
	
	private static class Field {
		
		private String name;
		private boolean required;
		private String defaultValue;
		private byte type;
		
		// Constructor for optional field
		public Field(String name, String defaultValue, byte type) {
			this.name = name;
			this.required = false;
			this.defaultValue = defaultValue;
			this.type = type;
		}
		
		// constructor for required field
		public Field(String name, byte type) {
			this.required = true;
			this.name = name;
			this.type = type;
		}
	}

}
