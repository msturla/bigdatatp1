package com.bigdata.pinkelephant.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class BagEnumerator extends EvalFunc<DataBag> {
	
	private BagFactory bagFactory = BagFactory.getInstance();
	
	public DataBag exec(Tuple b) throws IOException {
	    try {
	    	DataBag newBag = bagFactory.newDefaultBag();
	        DataBag bag = (DataBag) b.get(0);
	        Iterator<Tuple> it = bag.iterator();
	        long n = 0;
	        while (it.hasNext()) {
	            Tuple t = it.next();
	            if (t != null) {
	                t.append(n++);
	            }
	            newBag.add(t);
	        }
	        return newBag;
	    } catch (Exception e) {
	        throw new RuntimeException(e);         
	    }
	}
	
	public Schema outputSchema(Schema input) {
		try {
			//first get the bag schema
			Schema.FieldSchema schem = input.getField(0);
			for(Schema.FieldSchema fSchema : schem.schema.getFields()) {
				fSchema.schema.add(new Schema.FieldSchema("rank", DataType.LONG));
			}
			return input;
		} catch (Exception e) {
			return null;
		}
	}
}
