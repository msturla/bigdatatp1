package com.bigdata.pinkelephant.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
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
	            Tuple t = (Tuple)it.next();
	            if (t != null && t.size() > 0 && t.get(0) != null) {
	                t.append(n++);
	            }
	            newBag.add(t);
	        }
	        return newBag;
	    } catch (ExecException ee) {
	        throw ee;
	    } catch (Exception e) {
	        int errCode = 2106;
	        String msg = "Error while computing item number in " + this.getClass().getSimpleName();
	        throw new ExecException(msg, errCode, PigException.BUG, e);           
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
