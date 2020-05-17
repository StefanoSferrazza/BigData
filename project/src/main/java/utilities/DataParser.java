package utilities;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;


@Description(
	    name     = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
	    value    = "_FUNC_( value) : Double the value of numeric argument, " +
	               "Concatinate value to itself for string arguments.",
	    extended = "Example:\n" +
	               "    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
	               "    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
	               "    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
	               "    (returns \"Tim MayTim May\" if the name was \"Tim May\")\n"
	)
	/**
	 * This class is a Generic User Defined Function 
	 * xxxxxxxxxxxxxx
	 */
public class DataParser  extends GenericUDF {

	StringObjectInspector outputOI;
	StringObjectInspector inputOI;
	
	StringObjectInspector ticker;
	PrimitiveObjectInspector open;
	PrimitiveObjectInspector close;
	PrimitiveObjectInspector adj_close;
	PrimitiveObjectInspector low_the;
	PrimitiveObjectInspector high_the;
	PrimitiveObjectInspector volume;
	StringObjectInspector date_ticker;


	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		assert (arguments.length == 1);
		assert(arguments[0].getCategory() == Category.PRIMITIVE);

		inputOI  = (StringObjectInspector)arguments[0];

		
		
		outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return outputOI;
	}



	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		if (arguments.length != 2) return null;

		Object oin1 = arguments[0].get();
		Object oin2 = arguments[1].get();

		if (oin1 == null || oin2 == null) {
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			System.out.println();
			return null;
		}



		return new Text("%");
	}



	@Override
	public String getDisplayString(String[] errorInfo) {
		return "Data parsed";
	}

}
