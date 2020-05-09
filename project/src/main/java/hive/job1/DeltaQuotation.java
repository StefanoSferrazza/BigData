package hive.job1;

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

import utilities.Utilities;

public class DeltaQuotation  extends GenericUDF {

	PrimitiveObjectInspector inputOI1;
	PrimitiveObjectInspector inputOI2;
	StringObjectInspector outputOI;

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {		//controlla correttezza input ed in caso contrario fallisce
		assert (arguments.length == 2);
		assert(arguments[0].getCategory() == Category.PRIMITIVE);
		assert(arguments[1].getCategory() == Category.PRIMITIVE);

		inputOI1  = (PrimitiveObjectInspector)arguments[0];
		inputOI2  = (PrimitiveObjectInspector)arguments[1];

		assert(inputOI1.getPrimitiveCategory() == PrimitiveCategory.DECIMAL);
		assert(inputOI2.getPrimitiveCategory() == PrimitiveCategory.DECIMAL);

		outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
		return outputOI;
	}



	/**
	 * metodo principale
	 */
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

		float firstClose = (Float) inputOI1.getPrimitiveJavaObject(oin1); 
		float lastClose = (Float) inputOI1.getPrimitiveJavaObject(oin2); 

		float deltaQuotation = ((lastClose - firstClose) / firstClose)*100;
		deltaQuotation = Utilities.truncateToSecondDecimal(deltaQuotation);

		return new Text(deltaQuotation + "%");
	}



	@Override
	public String getDisplayString(String[] children) {
		return "DeltaQuotation computed";
	}

}
