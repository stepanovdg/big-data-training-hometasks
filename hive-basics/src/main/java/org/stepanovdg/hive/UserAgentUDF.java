package org.stepanovdg.hive;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

/**
 * Created by Dmitriy Stepanov on 27.02.18.
 */
@UDFType( deterministic = false )
public class UserAgentUDF extends GenericUDF {

  @Override public ObjectInspector initialize( ObjectInspector[] arguments ) {
    // Define the field names for the struct<> and their types
    ArrayList<String> structFieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

    // fill struct field names
    structFieldNames.add( "device" );
    structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );
    structFieldNames.add( "os_name" );
    structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );
    structFieldNames.add( "browser" );
    structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );
    structFieldNames.add( "UA" );
    structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableStringObjectInspector );

    StructObjectInspector si = ObjectInspectorFactory.getStandardStructObjectInspector( structFieldNames,
      structFieldObjectInspectors );
    return si;
  }

  @Override public Object evaluate( DeferredObject[] arguments ) throws HiveException {
    if ( arguments == null || arguments.length < 1 ) {
      throw new HiveException( "Arguments are empty" );
    }
    if ( arguments[ 0 ].get() == null ) {
      throw new HiveException( "Arguments contains null instead of user agent string" );
    }

    Object argObj = arguments[ 0 ].get();

    // get argument
    String argument = null;
    if ( argObj instanceof Text ) {
      argument = argObj.toString();
    } else if ( argObj instanceof String ) {
      argument = (String) argObj;
    } else {
      throw new HiveException(
        "Argument is neither a Text nor String, it is a " + argObj.getClass().getCanonicalName() );
    }
    // parse UA string and return struct, which is just an array of objects: Object[]
    return parseUAString( argument );
  }

  private Object parseUAString( String argument ) {
    Object[] result = new Object[ 4 ];
    UserAgent ua = new UserAgent( argument );
    result[ 0 ] = new Text( ua.getOperatingSystem().getDeviceType().getName() );
    result[ 1 ] = new Text( ua.getOperatingSystem().getName() );
    result[ 2 ] = new Text( ua.getBrowser().getGroup().getName() );
    result[ 3 ] = new Text( ua.getBrowser().getBrowserType().getName() );
    return result;
  }

  @Override public String getDisplayString( String[] children ) {
    return "UDF is used to parse User Agent field and create structure from it";
  }
}
