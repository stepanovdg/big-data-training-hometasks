package org.stepanovdg.hive;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.io.Text;

/**
 * Created by Dmitriy Stepanov on 02.03.18.
 */
public class UserAgentUDFTest extends TestCase {

  public void testEvaluate() {
    String input = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
    Text[] result = new Text[ 4 ];
    result[ 0 ] = new Text( "Unknown" );
    result[ 1 ] = new Text( "Unknown" );
    result[ 2 ] = new Text( "Robot/Spider" );
    result[ 3 ] = new Text( "Robot" );
    GenericUDF.DeferredJavaObject deferredJavaObject = new GenericUDF.DeferredJavaObject( input );
    GenericUDF.DeferredJavaObject[] deferredJavaObjects = { deferredJavaObject };
    UserAgentUDF userAgentUDF = new UserAgentUDF();
    try {
      Object[] evaluate = (Object[]) userAgentUDF.evaluate( deferredJavaObjects );
      assertEquals( result[ 0 ].toString(), evaluate[ 0 ].toString() );
      assertEquals( result[ 1 ].toString(), evaluate[ 1 ].toString() );
      assertEquals( result[ 2 ].toString(), evaluate[ 2 ].toString() );
      assertEquals( result[ 3 ].toString(), evaluate[ 3 ].toString() );
    } catch ( HiveException e ) {
      fail( "Exception" + e );
    }
  }


}