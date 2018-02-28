package org.stepanovdg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Dmitriy Stepanov on 27.02.18.
 */
public class ImpressionSerDe extends AbstractSerDe {

  public static final String IMPRESSION_SERDE_OUTPUT_SEPARATOR = "impression.serde.output.separator";
  ArrayList<Object> row;
  private ObjectInspector objectInspector;
  private WritableIntObjectInspector writableIntObjectInspector;
  private WritableStringObjectInspector
    writableStringObjectInspector;
  private String outputSeparator;

  public ImpressionSerDe() {
  }

  @Override public void initialize( @Nullable Configuration configuration, Properties properties ) {

    outputSeparator = properties.getProperty( IMPRESSION_SERDE_OUTPUT_SEPARATOR, "\t" );

    ArrayList<String> structFieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();

    // fill struct field names
    structFieldNames.add( "city_id" );
    writableIntObjectInspector = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    structFieldObjectInspectors.add( writableIntObjectInspector );
    structFieldNames.add( "user_agent" );
    writableStringObjectInspector = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    structFieldObjectInspectors.add( writableStringObjectInspector );


    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector( structFieldNames,
      structFieldObjectInspectors );

    this.row = new ArrayList<>( 2 );
    this.row.add( null );
    this.row.add( null );

  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return IntTextPairWritable.class;
  }

  @Override public Writable serialize( Object o, ObjectInspector objectInspector ) {
    StructObjectInspector outputRowOI = (StructObjectInspector) objectInspector;
    List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();

    return new Text( writableIntObjectInspector
      .getPrimitiveJavaObject( outputRowOI.getStructFieldData( o, outputFieldRefs.get( 0 ) ) )
      + outputSeparator + writableStringObjectInspector
      .getPrimitiveJavaObject( outputRowOI.getStructFieldData( o, outputFieldRefs.get( 1 ) ) ) );
  }

  @Override public SerDeStats getSerDeStats() {
    return null;
  }

  @Override public Object deserialize( Writable writable ) {
    IntTextPairWritable wr = (IntTextPairWritable) writable;
    this.row.set( 0, wr.getOne() );
    this.row.set( 1, wr.getTwo() );
    return this.row;
  }

  @Override public ObjectInspector getObjectInspector() {
    return objectInspector;
  }
}
