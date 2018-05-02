package bdcc.serde;

import bdcc.htm.HTMNetwork;
import bdcc.htm.MonitoringRecord;
import bdcc.htm.ResultState;
import com.esotericsoftware.kryo.Kryo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.serializer.KryoRegistrator;

public class SparkKryoHTMRegistrator implements KryoRegistrator {

  public SparkKryoHTMRegistrator() {
  }

  @Override
  public void registerClasses( Kryo kryo ) {
    // the rest HTM.java internal classes support Persistence API (with preSerialize/postDeserialize methods),
    // therefore we'll create the seralizers which will use HTMObjectInput/HTMObjectOutput (wrappers on top of
    // fast-serialization)
    // which WILL call the preSerialize/postDeserialize
    SparkKryoHTMSerializer.registerSerializers( kryo );
    kryo.register( ConsumerRecord.class, new ConsumerRecordKryoSerializer() );
    kryo.register( MonitoringRecord.class );
    kryo.register( ResultState.class );
    //kryo.register( org.apache.spark.streaming.rdd.MapWithStateRDDRecord.class );
    // kryo.register( org.apache.spark.streaming.util.OpenHashMapBasedStateMap.class );
    kryo.register( HTMNetwork.class, new SparkKryoHTMSerializer<>() );
  }
}