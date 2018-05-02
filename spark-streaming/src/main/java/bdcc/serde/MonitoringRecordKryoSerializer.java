package bdcc.serde;

import bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by Dmitriy Stepanov on 29.03.18.
 */
public class MonitoringRecordKryoSerializer extends Serializer<MonitoringRecord> {

  public static final Class<byte[]> byteArrayClass = byte[].class;

  @Override public void write( Kryo kryo, Output output, MonitoringRecord object ) {
    KafkaJsonMonitoringRecordSerDe kafkaJsonMonitoringRecordSerDe = new KafkaJsonMonitoringRecordSerDe();
    kryo.writeObject( output, kafkaJsonMonitoringRecordSerDe.serialize( null, object ) );
  }

  @Override public MonitoringRecord read( Kryo kryo, Input input, Class<MonitoringRecord> type ) {
    KafkaJsonMonitoringRecordSerDe kafkaJsonMonitoringRecordSerDe = new KafkaJsonMonitoringRecordSerDe();
    return kafkaJsonMonitoringRecordSerDe.deserialize( null, kryo.readObject( input, byteArrayClass ) );
  }
}
