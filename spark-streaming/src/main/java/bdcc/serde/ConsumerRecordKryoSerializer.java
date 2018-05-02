package bdcc.serde;

import bdcc.htm.MonitoringRecord;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by Dmitriy Stepanov on 29.03.18.
 */
public class ConsumerRecordKryoSerializer extends Serializer<ConsumerRecord<String, MonitoringRecord>> {

  @Override public void write( Kryo kryo, Output output, ConsumerRecord<String, MonitoringRecord> object ) {
    kryo.writeObject( output, object.partition() );
    kryo.writeObject( output, object.topic() );
    kryo.writeObject( output, object.offset() );
    kryo.writeObject( output, object.key() );
    KafkaJsonMonitoringRecordSerDe kafkaJsonMonitoringRecordSerDe = new KafkaJsonMonitoringRecordSerDe();
    kryo.writeObject( output, kafkaJsonMonitoringRecordSerDe.serialize( null, object.value() ) );
  }

  @Override public ConsumerRecord<String, MonitoringRecord> read( Kryo kryo, Input input,
                                                                  Class<ConsumerRecord<String, MonitoringRecord>>
                                                                    type ) {
    int partition = kryo.readObject( input, int.class );
    String topic = kryo.readObject( input, String.class );
    long offset = kryo.readObject( input, long.class );
    String key = kryo.readObject( input, String.class );
    KafkaJsonMonitoringRecordSerDe kafkaJsonMonitoringRecordSerDe = new KafkaJsonMonitoringRecordSerDe();
    MonitoringRecord value =
      kafkaJsonMonitoringRecordSerDe.deserialize( null, kryo.readObject( input, byte[].class ) );
    return new ConsumerRecord<String, MonitoringRecord>( topic, partition, offset, key, value );
  }
}
