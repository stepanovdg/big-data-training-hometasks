package org.stepanovdg.spark.kafka;

import bdcc.htm.MonitoringRecord;
import bdcc.kafka.KafkaHelper;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Created by Dmitriy Stepanov on 26.03.18.
 */
public class PartitionContextImpl implements PartitionContext {

  private static PartitionContextImpl partitionContext = new PartitionContextImpl();
  private KafkaProducer<String, MonitoringRecord> producer;

  public PartitionContextImpl() {
    this.producer = KafkaHelper.createAnomallyProducer();
    Runtime.getRuntime().addShutdownHook( new Thread( producer::close ) );
  }

  public static PartitionContextImpl getPartitionContext() {
    return partitionContext;
  }

  public static void setPartitionContext( PartitionContextImpl partitionContext ) {
    PartitionContextImpl.partitionContext = partitionContext;
  }

  @Override public KafkaProducer<String, MonitoringRecord> getKafkaProducer() {

    return producer;
  }

  @Override public void close() {
    producer.flush();
    // pull not required as KafkaProducer is thread safe - would have 1 instance per all threads
  }
}
