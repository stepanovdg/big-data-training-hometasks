package org.stepanovdg.spark.kafka;

import bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.Closeable;

/**
 * Created by Dmitriy Stepanov on 26.03.18.
 */
public interface PartitionContext extends Closeable {
  KafkaProducer<String, MonitoringRecord> getKafkaProducer();
}
