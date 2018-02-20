package org.stepanovdg.mapreduce.task3.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 * <p>
 * Key - CityId
 * Value - BID price
 */
public class HighBidInputFormat extends FileInputFormat<IntWritable, IntWritable> {

  @Override
  protected boolean isSplitable( JobContext context, Path file ) {
    final CompressionCodec codec =
      new CompressionCodecFactory( context.getConfiguration() ).getCodec( file );
    if ( null == codec ) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  @Override
  public RecordReader<IntWritable, IntWritable> createRecordReader( InputSplit split, TaskAttemptContext context ) {
    context.setStatus( split.toString() );
    return new ImpressionRecordReader( context.getConfiguration() );
  }
}
