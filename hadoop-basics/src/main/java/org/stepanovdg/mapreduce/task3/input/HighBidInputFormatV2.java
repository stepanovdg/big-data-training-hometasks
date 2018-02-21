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
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 * <p>
 * Key - CityId
 * Value - BID price
 */
public class HighBidInputFormatV2 extends FileInputFormat<IntWritable, IntTextPairWritable> {

  @Override
  protected boolean isSplitable( JobContext context, Path file ) {
    final CompressionCodec codec =
      new CompressionCodecFactory( context.getConfiguration() ).getCodec( file );
    return null == codec || codec instanceof SplittableCompressionCodec;
  }

  @Override
  public RecordReader<IntWritable, IntTextPairWritable> createRecordReader( InputSplit split,
                                                                            TaskAttemptContext context ) {
    context.setStatus( split.toString() );
    return new ImpressionRecordReaderV2( context.getConfiguration() );
  }
}
