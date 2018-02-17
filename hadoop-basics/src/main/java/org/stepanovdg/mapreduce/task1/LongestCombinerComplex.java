package org.stepanovdg.mapreduce.task1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class LongestCombinerComplex
  extends Reducer<ComplexIntTextWritable, NullWritable, ComplexIntTextWritable, NullWritable> {
  private static int maxLength = 0;
  private static final NullWritable nul = NullWritable.get();

  @Override
  protected void reduce( ComplexIntTextWritable key, Iterable<NullWritable> values, Context context )
    throws IOException, InterruptedException {
    context.write( key, nul );
  }

  @Override public void run( Context context ) throws IOException, InterruptedException {
    setup( context );
    try {
      while ( context.nextKey() ) {
        if ( context.getCurrentKey().getInt() == maxLength ) {
          reduce( context.getCurrentKey(), null, context );
        }
        // If a back up store is used, reset it
        resetBackup( context );
      }
    } finally {
      cleanup( context );
    }
  }

  private void resetBackup(
    Context context )
    throws IOException, InterruptedException {
    Iterator<NullWritable> iter = context.getValues().iterator();
    if ( iter instanceof ReduceContext.ValueIterator ) {
      ( (ReduceContext.ValueIterator<NullWritable>) iter ).resetBackupStore();
    }
  }

  @Override protected void setup( Context context ) throws IOException, InterruptedException {
    super.setup( context );
    if ( context.getCurrentKey() == null ) {
      //do first row
      context.nextKey();
      ComplexIntTextWritable currentKey = context.getCurrentKey();
      if ( currentKey != null ) {
        maxLength = currentKey.getInt();
      }
      reduce( currentKey, null, context );
    }
  }
}
