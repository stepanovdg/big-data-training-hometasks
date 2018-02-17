package org.stepanovdg.mapreduce.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class LongestReducerComplex extends Reducer<ComplexIntTextWritable, Text, DescendingIntWritable, Text> {
  private static final Logger logger = Logger.getLogger( LongestReducerComplex.class );
  private static int maxLength = 0;

  @Override
  protected void reduce( ComplexIntTextWritable key, Iterable<Text> values, Context context )
    throws IOException, InterruptedException {
    if ( logger.isDebugEnabled() ) {
      logger.debug( "ReduceRead" + key );
    }

    context.write( key.getIntWritable(), key.getText() );
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
    Iterator<Text> iter = context.getValues().iterator();
    if ( iter instanceof ReduceContext.ValueIterator ) {
      ( (ReduceContext.ValueIterator<Text>) iter ).resetBackupStore();
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
