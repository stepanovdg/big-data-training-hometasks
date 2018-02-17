package org.stepanovdg.mapreduce.task1;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class LongestReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

 /* @Override
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    try {
      if (context.nextKey()) {
        while (true) {
          Iterator<Text> iter = context.getValues().iterator();
          // If a back up store is used, reset it
          if (iter instanceof ReduceContext.ValueIterator) {
            ((ReduceContext.ValueIterator<Text>) iter).mark();
          }
          for(;iter.hasNext();iter.next()){

          }
          IntWritable key = context.getCurrentKey();
          if (!context.nextKey()) {
            if (iter instanceof ReduceContext.ValueIterator) {
              ((ReduceContext.ValueIterator<Text>) iter).reset();
            }
            for (; iter.hasNext(); ) {
              Text t = iter.next();
              context.write(key, t);
            }
            break;
          }
          if (iter instanceof ReduceContext.ValueIterator) {
            ((ReduceContext.ValueIterator<Text>) iter).resetBackupStore();
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      cleanup(context);
    }
  }*/

  @Override
  protected void reduce(IntWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    MarkableIterator<Text> mitr = new MarkableIterator<Text>(values.iterator());
    mitr.mark();
    while (mitr.hasNext()) {
      mitr.next();
    }
    if (!context.nextKey()) {
      mitr.reset();
      for (; mitr.hasNext(); ) {
        Text t = mitr.next();
        context.write(key, t);
      }
    } else {
      mitr.clearMark();
    }
  }
}
