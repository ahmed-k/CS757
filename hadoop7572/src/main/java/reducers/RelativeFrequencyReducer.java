package reducers;

import compositekeys.PairKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Ahmed Alabdullah on 3/18/15.
 */
public class RelativeFrequencyReducer extends Reducer<PairKey, IntWritable, PairKey, DoubleWritable> {


    DoubleWritable total = new DoubleWritable();
    DoubleWritable relative = new DoubleWritable();
    Integer currentKey = -1;


    @Override
    public void reduce(PairKey key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {

        //marginal key
        if (key.getHighID() == -1)  {
            if (key.getLowID() == currentKey) {
                total.set(total.get() + sumUp(vals));
            }
            else {
                currentKey = key.getLowID();
                total.set(0);
                total.set(sumUp(vals));
            }
        }
        else {
            int _relative = sumUp(vals);
            relative.set((double) _relative/total.get());
            context.write(key, relative);
        }


    } //reduce


    private int sumUp(Iterable<IntWritable> vals) {
        int retv = 0;
        for (IntWritable val:vals) {
            retv += val.get();
        }
        return retv;
    }

}
