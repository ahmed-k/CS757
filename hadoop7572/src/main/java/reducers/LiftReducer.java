package reducers;

import compositekeys.PairKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Ahmed Alabdullah on 3/18/15.
 */
public class LiftReducer extends Reducer<PairKey, IntWritable, PairKey, DoubleWritable> {


    DoubleWritable lift = new DoubleWritable();
    DoubleWritable total = new DoubleWritable();
    DoubleWritable liftOut = new DoubleWritable();
    Integer currentKey = -1;


    @Override
    public void reduce(PairKey key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {

        //unconditional probability
        if (key.getHighID() == -2) {
            if (key.getLowID() == currentKey) {
                lift.set(lift.get() + sumUp(vals));
            }
            else {
                currentKey = key.getLowID();
                lift.set(0);
                lift.set(sumUp(vals));
            }

        }
        //marginal key
        else if (key.getHighID() == -1)  {
            if (key.getLowID() == currentKey) {
                total.set(total.get() + (double) sumUp(vals));
            }
            else {
                currentKey = key.getLowID();
                total.set(0);
                total.set(sumUp(vals));
            }
        }
        else {
            double _relative = sumUp(vals);
            double unrounded = (_relative / total.get()) / ((_relative / lift.get()));
            double rounded = Math.round(unrounded * 10.0) / 10.0;

            if (rounded > 1.5) {
                liftOut.set(rounded);
                context.write(key, liftOut);
            }


        }


    } //reduce


    private double sumUp(Iterable<IntWritable> vals) {
        double retv = 0;
        for (IntWritable val:vals) {
            retv += (double) val.get();
        }
        return retv;
    }

}
