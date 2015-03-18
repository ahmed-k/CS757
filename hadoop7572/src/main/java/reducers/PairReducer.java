package reducers;

/**
 * Created by alabdullahwi on 3/15/2015.
 */

import compositekeys.PairKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class PairReducer extends Reducer<PairKey, IntWritable, PairKey, IntWritable> {

    private IntWritable sum = new IntWritable();

    @Override
    public void reduce(PairKey key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
        int _sum = 0;
        for (IntWritable val : vals) {
            _sum += val.get();
        }
        sum.set(_sum);
        context.write(key, sum);
    } //reduce

}


