package reducers;

import compositekeys.OccurrenceCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by alabdullahwi on 3/16/2015.
 */


public class StripeReducer extends Reducer<IntWritable, MapWritable,  Text, IntWritable> {

    private OccurrenceCountWritable masterMap = new OccurrenceCountWritable();

    @Override
    public void reduce(IntWritable key, Iterable<MapWritable> vals, Context context) throws IOException, InterruptedException {

        for (MapWritable val : vals) {
            masterMap.absorb(val);
        }

        masterMap.unroll(key, context);

    } //reduce

}