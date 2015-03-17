package reducers;

import compositekeys.OccurrenceCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

/**
 * Created by alabdullahwi on 3/16/2015.
 */


public class StripeReducer extends Reducer<IntWritable, MapWritable, Text, IntWritable> {

    private OccurrenceCountWritable masterMap = new OccurrenceCountWritable();
    private Text output = new Text();

    @Override
    public void reduce(IntWritable key, Iterable<MapWritable> vals, Context context) throws IOException, InterruptedException {

        for (MapWritable val : vals) {
            masterMap.absorb(val);
        }

        for (Map.Entry<Writable, Writable> e: masterMap.entrySet()) {
            IntWritable oKey = (IntWritable) e.getKey();
            //this discounts repeats
            if (key.get() < oKey.get()) {
                output.set("<" + key + ", " + oKey.toString() + ">");
                IntWritable val = (IntWritable) e.getValue();
                context.write(output, val);
            }
        }

    } //reduce

}