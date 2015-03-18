package reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by alabdullahwi on 3/18/2015.
 */


public class FrequentItemsetReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable sum = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {
        int _sum = 0;
        for (IntWritable val : vals) {
            _sum += val.get();
        }
        sum.set(_sum);
        context.write(key, sum);
    } //reduce

}

