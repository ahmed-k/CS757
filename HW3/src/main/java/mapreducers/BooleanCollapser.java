package mapreducers;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by alabdullahwi on 4/25/2015.
 */
public class BooleanCollapser {
    public static class CollapserMapper extends Mapper<Text, Text, IntWritable,BooleanWritable> {
        static int row;
        static int col;
        static boolean val;
        static IntWritable keyOut = new IntWritable();
        static BooleanWritable valOut = new BooleanWritable();

        public void map(Text _key, Text _value, Mapper.Context context) throws IOException, InterruptedException {
            String[] value  = _value.toString().split("\t");
            val =  Boolean.valueOf(value[value.length-1]);
            keyOut.set(1);
            valOut.set(val);
            context.write(keyOut,valOut);
        }//map
    }
    public static class CollapserReducer extends Reducer<IntWritable, BooleanWritable, Text, BooleanWritable> {

        static Text keyOut = new Text();
        static BooleanWritable valOut = new BooleanWritable();
        static boolean _val= false;

        public void reduce (IntWritable _key, Iterable<BooleanWritable> _vals, Context context) throws IOException, InterruptedException {
            for (BooleanWritable val : _vals) {
                _val |= val.get();
            }
            keyOut.set("SHALL I CONTINUE?");
            valOut.set(_val);
            context.write(keyOut, valOut);
        }  //reduce

    }//reduce wrapper
}
