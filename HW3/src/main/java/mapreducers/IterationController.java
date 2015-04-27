package mapreducers;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by alabdullahwi on 4/21/2015.
 */

public class IterationController {
    public static class IterationMapper extends Mapper<Text, Text, Text,DoubleWritable> {
        static int row;
        static int col;
        static double val;
        static DoubleWritable valOut = new DoubleWritable();
        static Text keyOut = new Text();
        public void map(Text _key, Text _value, Mapper.Context context) throws IOException, InterruptedException {
            String[] value  = _value.toString().split("\t");
            String matrixID = value[2];
            row      = Integer.valueOf(_key.toString());
            col      = Integer.valueOf(value[0]);
            val      = Double.valueOf(value[1]);
            keyOut.set(matrixID+"\t"+row+"\t"+col);
            valOut.set(val);
            context.write(keyOut,valOut);
        }//map
    }
    public static class IterationReducer extends Reducer<Text, DoubleWritable, Text, BooleanWritable> {

        static Text keyOut = new Text();
        static BooleanWritable valOut = new BooleanWritable();
        static boolean _val= true;

        public void reduce (Text _key, Iterable<DoubleWritable> _vals, Context context) throws IOException, InterruptedException {
            Iterator<DoubleWritable> iterator = _vals.iterator();
            //expect 2 values here
            double comparisonResult = Math.abs(iterator.next().get() - iterator.next().get());
            _val &= (comparisonResult > 0.3);
            keyOut.set(_key.toString());
            valOut.set(_val);
            context.write(keyOut, valOut);
        }  //reduce

    }//reduce wrapper
}


