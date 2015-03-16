package compositekeys;

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
public class OccurrenceCountWritable extends MapWritable {


    public void absorb(MapWritable arr)  {
        for (Map.Entry<Writable, Writable> e : arr.entrySet()) {
            Writable key = e.getKey();
            Writable _myVal = this.get(e.getKey());
            if (_myVal != null) {
                IntWritable myVal = (IntWritable) _myVal;
                IntWritable oVal = (IntWritable) e.getValue();
                int my = myVal.get();
                int o = oVal.get();
                IntWritable update = new IntWritable(my+o);
                this.put(key,update);
            }
            else {
                this.put(key, _myVal);
            }

        }
    }


    public void unroll(IntWritable key, Reducer.Context context) throws IOException, InterruptedException {

        Text output = new Text();
        for (Map.Entry<Writable, Writable> e: this.entrySet()) {
            output.set("<" + key + ", "+ ((IntWritable) e.getKey()).toString() + ">");
            IntWritable val = (IntWritable) e.getValue();
            context.write("",val);
        }



    }


}
