package compositekeys;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.Map;

/**
 * Created by alabdullahwi on 3/16/2015.
 */
public class OccurrenceCountWritable extends MapWritable {


    public void absorb(MapWritable arr)  {
        for (Map.Entry<Writable, Writable> e : arr.entrySet()) {
            Writable key = e.getKey();
            Writable _myVal = this.get(e.getKey());
            Writable _oVal = e.getValue();
            if (_myVal != null) {
                IntWritable myVal = (IntWritable) _myVal;
                IntWritable oVal = (IntWritable) _oVal;
                int my = myVal.get();
                int o = oVal.get();
                IntWritable update = new IntWritable(my+o);
                this.put(key,update);
            }
            else {
                this.put(key, _oVal);
            }

        }
    }


}
