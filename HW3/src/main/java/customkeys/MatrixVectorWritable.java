package customkeys;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

/**
 * Created by ed on 4/25/15.
 */
public class MatrixVectorWritable extends ArrayWritable {

    public MatrixVectorWritable() {
        super(DoubleWritable.class);
    }
    public MatrixVectorWritable(double[] values) {
        super(DoubleWritable.class);
        set(values);
    }

    public void set(double[] values) {
        DoubleWritable[] vals = new DoubleWritable[values.length];
        for (int i = 0 ; i < values.length ; i++) {
            vals[i] = new DoubleWritable(values[i]);
        }
        set(vals);
    }

    public double[] getDoubles() {
        Writable[] _vals = get();
        double[] vals = new double[_vals.length];
        for (int i = 0; i < _vals.length; i++) {
            vals[i] = ((DoubleWritable) _vals[i]).get();
        }
       return vals;
    }
}
