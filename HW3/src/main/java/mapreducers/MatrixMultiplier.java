package mapreducers;

import customkeys.MatrixVectorWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ed on 4/25/15.
 */

public class MatrixMultiplier {

    public static class MultiplierMapper extends Mapper<Text, Text, Text, MatrixVectorWritable> {

        static int d;
        static int m;
        static int n;
        static double[][] V;
        static double[][] U;
        static int row;
        static int col;
        static double val;
        static Text keyOut = new Text();
        static MatrixVectorWritable valOut = new MatrixVectorWritable();

        public void setup(Mapper.Context context) throws IOException, InterruptedException {
            d = Integer.valueOf(context.getConfiguration().get("d"));
            m = Integer.valueOf(context.getConfiguration().get("m"));
            n = Integer.valueOf(context.getConfiguration().get("n"));
        }

        public void map(Text _key, Text _value, Mapper.Context context) throws IOException, InterruptedException {
            String[] value  = _value.toString().split("\t");
            String matrixID = value[2];
            row      = Integer.valueOf(_key.toString());
            col      = Integer.valueOf(value[0]);
            val      = Double.valueOf(value[1]);
            //populate U and V models
            if ("V".equals(matrixID)) {
                if (V == null) {
                    V = new double [n][d];
                }
                V[col-1][row-1] = val;
            }
            else if ("U".equals(matrixID)) {
                if (U == null) {
                    U = new double[m][d];
                }
                U[row-1][col-1] = val;
            }
        }//map


        public void cleanup(Context context) throws IOException, InterruptedException {

            for (int r =0 ; r< m ; r++) {
                for ( int s = 0 ; s < n ; s++){
                    keyOut.set(r+"\t"+s);
                    if (V != null) {
                        valOut.set(V[s]);
                        context.write(keyOut, valOut);
                    }
                    if (U != null) {
                        valOut.set(U[r]);
                        context.write(keyOut,valOut);
                    }
                }
            }
        }
    }

    public static class MultiplierReducer extends Reducer<Text, MatrixVectorWritable, Text, Text> {

        static Text keyOut = new Text();
        static Text valOut = new Text();
        static double out = 0;

        public void reduce (Text _key, Iterable<MatrixVectorWritable> _vals, Context context) throws IOException, InterruptedException {
            String[] key = _key.toString().split("\t");
            int cellRow = Integer.valueOf(key[0]);
            int cellCol = Integer.valueOf(key[1]);
            double calculationResult = calculateCell(_vals);
            assert calculationResult > -1;
            keyOut.set((cellRow+1)+"\t"+(cellCol+1)+"\t"+calculationResult);
            valOut.set("P");
            context.write(keyOut, valOut);
        }  //reduce

        public double calculateCell(Iterable<MatrixVectorWritable> matrices) {
            Iterator<MatrixVectorWritable> iterator = matrices.iterator();
            double[] first= iterator.next().getDoubles();
            double[] second= iterator.next().getDoubles();
            out = 0 ;
            assert first.length == second.length;
            for (int i = 0 ; i < first.length; i++) {
                out += first[i] * second[i];
            }
            return out;
        }
    }//reduce wrapper
}

