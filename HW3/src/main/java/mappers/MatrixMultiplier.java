package mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by alabdullahwi on 4/21/2015.
 */
public class MatrixMultiplier {

    public static class MatrixMultiplierMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException {

            int UDim = Integer.valueOf(context.getConfiguration().get("UDim"));
            int VDim = Integer.valueOf(context.getConfiguration().get("VDim"));
            String matrixID = key.toString();
            int dim = -1;
            if (matrixID.equals("U")) {
                String[] u = value.toString().split("\t");
                String row = u[0];
                String col = u[1];
                String val = u[2];
                for (int i = 0; i < dim; i++) {
                    String outputKey = row + "," + i;
                    context.write(outputKey, matrixID + "\t" + row + "\t" + col + "\t" + val);
                }
            } else if (matrixID.equals("V")) {
                String[] v = value.toString().split("\t");
                String row = v[0];
                String col = v[1];
                String val = v[2];
                for (int i = 0; i < dim; i++) {
                    String outputKey = i + "," + col;
                    context.write(outputKey, matrixID + "\t" + row + "\t" + col + "\t" + val);
                }
            }
        }//map
    }

        public static class MatrixMultiplierReducer extends Reducer<Text, Text, Text, DoubleWritable> {


            static int intDim;
            static DoubleWritable result = new DoubleWritable();
            static double[] URow;
            static double[] VCol;

            public void setup(Context context) throws IOException, InterruptedException {
                intDim = Integer.valueOf(context.getConfiguration().get("intDim"));
                URow = new double[intDim];
                VCol = new double[intDim];
            }

            public void reduce (Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException  {
                //populating row and column that will produce the cell
                for (Text val : vals) {
                    String[] arr = val.toString().split("\t");
                    String matrixID = arr[0];
                    int row = Integer.valueOf(arr[1]);
                    int col = Integer.valueOf(arr[2]);
                    double value = Double.valueOf(arr[3]);
                    if (matrixID.equals("U")) {
                        URow[col] = value;
                    }
                    else if (matrixID.equals("V")) {
                        VCol[row] = value;
                    }
                }//for


                //calculating the cell value
                double sum =  0;
                for (int i = 0 ; i< intDim ; i++) {
                    sum += VCol[i] * URow[i];
                }

                result.set(sum);
                context.write(key, result);

            }//reduce
        }//reduce wrapper
    }

