package mappers;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by alabdullahwi on 4/21/2015.
 */

public class BatchGradientDescent {
    public static class BGDMapper extends Mapper<Text, Text, Text, Text> {

        static Text valOut = new Text();
        static Text keyOut = new Text();
        public void map(Text _key, Text _value, Mapper.Context context) throws IOException, InterruptedException {

            String[] value  = _value.toString().split("\t");
            String matrixID = value[2];
            String row      = _key.toString();
            String col      = value[0];
            String val      = value[1];

            //V matrix is needed everywhere we want to predict a U cell
            if (matrixID.equals("V")) {
                String outVal = "V\t"+row+"\t"+col;
                valOut.set(outVal);
                keyOut.set("U\t"+"*");
            }
            //all of U matrix is needed anytime we want to predict a V cell
            //worry about that later
            else if (matrixID.equals("U")) {
                keyOut.set("U"+row);
                valOut.set("U\t"+ val);
            }
            //is original dataset
            else {
                keyOut.set("U"+row);
                valOut.set("O\t"+val);
            }
            context.write(keyOut,valOut);








        }//map
    }

    public static class MatrixMultiplierReducer extends Reducer<Text, Text, Text, DoubleWritable> {


        public void reduce (Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException  //reduce
    }//reduce wrapper
}


