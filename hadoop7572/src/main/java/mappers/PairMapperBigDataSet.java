package mappers;

/**
 * Created by alabdullahwi on 3/16/2015.
 */

import compositekeys.PairKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by alabdullahwi on 3/15/2015.
 */

public class PairMapperBigDataSet extends Mapper<Text, Text, PairKey, IntWritable> {

    private IntWritable one = new IntWritable(1);
    private PairKey _key = new PairKey();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        String[] arr = key.toString().split("_");
        _key.setLowID(new Integer(arr[0]));
        _key.setHighID(new Integer(arr[1]));
        context.write(_key, one);
    }//map

}//PairMapperBigDataSet
