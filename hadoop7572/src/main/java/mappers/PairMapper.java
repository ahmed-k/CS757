package mappers;

import compositekeys.PairKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by alabdullahwi on 3/15/2015.
 */

public class PairMapper extends Mapper<Text, Text, PairKey, IntWritable> {

    private Map<Integer, List<Integer>> temp = new HashMap<Integer, List<Integer>>();
    private IntWritable one = new IntWritable(1);
    private PairKey _key = new PairKey();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Integer userID = new Integer(key.toString());
        String[] vals = value.toString().split("\t");
        if (Double.parseDouble(vals[1]) >= 4) {
            List candidates  = temp.get(userID);
            if (candidates == null) {
                candidates = new ArrayList<Integer>();
            }
            candidates.add(new Integer(vals[0]));
            temp.put(userID, candidates);

        }

    }//map

    public void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, List<Integer>> e : temp.entrySet()) {
            List<Integer> _set = e.getValue();
            Collections.sort(_set);
            Integer [] arr = _set.toArray(new Integer[_set.size()]);
            for (int i = 0 ; i < arr.length-1 ; i++) {
                for (int j = i+1 ; j < arr.length ; j++) {
                    _key.setLowID(arr[i]);
                    _key.setHighID(arr[j]);
                    context.write(_key, one);
                }//for j
            }//for i
        }//for Map Entries
    }//cleanup

}//PairMapper