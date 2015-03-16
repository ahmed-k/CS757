package mappers;

import compositekeys.PairKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by alabdullahwi on 3/15/2015.
 */
//let's see how this goes

public class PairMapper extends Mapper<Text, Text, PairKey, IntWritable> {

    private Map<Integer, SortedSet<Integer>> temp = new HashMap<Integer, SortedSet<Integer>>();
    private IntWritable one = new IntWritable(1);
    private PairKey _key = new PairKey();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Integer userID = new Integer(key.toString());
        Configuration conf = context.getConfiguration();
        String separator = conf.get("separator");
        String[] vals = value.toString().split(separator);
        String _movieID = vals[0];
        String _rating = vals[1];
        Integer rating = new Integer(_rating);
        Integer movieID = new Integer(_movieID);
        if (rating > 3) {
            SortedSet candidates  = temp.get(userID);
            if (candidates == null) {
                candidates = new TreeSet<Integer>();
            }
            candidates.add(movieID);
            temp.put(userID, candidates);
        }
    }//map

    public void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, SortedSet<Integer>> e : temp.entrySet()) {
            SortedSet<Integer> _set = e.getValue();
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