package mappers;

/**
 * Created by alabdullahwi on 3/16/2015.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alabdullahwi on 3/15/2015.
 */

public class StripeMapper extends Mapper<Text, Text, IntWritable, MapWritable> {

    private Map<Integer, List<Integer>> temp = new HashMap<Integer, List<Integer>>();
    private IntWritable one = new IntWritable(1);
    private MapWritable val = new MapWritable();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Integer userID = new Integer(key.toString());
        String[] vals = value.toString().split("\t");
        if (new Double(vals[0]) >= 4) {
            List candidates  = temp.get(userID);
            if (candidates == null) {
                candidates = new ArrayList<Integer>();
            }
            candidates.add(new Integer(vals[1]));
            temp.put(userID, candidates);

        }
    }//map

    public void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, List<Integer>> e : temp.entrySet()) {
            List<Integer> _set = e.getValue();
            Integer [] arr = _set.toArray(new Integer[_set.size()]);
            for (int i = 0 ; i < arr.length ; i++) {
                Map<IntWritable, IntWritable> occurrences = new HashMap<IntWritable, IntWritable>();
                for (int j = 0 ; j < arr.length ; j++) {
                    if (arr[i] != arr[j]) {
                        occurrences.put(new IntWritable(arr[j]), one);
                        }
                }//for j
                val.putAll(occurrences);
                context.write(new IntWritable(arr[i]),val);
            }//for i
        }//for Map Entries

    }//cleanup

}//PairMapper