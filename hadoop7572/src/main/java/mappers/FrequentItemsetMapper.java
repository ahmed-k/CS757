package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by alabdullahwi on 3/18/2015.
 */
public class FrequentItemsetMapper extends Mapper<Text, Text, Text, IntWritable> {


    private Map<String, Integer> supportMap = new HashMap<String, Integer>();
    private Map<Integer, List<Integer>> temp = new HashMap<Integer, List<Integer>>();
    private IntWritable one = new IntWritable(1);
    private final int SUPPORT_THRESHOLD = 30;
    private Text outKey = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        Integer userID = new Integer(key.toString());
        String[] vals = value.toString().split("\t");
        if (Double.parseDouble(vals[1]) >= 4) {
            List candidates = temp.get(userID);
            if (candidates == null) {
                candidates = new ArrayList<Integer>();
            }
            candidates.add(new Integer(vals[0]));
            temp.put(userID, candidates);

        }
    }

    public List<String> doCombine(Integer[] arr) {

        List<String> retv = new ArrayList<String>();
        if (arr.length == 1) {
            return retv;
        }
        String _key = "<";
        for (int i = 0 ; i < arr.length-1 ; i++) {
            _key += arr[i]+",";
            for (int j = i+1 ; j < arr.length; j++) {
                retv.add(_key+arr[j]+">");
            }
        }
        retv.addAll(doCombine(Arrays.copyOfRange(arr, 1, arr.length)));
        return retv;
    }


    public void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, List<Integer>> e : temp.entrySet()) {
            List<Integer> _set = e.getValue();
            Collections.sort(_set);
            Integer [] arr = _set.toArray(new Integer[_set.size()]);
            List<String> itemsets = doCombine(arr);
            for (String itemset : itemsets) {
                Integer _support = supportMap.get(itemset);
                if (_support == null) { _support = 0; }
                _support += 1;
                if (_support >= SUPPORT_THRESHOLD ) {
                    outKey.set(itemset);
                    context.write(outKey, one);
                }
                supportMap.put(itemset, _support);
            }



        }//for Map Entries
    }//cleanup

}//PairMapper





