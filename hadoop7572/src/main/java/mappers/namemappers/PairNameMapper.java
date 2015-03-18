package mappers.namemappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by alabdullahwi on 3/18/2015.
 */
public class PairNameMapper extends Mapper<Text, Text, Text, IntWritable> {

    private IntWritable outVal = new IntWritable();
    private SortedMap<Integer, String> topTwenty = new TreeMap<Integer, String>();
    private Text out = new Text();
    public void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String _val = value.toString().split("\t")[0];
        String _key = key.toString();
        addIfTopTwenty(_key, _val);

    }//map

    private void addIfTopTwenty(String pair, String _count) {
        Integer count = new Integer(_count);
        if (topTwenty.size() < 20) {
           topTwenty.put(count, pair);
        }

        else {
            if (topTwenty.firstKey() < count) {
                topTwenty.remove(topTwenty.firstKey());
                topTwenty.put(count, pair);
            }
        }
    }

    public boolean isPair(String _key) {
        return _key.matches("^<\\d+, \\d+>$");
    }

    public boolean isMovieID(String _key) {
        return _key.matches("^\\d+$");
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, String> e : topTwenty.entrySet()) {
            out.set(e.getValue());
            outVal.set(e.getKey());
            context.write(out, outVal);
        }//for Map Entries
    }//cleanup






}
