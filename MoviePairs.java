import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by Ahmed Alabdullah on 3/15/15.
 */
public class MoviePairs {

    /**

    Composite Key Object
     */
    private static class PairKey implements WritableComparable {

        private Integer lowID;
        private Integer highID;

        public PairKey(Integer lowID, Integer highID) {
            this.lowID = lowID;
            this.highID = highID;
        }

        public Integer getLowID() {
            return lowID;
        }

        public Integer getHighID() {
            return highID;
        }

        @Override
        public int compareTo(Object o) {
            if (o==null) {
                return 0;
            }
            PairKey other = (PairKey) o;
            return (other.getLowID().compareTo(lowID));
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(lowID.intValue());
            dataOutput.writeInt(highID.intValue());
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            lowID = new Integer(dataInput.readInt());
            highID = new Integer(dataInput.readInt());
        }
    }

    /**
     * Comparators
     */

    public class CompositeKeyComparator extends WritableComparator {
        protected CompositeKeyComparator() {
            super(PairKey.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            PairKey k1 = (PairKey) w1;
            PairKey k2 = (PairKey) w2;

            int result = k1.getLowID().compareTo(k2.getLowID());
            return result;
        }


    }

    public class NaturalKeyPartitioner extends Partitioner<PairKey, IntWritable> {

        public int getPartition(PairKey k, IntWritable v, int numPartitions) {
            int hash = k.getLowID().hashCode();
            int partition = hash % numPartitions;
            return partition;
        }

    }

    public class NaturalKeyGroupingComparator extends WritableComparator {
        protected NaturalKeyGroupingComparator() {
            super(PairKey.class, true);
        }


        public int compare(WritableComparable w1, WritableComparable w2) {
            PairKey k1 = (PairKey) w1;
            PairKey k2 = (PairKey) w2;
            return k1.getLowID().compareTo(k2.getLowID());
        }
    }


    /**
     *              MAPPER
     */


    public static class PairMapper extends Mapper<Text, Text, PairKey, IntWritable> {

        private SortedSet<Integer> candidates = new TreeSet<Integer>();
        private IntWritable one = new IntWritable(1);
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Text userID = new Text();
            userID.set(key.toString());
            String[] vals = value.toString().split("\t");
            String _movieID = vals[0];
            String _rating = vals[1];
            Integer movieID = new Integer(_movieID);
            Integer rating = new Integer(_rating);
            if (rating > 3) {
                candidates.add(movieID);
            }
        }//map

        public void cleanup(Context context) throws IOException, InterruptedException {
            Integer [] arr = candidates.toArray(new Integer[candidates.size()]);
            for (int i = 0 ; i < arr.length ; i++) {
                for (int j = 0 ; j < arr.length ; j++) {
                    context.write(new PairKey(arr[i],arr[j]), one);
                }//for j

            }//for i
        }//cleanup



    }//PairMapper


    /**
     *          REDUCER
     */

    public static class PairReducer extends Reducer<PairKey, Iterable<IntWritable>, Text, IntWritable> {

        public void reduce(PairKey key, Iterable<IntWritable> vals, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : vals) {
                sum+= val.get();
            }//for
            IntWritable result = new IntWritable(sum);
            context.write(new Text(key.toString()), result);
        } //reduce

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: moviepairs <in> <out>");
            System.exit(2);
        }

        //CONFIGURE THE JOB
        Job job = new Job(conf, "movie pairs");

        job.setJarByClass(MoviePairs.class);

        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setMapperClass(PairMapper.class);
        job.setCombinerClass(PairReducer.class);
        job.setReducerClass(PairReducer.class);

        job.setMapOutputKeyClass(PairKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0 :1);

    }


}
