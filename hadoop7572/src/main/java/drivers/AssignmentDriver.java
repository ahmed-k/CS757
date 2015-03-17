package drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Ahmed Alabdullah on 3/15/15.
 */
public class AssignmentDriver {



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: JobDriver <in> <out> <pair|stripe> <dataset>");
            System.exit(2);
        }
        String technique = otherArgs[2];
        String dataset = otherArgs[3];


        Job job = new Job(conf, "movie pairs");

        //specific configuration depending on task required
        JobConfigurer.configureJob(technique, dataset, job);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true)? 0 :1);

    }


}
