package drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by alabdullahwi on 4/21/2015.
 */
public class AssignmentDriver {


    private static Configuration createConfiguration(String[] args) {
        String m = args[2];
        String n = args[3];
        String d = args[4];
        Configuration conf = new Configuration();
        conf.set("m", m);
        conf.set("n", n);
        conf.set("d", d);
        return conf;
    }

    public static void main(String[] args) throws Exception {
        //get args
        Configuration conf = createConfiguration(args);
        JobFactory jf = new JobFactory();
        Job job;
        if (args.length == 6) {
            job = jf.configureMultiplyJob(conf, args);
            System.exit(job.waitForCompletion(true)? 0 :1);
        }
        else {
            job = jf.configureBGDJob(conf, args);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            if (job.waitForCompletion(true)) {
                Job job2 = jf.createIterationJob();
                if(job2.waitForCompletion(true)) {
                    Job job3 = jf.createCollapseJob();
                    System.exit(job3.waitForCompletion(true)? 0: 1);
                }
            }
        }
    }




}
