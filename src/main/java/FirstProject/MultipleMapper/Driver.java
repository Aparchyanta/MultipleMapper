package FirstProject.MultipleMapper;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new Driver(), args);
	    System.exit(exitCode);

	}
	public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                    .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        Job job = new Job(getConf());
        
        job.setJarByClass(Driver.class);
        job.setJobName(this.getClass().getName());

       // MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MyMapper2.class);
        
        
        //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyMapper1.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //job.setMapperClass(MyMapper1.class);
      //  job.setMapperClass(MyMapper.class);
        //job.setReducerClass(CountR.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setNumReduceTasks(0);
        
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
}
