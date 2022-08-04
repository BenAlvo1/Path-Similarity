import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step1 {
    public static void main(String[] args) throws Exception {
        int BIARCS_NUM = 46;
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Round1");
        job1.getConfiguration().set("mapred.job.jvm.num.tasks","-1");
        job1.setJarByClass(Round1.class);
        job1.setMapperClass(Round1.MapperClass.class);
        job1.setCombinerClass(Round1.CombinerClass.class);
        job1.setPartitionerClass(Round1.PartitionerClass.class);
        job1.setReducerClass(Round1.ReducerClass.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        Path[] inputs = new Path[BIARCS_NUM];
        int counter=0;
        for(int i=0;i<BIARCS_NUM*2;i+=2){
            if(i<10)
                inputs[counter] = new Path("s3://biarcs/biarcs.0"+i+"-of-99");
            else inputs[counter] = new Path("s3://biarcs/biarcs."+i+"-of-99");
            counter++;
        }
        TextInputFormat.setInputPaths(job1, inputs);
        job1.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job1, new Path(Round1.output));

        if (!job1.waitForCompletion(true)){
            System.exit(1);
        }

        Counter SLOT_COUNT = job1.getCounters().findCounter(MyCounter.N_COUNTERS.SLOT);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Round2");
        job2.getConfiguration().setLong(MyCounter.N_COUNTERS.SLOT.name(), SLOT_COUNT.getValue());
        job2.getConfiguration().set("mapred.job.jvm.num.tasks","-1");
        job2.setJarByClass(Round2.class);
        job2.setMapperClass(Round2.MapperClass.class);
        job2.setPartitionerClass(Round2.PartitionerClass.class);
        job2.setReducerClass(Round2.ReducerClass.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(Round1.output));
        FileOutputFormat.setOutputPath(job2, new Path(Round2.output));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
