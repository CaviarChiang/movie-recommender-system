package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {

    public class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input:  key\tsubPR
            //           2\t1/4*1/6012
            // output: key = 2
            //       value = 1/4*1/6012
            String[] pageSubRank = value.toString().trim().split("\t"); // \t is the default delimiter when you write to hdfs
            double subRank = Double.parseDouble(pageSubRank[1]);
            context.write(new Text(pageSubRank[0]), new DoubleWritable(subRank));
        }

    }

    public class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // input:  key = 2 (toPage)
            //      values = <1/4*1/6012, 1/9*6012, ...> (toPage's weights combined)
            // output: key = 2
            //       value = sum
            double total = 0;
            for (DoubleWritable value : values) {
                total += value.get();
            }

            DecimalFormat df = new DecimalFormat("#.00000"); // max decimal points: 5
            total = Double.valueOf(df.format(total));

            context.write(key, new DoubleWritable(total));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // create configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

        // set mapper and reducer class
        job.setMapperClass(PassMapper.class);
        job.setReducerClass(SumReducer.class);

        // set (reducer) output key and value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // set input and output dir
        // args[0]: dir of subPR, output of the 1st mapreduce job, input of the 2nd mapreduce job
        // args[1]: dir of pr, output of the 2nd mapreduce job, input of the 1st mapreduce job's 2nd mapper
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // tell job to wait for completion
        job.waitForCompletion(true);

    }

}
