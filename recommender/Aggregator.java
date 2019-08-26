package recommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/** 5th mapreduce job
 * 0. Task: Sum the cells that have the same key/tag
 *    rawInput format: user1:movie1\trating1*relation1
 *      output format: user1:movie1\tsum
 *
 * 1. mapper1
 *      input line value: user1:movie1\t10*2/8
 *                output: key: user1:movie1
 *                      value: DoubleWritable(2/8)
 *
 * 2. reducer
 *      input: key: user1:movie1
 *          values: <DoubleWritable(rating1*relation1), DoubleWritable(rating1*relation2), DoubleWritable(rating1*relation3)>
 *     output: key: user1:movie1
 *           value: DoubleWritable(rating1*relation1 + rating1*relation2 + rating1*relation3)
 *    Note: DoubleWritable objects cannot be added by themselves
 * */

public class Aggregator {

    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input line value: user:movie/trating*relation
            //       output key: user:movie
            //            value: rating*relation
            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new DoubleWritable(Double.parseDouble(line[1])));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            // input:  key: user1:movie1
            //      values: <rating1*relation1,...> // DoubleWritable cannot be added by themselves
            // output: key: user1:movie1
            //       value: sum
            int sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Aggregator.class);

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
