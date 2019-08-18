package recommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**CHAPTER 4: Lesson Learnt on Recommender System - 3rd mapreduce job
 * 0. Task: Normalize the co-occurrence matrix and **output the transposed column vector**
 *    rawInput format: movie1:movie2\t2
 *      output format: movie2\tmovie1=2/8
 *
 * 1. mapper
 *      input line value: movie1:movie2\t2
 *                output: key: movie1
 *                      value: movie2=2
 *
 * 2. reducer
 *      input: key: movie1
 *          values: <movie1=2,movie2=4,movie3=2>
 *     output: key: movie2
 *           value: movie1=4/8
 *             key: movie3
 *           value: movie1=2/8
 *             key: movie1
 *           value: movie1=2/8
 *    Note: - To normalize, you'll need the sum as denominator; you can put iterable values to a hash map
 *          - For later cell multiplication purposes, transpose the output vector (to col vec) before writing to context
 * */

public class Normalizer {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input line value: movie1:movie2\trelation
            //       output key: movie1
            //            value: movie2=2

            String[] movies_relation = value.toString().trim().split("\t");
            if (movies_relation.length != 2) return;

            String relation = movies_relation[1];
            String movie1 = movies_relation[0].split(":")[0];
            String movie2 = movies_relation[0].split(":")[1];
            context.write(new Text(movie1), new Text(movie2 + "=" + relation));
        }

    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //           sum -> denominator
            // context write -> transpose
            // input:  key: movie1
            //      values: <movie1=2, movie2=4, movie3=2>
            // output: key: movie3
            //       value: movie1=2/8

            int sum = 0;
            // put iterable (values) to a map
            Map<String, Integer> map = new HashMap<String, Integer>();

            for (Text value : values) {
                String[] movie_relation = value.toString().split("=");
                String movie2 = movie_relation[0];
                int relation = Integer.parseInt(movie_relation[1]);
                sum += relation;
                map.put(movie2, relation);
            }

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                // transpose
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + (double) entry.getValue() / sum;
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Normalizer.class);

        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
