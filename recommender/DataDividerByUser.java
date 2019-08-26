package recommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/** 1st mapreduce job
 * 0. Task: for a specific user, assemble all its movie ratings (a user represents a line)
 *    rawInput format: user,movie,rating
 *      output format: userid\tmovie1:rating1,movie2:rating2,movie3:rating3,...
 *
 * 1. mapper
 *      input line value: user,movie,rating
 *                output: key: userid
 *                      value: movie1:rating1
 * 2. reducer
 *      input: key: userid
 *          values: <movie1:rating1, movie2:rating2, movie3:rating3, ...>
 *     output: key: userid
 *           value: userid1:rating1,movie2:rating2,movie3:rating3,...
 * */

public class DataDividerByUser { // 1st mapreduce job

    // Java doesn't allow you to create top-level static classes; only nested (inner) static classes.
    // In Java, static is a keyword used to describe how objects are managed in memory. It means that
    // the static object belongs specifically to the class, instead of instances of that class.
    // Variables, methods, and nested classes can be static.

    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input line value: user,movie,rating
            //           output: key: userid
            //                 value: movie1:rating1
            String[] user_movie_rating = value.toString().trim().split(",");
            int userID = Integer.parseInt(user_movie_rating[0]);
            String movieId = user_movie_rating[1];
            String rating = user_movie_rating[2];

            context.write(new IntWritable(userID), new Text(movieId + ":" + rating));
        }
    }

    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // input:  key: userid
            //      values: <movie1:rating1, movie2:rating2, ...>
            // output: key: userid
            //       value: ,movie1:rating1,movie2:rating2,...
            StringBuilder sb = new StringBuilder();
            while (values.iterator().hasNext()) {
                sb.append("," + values.iterator().next());
            }
            context.write(key, new Text(sb.toString().replaceFirst(",",""))); // replace the 1st comma with empty
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataDividerByUser.class);

        // set mapper and reducer class
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        // set input and output format class                    // why set i/o format class?
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // when the output classes of mapper & reducer are the same
        // no need to setMapOutputKeyClass & setMapOutputValueClass

        // set (reducer) output key and value class
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // set input and output dir
        TextInputFormat.setInputPaths(job, new Path(args[0]));  // what's the difference between TextInputFormat & FileInputFormat???
        TextOutputFormat.setOutputPath(job, new Path(args[1])); // what's the difference between addInputPath & setInputPaths???

        // tell job to wait for completion
        job.waitForCompletion(true);
    }


}
