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

/** 2nd mapreduce job
 * 0. Task: create movie co-occurrence matrix **regardless of users and ratings**; each row represents a cell in co-matrix
 *    rawInput format: userid\tmovie1:rating1,movie2:rating2,movie3:rating3,...
 *      output format: movie1:movie2\t2
 *
 * 1. mapper
 *      input line value: userid\tmovie1:rating1,movie2:rating2,movie3:rating3,...
 *                output: key: movie1:movie1
 *                      value: 1
 *                        key: movie1:movie2
 *                      value: 1
 *                        key: movie1:move3
 *                      value: 1
 *                        ...
 *    Note: mapper will need 2 loops to iterate all movie_rating pairs for a specific user
 *          [movie1,movie2,movie3] -> [1:1,1:2,1:3,2:1,2:2,2:3,3:1,3:2,3:3]
 *
 * 2. reducer
 *      input: key: movie2:movie2
 *          values: <IntWritable(1), IntWritable(1), IntWritable(1), IntWritable(1)>
 *     output: key: movie2:movie2
 *           value: 4
 *    Note: IntWritable object is serializable
 *          IntWritable(1) cannot be merged to IntWritable(4) or 4 !!!
 * */

public class CoOccurrenceMatrixGenerator { // 2nd mapreduce job

    // Co-Occurrence Matrix has nothing to do with users/ratings
    // It represents the relationship between movies

    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input line value: userid\tmovie1:rating1,movie2:rating2,... (default write delimiter (between key & value) is \t)
            //      output: key: movie1:movie1
            //            value: 1
            // eg. [1,2,3]: 1:1,2:2,3:3,1:2,2:1,1:3,3:1,2:3,3:2
            //              1:1,1:2,1:3,2:1,2:2,2:3,3:1,3:2,3:3

            String line = value.toString().trim();
            String[] user_movieRatings = line.split("\t");
            if (user_movieRatings.length != 2) { // bad input
                return;
            }

            String[] movie_ratings = user_movieRatings[1].split(",");
            for (int i = 0; i < movie_ratings.length; i++) {
                // movie_rating: movie1:rating1
                String movie1 = movie_ratings[i].trim().split(":")[0];
                for (int j = 0; j < movie_ratings.length; j++) {
                    String movie2 = movie_ratings[j].trim().split(":")[0];
                    context.write(new Text(movie1 + ":" + movie2), new IntWritable(1));
                }
            }
        }

    }

    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // input:  key: movie2:movie2
            //      values: [IntWritable(1),IntWritable(1),IntWritable(1),IntWritable(1)]
            // A list of IntWritable objects; IntWritable object is serializable
            // IntWritable(1) cannot be merged to IntWritable(4) / 4 !!!
            // output: key: movie2:movie2
            //       value: 4
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}
