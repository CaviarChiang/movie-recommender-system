package recommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**CHAPTER 4: Lesson Learnt on Recommender System - 4th mapreduce job
 * 0. Task: To multiply normalized co-occurrence matrix and user-rating matrix, perform cell multiplication
 *    rawInput format: movie2\tmovie1=2/8
 *                     user,movie,rating
 *      output format: user1:movie1\t10*2/8
 *
 * 1. mapper1
 *      input line value: movie2\tmovie1=2/8
 *                output: key: movie2
 *                      value: movie1=2/8
 *
 * 2. mapper2
 *      input line value: user,movie,rating
 *                output: key: movie2
 *                      value: user:rating
 *
 * 3. reducer
 *      input: key: movie2       (rating of 10 by user1)
 *          values: <movie1=2/8,movie2=4/8,movie3=2/8, user1:10,user2:8,user3:9,...>
 *     output: key: user1:movie1
 *           value: 10*2/8       (movie2 has 2/8 contribution to the predicted rating of movie1 by user1)
 *             key: user1:movie2
 *           value: 10*4/8       (movie2 has 4/8 contribution to the predicted rating of movie2 by user1)
 *             key: user1:movie3
 *           value: 10*2/8       (movie2 has 2/8 contribution to the predicted rating of movie3 by user1)
 *             ...
 *    Note: - Split and store movie_relation pairs and user_rating paris in separate hash maps
 *          - Output key is movie1, not movie2, which is the input key!!!
 *
 * 4. Configuration and jobs
 *      - when there's two mappers in one mapreduce job, chain the two mappers by:
 *          `ChainMapper.addMapper(job, CoOccurrenceMapper.class, LongWritable.class, Text.class, Text.class,Text.class, conf);...`
 *      - set map output key/value class when it's not consistent with reducer output key/value class:
 *          `job.setMapOutputKeyClass(Text.class); job.setMapOutputValueClass(Text.class);`
 *      - specify which mapper reads which dir as input:
 *          `MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoOccurrenceMapper.class);`
 * */

public class Multiplier {

    public static class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input line value: movie2\tmovie1=relation
            //       output key: movie2
            //            value: movie1=relation
            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));
        }

    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input line value: user1,movie2,rating
            //       output key: movie2
            //            value: user1:rating
            String[] line = value.toString().trim().split(",");
            context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));
        }

    }

    public class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input:  key: movie2
            //      values: <movie1=relation1, movie2=relation2, movie3=relation3, ...,
            //               user1:rating1, user2:rating2, user3:rating3, ...>
            // output: key: user1:movie1
            //       value: rating1*relation1
            //         key: user1:movie2
            //       value: rating1*relation2
            //         key: user1:movie3
            //       value: rating1*relation3
            //              ...
            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String, Double>();

            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movie_relation = value.toString().split("=");
                    relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else {
                    String[] user_rating = value.toString().split(":");
                    ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            for (Map.Entry<String, Double> entry1 : relationMap.entrySet()) {
                String movie1 = entry1.getKey();
                Double relation = entry1.getValue();

                for (Map.Entry<String, Double> entry2 : ratingMap.entrySet()) {
                    String user = entry2.getKey();
                    Double rating = entry2.getValue();

                    String outputKey = user + ":" + movie1; // not input key (movie2)
                    Double outputValue = relation * rating;
                    context.write(new Text(outputKey), new DoubleWritable(outputValue));
                }
            }

        }

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplier.class);

        // chain two mappers
        ChainMapper.addMapper(job, CoOccurrenceMapper.class, LongWritable.class, Text.class, Text.class,Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);

        // set mapper/reducer class
        job.setMapperClass(CoOccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        // set map output key/value class when it's not consistent with reducer output key/value class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // specify which mapper reads which dir
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoOccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

    }

}
