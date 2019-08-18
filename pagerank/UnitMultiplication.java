package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**CHAPTER 3: Lesson Learnt on PageRank
 *
 * 1. 1st mapreduce job
 *
 * 1.1 1st mapper
 *  - input:  transition.txt -> 1\t2,8,9,24
 *  - output: key = 1 (fromPage)
 *          value = to=probFromTo
 *  - check dead ends: page that doesn't direct to any other pages (1\t)
 *
 * 1.2 2nd mapper
 *  - input:  pr.txt -> 1\t1/6012
 *  - output: key = 1 (fromPage)
 *          value = 1/6012
 *
 * 1.3 reducer
 *  - input:  key = 1 (fromPage)
 *         values = <2=1/4, 8=1/4, 8=1/4, 24=1/4, 1/6012> (toPages and pr of fromPage)
 *  - output: key = 2 (toPage)
 *          value = text(1/4*1/6012)                      (the key fromPage's contribution to the pr of current toPage)
 *          write to hdfs as subPR
 *  - need to separate transition cells from pr cell, and then multiply
 *
 * 1.4 job configurations
 *  - we have 2 mappers in this mapreduce job, so we need to set mapper class by chaining the 2 mappers:
 *      `ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);`
 *      `ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);`
 *  - we also need to set different input dirs for the two mappers
 *      `MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);`
 *      `MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);`
 *  - generic steps:
 *      + create configuration and job
 *      + set mapper class and reducer class
 *      + set (reducer) output key class and output value class
 *      + set input and output dir
 *      + tell job to wait for completion
 *  - args[0]: dir of relation.txt, read by TransitionMapper
 *    args[1]: dir of pr, read by PRMapper
 *    args[2]: dir of subPR, output of the 1st mapreduce job
 *
 *
 * 2. 2nd mapreduce job
 *
 * 2.1 mapper
 *  - input:  subPR -> 2\ttext(1/4*1/6012)
 *  - output: key = 2 (toPage)
 *          value = double(1/4*1/6012)
 *
 * 2.2 reducer
 *  - input:  key = 2 (toPage)
 *         values = <double(1/4*1/6012), double(1/3*1/6012), double(1/9*1/6012), ...>
 *  - output: key = 2 (toPage)
 *          value = double(sum)
 *          write to hdfs as pr
 *
 * 2.3 job configurations
 *  - args[0]: dir of subPR, output of the 1st mapreduce job, input of the 2nd mapreduce job
 *    args[1]: dir of pr, output of the 2nd mapreduce job, input of the 1st mapreduce job's 2nd mapper
 *
 *
 * 3. Driver
 *  - Driver 进行 Transition Matrix * PR Matrix 的迭代 (mapreduce 会把结果写到 hdfs 上)
 *  - args[0]: dir of relation.txt, read by TransitionMapper
 *    args[1]: dir of pr, output of the 2nd mapreduce job, input of the 1st mapreduce job's 2nd mapper
 *    args[2]: dir of subPR, output of the 1st mapreduce job, input of the 2nd mapreduce job
 *    args[3]: count of iteration times
 *  - transition matrix file is fixed in our case, but the pr and subPR are varying in every iteration
 *  - pass cmd line args from Driver's main to mapreduce classes' main:
 *      + pass dir of transition matrix, pr and subPR to 1st mapreduce job in every iteration:
 *          `String[] args1 = { transitionMatrix, prMatrix + i, subPageRank + i };`
 *          `multiplication.main(args1);`
 *      + pass dir of subPR, pr to 2nd mapreduce job in every iteration:
 *          `String[] args2 = { subPageRank + i,  prMatrix + (i + 1) };`
 *          `sum.main(args2);`
 *  - make sure to increment output pr dir by 1 because mapreduce won't start if output dir already exists
 *  - final output is stored in hdfs /pagerankN/, where N is the times of convergence
 *
 *
 * 4. Solution to Package org.apache.hadoop.mapreduce.lib.chain is missing from hadoop-core JAR files
 *  + https://apache.googlesource.com/hadoop-mapreduce/
 *  + https://issues.cloudera.org/browse/DISTRO-519
 *  + https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/hadoop/hadoop-mapreduce-client-core/2.0.0-cdh4.3.1/
 *  1) Download hadoop-mapreduce-client-core-2.0.0-cdh4.3.1.jar to project/lib folder
 *  2) Right click on project, Open Module Settings
 *  3) In Libraries, Add New Project Library from Java
 *  4) Open the hadoop-core.jar file just downloaded
 *  5) Save and apply
 *
 *
 * 5. Running on hadoop - see pdf for instructions
 *
 * */

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input:  1\t2,8,9,24
            // output:   key = 1
            //         value = to=probOfFromToTo
            String line = value.toString().trim();
            String[] fromTo = line.split("\t");
            // fromTo at least has 2 elements; but it's possible a page doesn't direct to any other page (dead ends)

            if (fromTo.length < 2 || fromTo[1].trim().equals("")) {
                return;
            }

            String from = fromTo[0];
            String[] to = fromTo[1].split(",");
            for (String cur : to) {
                context.write(new Text(from), new Text(cur + "=" + (double) 1 / to.length));
            }
        }

    }

    public static class PRMapper extends  Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input:  pr.txt -> 1\t1/6012
            // output:  key = 1
            //        value = 1/6012
            String[] pr = value.toString().trim().split("\t"); // check validity?
            context.write(new Text(pr[0]), new Text(pr[1]));
        }

    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // input:  key = 1 (fromPage)
            //      values = <2=1/4, 7=1/4, 1/6012> (toPages + fromPageWeight)
            // separate transition cells from pr cell
            // then multiply
            // output: key = 2 (toPage)
            //       value = 1/4*1/6012 (toPage's partial weight component)

            // for the 2nd mapreduce job (mainly reducer):
            // input:  key = 2 (toPage)
            //      values = <1/4*1/6012, 1/9*6012, ...> (toPage's weights combined)
            // output: key = 2
            //       value = sum

            List<String> transitionCells = new ArrayList<String>();
            double prCell = .0;

            // separate transition cells from pr cell
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    transitionCells.add(value.toString().trim());
                } else {
                    prCell = Double.parseDouble(value.toString().trim());
                }
            }

            // multiply and write to context
            for (String cell : transitionCells) {
                String outputKey = cell.split("=")[0];
                double relation = Double.parseDouble(cell.split("=")[1]);
                String outputValue = String.valueOf(relation * prCell);
                context.write(new Text(outputKey), new Text(outputValue));
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // create configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);


        // set mapper class by chaining the two mappers
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        /* Solution to Package org.apache.hadoop.mapreduce.lib.chain is missing from hadoop-core JAR files
         * https://apache.googlesource.com/hadoop-mapreduce/
         * https://issues.cloudera.org/browse/DISTRO-519
         * https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/hadoop/hadoop-mapreduce-client-core/2.0.0-cdh4.3.1/
         * 1. Download hadoop-mapreduce-client-core-2.0.0-cdh4.3.1.jar to project/lib folder
         * 2. Right click on project, Open Module Settings
         * 3. In Libraries, Add New Project Library from Java
         * 4. Open the hadoop-core.jar file just downloaded
         * 5. Save and apply
         * */

        // set reducer class
        job.setReducerClass(MultiplicationReducer.class);


        // set reducer output key class and reducer output value class
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        // set different input paths for the two mappers
        // args[0] = dir of relation.txt, read by TransitionMapper
        // args[1] = dir or pr, read by PRMapper
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        // set output dir for the 1st mapreduce job
        FileOutputFormat.setOutputPath(job, new Path(args[2]));


        // tell job to wait for completion
        job.waitForCompletion(true);

    }

}