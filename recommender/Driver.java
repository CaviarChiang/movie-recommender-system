package recommender;

/** Driver
 * 0. Task: Initialize mapreduce jobs' classes
 *          Specify which args go to where
 *          Pass args to mapreduce jobs' main method
 *          Filter out movies the user already watched and extract top N movies for recommendation
 *
 * 1. args
 *      - 1st mapreduce job (DataDividerByUser):           rawInputFile, userid\tmovie1:rating1 output dir
 *      - 2nd mapreduce job (CoOccurrenceMatrixGenerator): userid\tmovie1:rating1 output dir, movie1:movie2\t2 co-occurrence matrix output dir
 *      - 3rd mapreduce job (Normalizer): movie1:movie2\t2 co-occurrence matrix output dir, movie2\tmovie1=2/8 normalizer output dir
 *      - 4th mapreduce job (Multiplier): movie2\tmovie1=2/8 normalizer output dir, rawInputFile, user1:movie1\t10*2/8 multiplier output dir
 *      - 5th mapreduce job (Aggregator): user1:movie1\t10*2/8 multiplier output dir, user1:movie1\tsum aggregator output dir
 *
 * 2. Summary
 * 1st mapreduce job: take raw input and assemble all movie rating pairs for a specific user (1 user / line)
 * 2nd mapreduce job: generate co-occurrence matrix for each movie1:movie2 pairs (regardless of users/ratings; 1 cell / line: each line represents a cell in co-matrix)
 * 3rd mapreduce job: normalize co-occurrence matrix and output the **transposed column vector**
 * 4th mapreduce job: perform cell multiplication
 *                    user1 gave movie2 a rating of 10;
 * 			          for movie2, the relation is: movie1=2/8, movie2=4/8, movie3=2/8
 *                    movie2 has 2/8 contribution to the rating of movie1 by user1
 *                    movie2 has 4/8 contribution to the rating of movie2 by user1
 *                    movie2 has 2/8 contribution to the rating of movie3 by user 1
 * 5th mapreduce job: sum all the cell computations (output: user1:movie1\tsum)
 * Driver: initialize mapreduce jobs’ classes
 *            specify which args go to which job/class
 *            pass args to jobs’ main method
 * Postprocessing: filter out movies users already watched and extract topK for recommendation
 *
 * 3. Refactor
 *      - in rawInputFile, for movies that a user hasn't yet seen, consider using that user's average rating in place of 0
 * */

public class Driver {

    public static void main(String[] args) throws Exception {

        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        Normalizer normalizer = new Normalizer();
        Multiplier multiplier = new Multiplier();
        Aggregator aggregator = new Aggregator();

        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixDir = args[2];
        String normalizerDir = args[3];
        String multiplierDir = args[4];
        String aggregatorDir = args[5];

        String[] path1 = {rawInput, userMovieListOutputDir};
        String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
        String[] path3 = {coOccurrenceMatrixDir, normalizerDir};
        String[] path4 = {normalizerDir, rawInput, multiplierDir};
        String[] path5 = {multiplierDir, aggregatorDir};

        // pass cmd args to classes' main method
        dataDividerByUser.main(path1);
        coOccurrenceMatrixGenerator.main(path2);
        normalizer.main(path3);
        multiplier.main(path4);
        aggregator.main(path5);

        // filter out movies the user already watched
        // extract top N movies

    }

}
