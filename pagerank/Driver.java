package pagerank;

public class Driver {

    // Driver 进行 Transition Matrix * PR Matrix 的迭代
    // mapreduce 会把结果写到硬盘上

    public static void main(String[] args) throws Exception {

        UnitMultiplication multiplication = new UnitMultiplication();
        UnitSum sum = new UnitSum();

        String transitionMatrix = args[0]; // dir where transition.txt resides
        String prMatrix = args[1];         // dir of pr (also where the 2nd mapreduce job's output resides)
        String subPageRank = args[2];      // dir where the 1st mapreduce job's output resides (subPR)

        int count = Integer.parseInt(args[3]); // iteration count

        for (int i = 0; i < count; i++) {  // i start from 0
            // pass transitionMatrix dir to 1st mapreduce job (transitionMatrix is fixed in our case)
            // pass prMatrix to 1st mapreduce job (prMatrix is varying)
            // pass subPR to 1st mapreduce job
            String[] args1 = { transitionMatrix, prMatrix + i, subPageRank + i };
            multiplication.main(args1);

            // pass subPR to 2nd mapreduce job as input dir (subPR is the output of the 1st mapreduce job)
            // pass (complete) pr to 2nd mapreduce job as output dir
            // make sure to increment output pr dir by 1 because mapreduce won't start if output dir already exists
            // final output is stored in hdfs /pagerankN/, where N is times of convergence
            String[] args2 = { subPageRank + i,  prMatrix + (i + 1) };
            sum.main(args2);
        }

    }

}
