package edu.usc.cs.ir.solr.similarity;

import edu.usc.cs.ir.vsm.Vector;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.SystemClock;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Computes similarity between documents in solr.
 */
public class SolrSimilarity {

    public static Function<Tuple2<Vector, Vector>, Double> COS_FUNC = pair -> pair._1().cosθ(pair._2());

    @Option(name = "-solr", required = true, usage = "Solr URL")
    private URL solrUrl;

    @Option(name = "-query", usage = "Solr query for selecting the documents")
    private String solrQuery = "*:*";

    @Option(name ="-master", usage="spark master name or url")
    private String masterUrl = "local";

    @Option(name = "-measure", usage = "name of similarity measure to use")
    private SimilarityMeasure measure = SimilarityMeasure.cosine;

    @Option(name = "-vectorizer", usage = "name of vectorizer to use")
    private Vectorizer vectorizer = Vectorizer.simple;

    @Option(name = "-out", usage = "path to directory for storing the output")
    private File output;

    private JavaSparkContext context;
    private Map<String, Long> featureDictionary;

    /**
     * Registry of all known distance (or similarity) measure functions
     */
    public enum SimilarityMeasure {
        cosine (COS_FUNC);

        public Function<Tuple2<Vector, Vector>, Double> measureFunction;


        SimilarityMeasure(Function<Tuple2<Vector, Vector>, Double> measureFunc) {
            this.measureFunction = measureFunc;
        }

        public Function<Tuple2<Vector, Vector>, Double> getMeasureFunction() {
            return measureFunction;
        }
    }


    /**
     * Registry of all known vectorizer functions
     */
    public enum Vectorizer {
        simple;
        //TODO: create a sophisticated (value based) vectorizer for better similarity results

        private VectorizeFunction vectorizer;

        public VectorizeFunction getVectorizer(SolrSimilarity similarity){
            if (vectorizer == null) {
                switch (this) {
                    //lazy decision
                    case simple:
                        vectorizer = new SimpleVectorizer(similarity.featureDictionary);
                        break;
                    default: throw new IllegalStateException("Not implemented yet " + name());
                }
            }
            return vectorizer;
        }
    }

    /**
     * Initialize method
     * @throws IOException
     * @throws SolrServerException
     */
    private void init() throws IOException, SolrServerException {
        SparkConf conf = new SparkConf()
                .setAppName(getClass().getName())
                .setMaster(masterUrl);
        this.context = new JavaSparkContext(conf);
        this.featureDictionary = new HashMap<>();
        long count = 0;
        for(String field: getAllFields()) {
            featureDictionary.put(field, count++);
        }
    }

    /**
     * gets all fields in solr using luke request handler
     * @return Set of field names
     * @throws IOException
     * @throws SolrServerException
     */
    public Set<String> getAllFields() throws IOException, SolrServerException {
        LukeRequest request = new LukeRequest();
        SolrServer server = new HttpSolrServer(solrUrl.toExternalForm());
        NamedList fields = (NamedList) server.request(request).get("fields");
        Set<String> fieldNames = new HashSet<>();
        for (int i = 0; i < fields.size(); i++) {
            fieldNames.add(fields.getName(i));
        }
        return fieldNames;
    }

    /**
     * Creates Spark RDD for results matching to solr query
     * @param query solr query
     * @return Spark RDD containing solr results
     */
    public RDD<SolrDocument> getSolrRDD(String query){
        return new SolrRDD(context.sc(), solrUrl, query);
    }


    public void run(){

        //Step: get the documents from solr which match to query and construct RDD
        JavaRDD<SolrDocument> solrRDD = getSolrRDD(solrQuery).toJavaRDD().cache();

        // Convert documents to vector form
        JavaRDD<Vector> vectorRDD = solrRDD.map(vectorizer.getVectorizer(this));

        //find all the pairs by doing cartesian product
        JavaPairRDD<Vector, Vector> cartesian = vectorRDD.cartesian(vectorRDD);

        //keep combinations only.
        // {a,b} x {a,b} => {(a,a), (a,b), (b,b), (b,a)}
        // this step removes (a,a), (b,b) and one of {(a,b), (b,a)},
        //this doesn't make sense with small data set, but visualize with large sets
        cartesian.filter(pair -> pair._1().id.hashCode() < pair._1().id.hashCode());

        //Find cosine of angle between these vector pairs
        JavaRDD<Tuple3<String, String, Double>> result = cartesian.map(pair ->
                new Tuple3<>(pair._1().id, pair._2().id, pair._1().cosθ(pair._2())));

        //store the result
        String outputFormat = "%s,%s,%f";
        result.map(row -> String.format(outputFormat, row._1(), row._2(), row._3()))
                .saveAsTextFile(output.getPath());

    }

    public static void main(String[] args) throws IOException, SolrServerException {

        long t1 = System.currentTimeMillis();
        SolrSimilarity solrSim = new SolrSimilarity();
        CmdLineParser parser = new CmdLineParser(solrSim);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(-1);
        }
        solrSim.init();
        solrSim.run();

        System.out.println("Done.., Time Taken :" + (System.currentTimeMillis() - t1) + "ms");
    }
}
