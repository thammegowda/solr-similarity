package edu.usc.cs.ir.solr.similarity;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;
import scala.reflect.ClassTag;

import java.net.URL;

/**
 * This class accepts a solr query and server url and then builds Spark RDD for the
 * results matching to solr.
 * The current implementation is merely a prototype with one partition
 * @author Thamme Gowda N
 * @since November 27, 2015
 */
public class SolrRDD extends RDD<SolrDocument> {

    private static final ClassTag<SolrDocument> EVIDENCE_TAG =
            ClassManifestFactory$.MODULE$.fromClass(SolrDocument.class);

    private final String queryStr;
    private final URL solrUrl;

    /**
     * Creates Solr RDD
     * @param sc Spark context
     * @param solrUrl url to solr server, often includes core url
     * @param query the solr query for retrieving result set
     */
    public SolrRDD(SparkContext sc, URL solrUrl, String query) {
        super(sc, new ArrayBuffer<>(), EVIDENCE_TAG);
        this.solrUrl = solrUrl;
        this.queryStr = query;
    }


    @Override
    public Iterator<SolrDocument> compute(Partition split, TaskContext context) {
        String query = ((SolrResultPartition) split).getQuery();
        return new SolrDocIterator(new HttpSolrServer(solrUrl.toExternalForm()), query);
    }

    @Override
    public Partition[] getPartitions() {
        return new Partition[]{new SolrResultPartition(0, queryStr)};
    }
}
