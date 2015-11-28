package edu.usc.cs.ir.solr.similarity;

import org.apache.spark.Partition;

/**
 * The partitioner for solr result set
 */
public class SolrResultPartition implements Partition {

    private int index;
    private String query;

    /**
     * Creates solr partitioner
     * @param index partition index
     * @param query solr query
     */
    public SolrResultPartition(int index, String query) {
        this.index = index;
        this.query = query;
    }

    @Override
    public int index() {
        return index;
    }

    public String getQuery() {
        return query;
    }
}
