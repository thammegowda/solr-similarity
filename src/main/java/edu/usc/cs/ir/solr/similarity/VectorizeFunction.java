package edu.usc.cs.ir.solr.similarity;

import edu.usc.cs.ir.vsm.Vector;
import org.apache.solr.common.SolrDocument;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Vectorizer Contract. This function takes solrdocument and produces a vector
 *
 */
public interface VectorizeFunction extends Function<SolrDocument, Vector>,
        org.apache.spark.api.java.function.Function<SolrDocument, Vector>,
        Serializable {

    @Override
    default Vector call(SolrDocument v1) throws Exception {
        return apply(v1);
    }
}
