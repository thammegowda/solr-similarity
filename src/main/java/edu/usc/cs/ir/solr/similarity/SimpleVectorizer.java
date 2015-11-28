package edu.usc.cs.ir.solr.similarity;

import edu.usc.cs.ir.vsm.Vector;
import org.apache.solr.common.SolrDocument;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * This implementation of vectorizer uses field names in document as dimension in vector and
 * length of values as magnitudes. As of now, this is merely a key based vectorizer.
 */
public class SimpleVectorizer implements VectorizeFunction, Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, Long> dict;

    /**
     * Creates a vectorizer.
     * @param dict dictionary for mapping features to vectors
     */
    public SimpleVectorizer(Map<String, Long> dict) {
        this.dict = dict;
    }

    @Override
    public Vector apply(SolrDocument doc)  {
        TreeMap<Long, Double> vect = new TreeMap<>();
        doc.forEach((name,val) -> {
            Long dimension = dict.get(name);
            if (dimension != null) {
                int valLength = 0;
                if (val instanceof Collection) {
                    Collection coll = (Collection) val;
                    for (Object o : coll) {
                        valLength += o.toString().length();
                    }
                } else {
                    valLength = val.toString().length();
                }
                vect.put(dimension, valLength * 1.0);
            }
        });
        return new Vector(doc.get("id").toString(), vect);
    }
}
