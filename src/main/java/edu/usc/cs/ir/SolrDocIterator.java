package edu.usc.cs.ir;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.AbstractIterator;

import java.util.Iterator;

/**
 * Iterates over solr result set.
 *
 *
 */
public class SolrDocIterator extends AbstractIterator<SolrDocument> implements Iterator<SolrDocument> {

    public static final Logger LOG = LoggerFactory.getLogger(SolrDocIterator.class);
    public static final int DEF_START = 0;
    public static final int DEF_ROWS = 1000;
    private SolrServer solr;

    private SolrQuery query;
    private long numFound;
    private int nextStart;
    private Iterator<SolrDocument> curPage;
    private SolrDocument next;
    private long count = 0;
    private long limit = Long.MAX_VALUE;

    public SolrDocIterator(String solrUrl, String queryStr, int start, int rows,
                           String...fields){
        this(new HttpSolrServer(solrUrl), queryStr, start, rows, fields);
    }

    public SolrDocIterator(SolrServer solr, String queryStr, String...fields) {
        this(solr, queryStr, DEF_START, DEF_ROWS, fields);
    }

    public SolrDocIterator(SolrServer solr, String queryStr, int start, int rows,
                           String...fields){
        this.solr = solr;
        this.nextStart = start;
        this.query = new SolrQuery(queryStr);
        this.query.setRows(rows);
        if (fields.length > 0) {
            this.query.setFields(fields);
        }
        this.numFound = start + 1;

        //lets assume at least one more doc left, so the next page call wil happen
        this.next = getNext(true);
        this.count = 1;
    }

    public void setFields(String...fields) {
        this.query.setFields(fields);
    }

    public long getNumFound() {
        return numFound;
    }

    public int getNextStart() {
        return nextStart;
    }


    public void setLimit(long limit) {
        this.limit = limit;
    }

    public SolrDocumentList queryNext()  {
        query.setStart(nextStart);
        try {
            LOG.debug("Query {}, Start = {}", query.getQuery(), nextStart);
            QueryResponse response = solr.query(query);
            this.numFound = response.getResults().getNumFound();
            return response.getResults();
        } catch (SolrServerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public SolrDocument next() {
        SolrDocument tmp = next;
        next = getNext(false);
        count++;
        return tmp;
    }

    private SolrDocument getNext(boolean forceFetch) {
        if (forceFetch || !curPage.hasNext() && nextStart < numFound) {
            //there is more
            SolrDocumentList page = queryNext();
            this.numFound = page.getNumFound();
            this.nextStart += page.size();
            this.curPage = page.iterator();
        }
        return count < limit && curPage.hasNext() ? curPage.next() : null;
    }
}
