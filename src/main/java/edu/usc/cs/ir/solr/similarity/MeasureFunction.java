package edu.usc.cs.ir.solr.similarity;

import edu.usc.cs.ir.vsm.Vector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Contract for Measurement functions
 */
public interface MeasureFunction extends
        Function<Tuple2<Vector, Vector>, Double>,
        Serializable {
}
