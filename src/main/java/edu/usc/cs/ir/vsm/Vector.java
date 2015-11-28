package edu.usc.cs.ir.vsm;

import java.io.Serializable;
import java.util.TreeMap;

/**
 * An instance of this class represents a vector in n dimensional vector space.
 * @author Thamme Gowda N
 * @see <a href="https://github.com/thammegowda/notes/blob/master/java/src/main/java/me/gowdru/notes/ir/vsm/Vector.java"> TG's Notes</a>
 *
 */
public class Vector implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String id;

    public long[] directions;
    public double[] magnitudes;

    public Vector(String id) {
        this.id = id;
    }

    /**
     * Creates a vector
     * @param vectorId identifier for vector
     * @param features map of dimension -> magnitude entries. Requires entries to be sorted in ascending order of
     *                 dimension numbers
     */
    public Vector(String vectorId, TreeMap<Long, Double> features) {
        this(vectorId);
        this.directions = new long[features.size()];
        this.magnitudes = new double[features.size()];
        int idx = 0;
        for (Long dimension : features.keySet()) {
            this.directions[idx] = dimension;
            this.magnitudes[idx] = features.get(dimension);
            idx++;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("(").append(id).append("):{");
        if (directions != null && magnitudes != null) {
            for (int i = 0; i < directions.length; i++) {
                builder.append(directions[i]).append(":").append(magnitudes[i]).append(' ');
            }
        }
        builder.append("}");
        return builder.toString();
    }

    /**
     * Computes magnitude of this vector
     * @return magnitude |this|
     */
    public double getMagnitude(){
        //v1 = ax + by + cz             #a,b,c are magnitude across x,y,z
        //|v1| = sqrt(a^2 + b^2 + c^2)  # based on pythagoras theorem, mid way b/w the point and the origin
        double magnitude = 0.0;
        for (double scale : magnitudes) {
            magnitude += scale * scale;
        }
        return Math.sqrt(magnitude);
    }

    /**
     * Computes Dot product of this vector with another vector
     * @param anotherVector another vector
     * @return dot (.) product
     */
    public double dotProduct(Vector anotherVector) {
        // A = ax+by+cz
        // B = mx+ny+oz
        // A.B = a*m + b*n + c*o
        Vector v1 = this;
        Vector v2 = anotherVector;
        /* Vector dimensions are sparsely represented, so absent dimensions implies zero magnitude */
        //assumption : feature dimensions in vector are sorted
        double product = 0.0;
        int i = 0; //v1 feature index
        int j = 0; //v2 feature index
        while (i < v1.directions.length && j < v2.directions.length) {
            if (v1.directions[i] == v2.directions[j]) {
                //both dimensions are found
                product += v1.magnitudes[i] * v2.magnitudes[j];
                i++;
                j++;
            } else if (v1.directions[i] < v2.directions[j]){
                // skip i
                i++;
            } else {
                //skip j
                j++;
            }
        }
        return product;
    }


    /**
     * Computes cosine angle between this vector and another vector
     * @param v2 another vector
     * @return cosine of angle between this vector and argument vector
     */
    public double cosθ(Vector v2) {
        Vector v1 = this;
        // V1.V2 = |V1| |V2| Cos(θ)
        //Cos(θ) = (V1.V2) / (|V1| |V2|)
        return v1.dotProduct(v2) / (v1.getMagnitude() * v2.getMagnitude());
    }
}
