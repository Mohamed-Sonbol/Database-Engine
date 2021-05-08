import java.util.Date;
import java.util.Hashtable;

public class Row implements java.io.Serializable {

    Object clusteringKeyValue;
    Hashtable<String, Object> data;

    public Row(Object clusteringKeyValue) {
        this.clusteringKeyValue = clusteringKeyValue;
        data = new Hashtable<>();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o.getClass() != this.getClass())
            return false;
        Row z = (Row) o;
        String type = this.clusteringKeyValue.getClass().getName();
        switch (type) {
            case "java.lang.Integer":
                Integer i1 = (Integer) this.clusteringKeyValue;
                Integer i2 = (Integer) z.clusteringKeyValue;
                return i1.equals(i2);
            case "java.util.Date":
                Date d3 = (Date) this.clusteringKeyValue;
                Date d4 = (Date) z.clusteringKeyValue;
                return d3.equals(d4);
            case "java.lang.String":
                String s1 = (String) this.clusteringKeyValue;
                String s2 = (String) z.clusteringKeyValue;
                return s1.equals(s2);
            case "java.lang.Double":
                Double d1 = (Double) this.clusteringKeyValue;
                Double d2 = (Double) z.clusteringKeyValue;
                return d1.equals(d2);
            default: return false;
        }

    }

}
