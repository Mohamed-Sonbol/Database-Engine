import java.util.Vector;

public class Result implements java.util.Iterator{

    Vector<Row> resultSet = new Vector<Row>();

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Object next() {
        return null;
    }
}
