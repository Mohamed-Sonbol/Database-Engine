import java.io.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

public class Index implements java.io.Serializable{

    String tblName;
    Hashtable<String, Vector> NRanges = new Hashtable<>();
    Vector NDimensionalArray;
    String[] indexedCols;
    private static final long serialVersionUID = 7526472295622776147L;

    public Index(String tblName, int dimensions, String[] starrColNames){
        this.tblName = tblName;
        indexedCols = starrColNames;
        NDimensionalArray = createNDimensions(dimensions);

    }

    public void deleteFromNDimensionalArray(int d, int[] indices, Vector res, RecordReference e) throws IOException {
        if (d == 0) {
            Bucket b = Bucket.loadBucket((String) res.get(indices[indices.length-1]));
            b.recordReferences.remove(e);
            b.saveBucket(tblName,indices);
            return;
        }
        for (int i = 0; i < indices.length; i++) {
            insertIntoNDimensionalArray(d-1, indices, (Vector) res.get(indices[i]), e);
        }

    }




    public void insertIntoNDimensionalArray(int d, int[] indices, Vector res, RecordReference e) throws IOException {
        if (d == 1) {
            if(res.get(indices[indices.length-1]) == null){
                Bucket b = new Bucket();
                b.recordReferences.add(e);
                b.saveBucket(tblName,indices);
                res.add(indices[indices.length-1],b.path);
            }
            else{
                Bucket b = Bucket.loadBucket((String) res.get(indices[indices.length-1]));
                b.recordReferences.add(e);
                b.saveBucket(tblName,indices);
            }
            return;
        }
        for (int i = 0; i < indices.length; i++) {
            insertIntoNDimensionalArray(d-1, indices, (Vector) res.get(indices[i]), e);
        }

    }

    public Result selectFromNDimensionalArray(SQLTerm[] sqlTerm , int[] indices, Vector res, int d)  {
        Result result = new Result();
        if (d == 1) {
            if (res.get(indices[indices.length - 1]) == null) {
                return null;
            } else {
                Bucket b = Bucket.loadBucket((String) res.get(indices[indices.length - 1]));
                for (int j = 0; j < b.recordReferences.size(); j++) {
                    Page p = Page.loadPage(sqlTerm[0]._strTableName, b.recordReferences.get(j).pageIndex);
                    Row r = p.rows.get(b.recordReferences.get(j).recordIndex);
                    boolean accepted = true;
                    for (int z = 0; z < sqlTerm.length; z++) {
                        if (!r.data.get(sqlTerm[z]._strColumnName).equals(sqlTerm[z]._objValue)) {
                            accepted = false;
                            break;
                        }
                    }
                    if (accepted)
                        result.resultSet.add(r);
                }
            }
            return result;
        }
        for (int i = 0; i < indices.length; i++) {
            selectFromNDimensionalArray(sqlTerm, indices, (Vector) res.get(indices[i]), d - 1);
        }
        return null;
    }



    public Vector createNDimensions(int d){
        if(d >= 1){
            d--;
            Vector newArray = new Vector<>(10);
            for(int i = 0 ; i < 10 ; i++){
                newArray.add(createNDimensions(d));
            }
        return newArray;
        }
        else
            return null;
    }

    public void setRanges(String[] ColName, Object[] colsMin, Object[] colsMax){

        for(int i = 0 ; i < ColName.length ; i++)
            NRanges.put(ColName[i], new Vector(10));

        for(int i=0 ; i< colsMin.length ; i++){
            String name= ColName[i];
            String type= colsMin[i].getClass().getName();
            switch (type) {
                case "java.lang.Integer":
                    float minimumInteger = Float.parseFloat(colsMin[i].toString());
                    float maximumInteger = Float.parseFloat(colsMax[i].toString());
                    int rangeInteger = (int) Math.ceil((maximumInteger-minimumInteger+1)/10);
                    for(int j =0 ; j<10 ; j++){
                        Vector ranges = new Vector(2);
                        ranges.add((int)minimumInteger);
                        ranges.add((int)minimumInteger+rangeInteger);
                        minimumInteger+=rangeInteger+1;
                        NRanges.get(name).add(ranges);
                    }
                    break;
                case "java.util.Date":
                    //test date to localdate conv
                    Date min = (Date) colsMin[i];
                    Date max = (Date) colsMax[i];
                    LocalDate x = min.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                    LocalDate y = max.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                    long daysRange = (long)Math.ceil((ChronoUnit.DAYS.between(x, y)+1)/10);

                    for(int j =0 ; j<10 ; j++){
                        Vector ranges = new Vector(2);

                        ranges.add(x);
                        x = x.plusDays(daysRange);

                        ranges.add(x);

                        x = x.plusDays(1);
                        NRanges.get(name).add(ranges);
                    }
                    break;
                case "java.lang.String":
                    Long minimumString = convert(colsMin[i].toString().toLowerCase());
                    Long maximumString = convert(colsMax[i].toString().toLowerCase());
                    Long rangeString = (maximumString-minimumString+1)/10;
                    for(int j = 0; j<10; j++){
                        Vector ranges = new Vector(2);
                        ranges.add(minimumString);
                        ranges.add(minimumString+rangeString);
                        minimumString+=rangeString+1;
                        NRanges.get(name).add(ranges);
                    }
                    break;
                case "java.lang.Double":
                    double minimumDouble = Double.parseDouble(colsMin[i].toString());
                    double maximumDouble = Double.parseDouble(colsMax[i].toString());
                    double rangeDouble = (maximumDouble-minimumDouble+1)/10;
                    for(int j =0 ; j<10 ; j++){
                        Vector ranges = new Vector(2);
                        ranges.add(minimumDouble);
                        ranges.add(minimumDouble+rangeDouble);
                        minimumDouble+=rangeDouble+1;
                        NRanges.get(name).add(ranges);
                    }
                    break;
                default: break;
            }
        }
    }

    public void fillIndex() throws IOException {
        Table tbl = Table.loadTable(tblName);
        String clusterKey = tbl.clusterKey;
        tbl.indexed = true;
        Table.saveTable(tbl,tbl.name);
        int[] indices = new int[indexedCols.length];
        for(int pi = 0 ; pi < tbl.numOfPages ; pi++){
            Page p = Page.loadPage(tblName, pi);
            for (int ri = 0 ; ri < p.rows.size() ; ri++){
                Row r = p.rows.get(ri);
                for(int ci = 0 ; ci < indexedCols.length ; ci++){
                    Object o;
                    if(indexedCols[ci].equals(clusterKey))
                        o = r.clusteringKeyValue;
                    else
                        o = r.data.get(indexedCols[ci]);
                    Vector v = NRanges.get(indexedCols[ci]);
                    for(int vi = 0 ; vi < v.size() ; vi++){
                        Vector minMax = (Vector) v.get(vi);
                        if (compare(o,minMax.get(0)) >= 0 && compare(o,minMax.get(1)) <= 0) {
                            indices[ci] = vi;
                            break;
                        }
                    }
                }
                RecordReference e = new RecordReference(pi,ri);
                insertIntoNDimensionalArray(indexedCols.length, indices, NDimensionalArray, e);
            }
        }
        this.saveIndex(tblName , tbl.indicesPath.size());
    }

    public int compare(Object o1, Object o2) {
        String type = o1.getClass().getName();
        switch (type) {
            case "java.lang.Integer":
                Integer i1 = (Integer) o1;
                Integer i2 = (Integer) o2;
                return i1.compareTo(i2);
            case "java.util.Date":
                Date d3 = (Date) o1;
                LocalDate date = d3.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                LocalDate d4 = (LocalDate) o2;
                return date.compareTo(d4);
            case "java.lang.String":
                String s1 = (String) o1;
                Long l2 = (Long) o2;
                Long l1 = convert(s1);
                return l1.compareTo(l2);
            case "java.lang.Double":
                Double d1 = (Double) o1;
                Double d2 = (Double) o2;
                return d1.compareTo(d2);
            default: return -1;
        }
    }

    public static String charRemoveAt(String str, int p) {
        return str.substring(0, p) + str.substring(p + 1);
    }

    public static long convert(String str)
    {
        long result = 0;
        int v = 0;
        if(str.contains("-"))
            str = charRemoveAt(str,str.indexOf('-'));
        for (int i = 0;i<str.length();i++) {
            if(!Character.isDigit(str.charAt(i))) {
                v = (int) Math.pow(26, (str.length() - i - 1));
                result += (long) (str.charAt(i) - 97 + 1) * v;
            }
            else{
                v = (int) Math.pow(10, (str.length() - i - 1));
                result += (long) Integer.parseInt(str.charAt(i)+"")*v;
            }
        }
        return result;
    }

    public void saveIndex(String strTableName , int indx) throws IOException {
        Table tbl = Table.loadTable(strTableName);
        try {
            String s = "src/main/resources/data/indices/" + strTableName + "_index" + indx + "_index.ser";
            File index = new File(s);
            if(!tbl.indicesPath.contains(s)){
                tbl.indicesPath.add(s);
            }
            if(!index.exists()) {
                index.createNewFile();
            }
            FileOutputStream fileOut = new FileOutputStream(index);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public static Index loadIndex(String strTableName, int indx) {
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main/resources/data/indices/" + strTableName + "_index" + indx + "_index.ser");
            ObjectInputStream inputStream = new ObjectInputStream(fileInputStream);
            Index index = (Index) inputStream.readObject();
            inputStream.close();
            fileInputStream.close();
            return index;
        }
        catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return null;
    }

    public void updateNDimensionalArray(int d, int[] indices, Vector res, RecordReference e) throws IOException {
        if (d == 0) {

            Bucket b = Bucket.loadBucket((String) res.get(indices[indices.length-1]));
            b.recordReferences.set(indices[indices.length-1], e);
            b.saveBucket(tblName,indices);

            return;
        }
        for (int i = 0; i < indices.length; i++) {
            insertIntoNDimensionalArray(d-1, indices, (Vector) res.get(indices[i]), e);
        }

    }

    public static void main(String[] args){


    }



}
