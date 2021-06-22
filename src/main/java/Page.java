import java.io.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class Page implements java.io.Serializable{

    Object min;
    Object max;
    boolean overflowExists;
    int maxRows;
    Vector<Page> overflowPages;
    Vector<Row> rows;
    private static final long serialVersionUID = 7526472295622776147L;

    public Page(){
        overflowPages = new Vector<Page>();
        overflowExists=false;
        try {
            maxRows = getMaximumRowsCountinPage();
        } catch (IOException e) {
            e.printStackTrace();
        }
        rows = new Vector<Row>(maxRows);
    }

    public static int getMaximumRowsCountinPage() throws IOException {
        FileReader fileReader = new FileReader("src/main/resources/DBApp.config");
        Properties properties = new Properties();
        properties.load(fileReader);
        return Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
    }



    public void savePage(String strTableName, int pageIndex) throws IOException {
        try {
            File page = new File("src/main/resources/data/pages/" + strTableName + "_page" + pageIndex + ".ser");
            if(!page.exists()) {
                page.createNewFile();
            }
            FileOutputStream fileOut = new FileOutputStream(page);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public void deletePage(String strTableName, int pageIndex) throws IOException {

        File page = new File("src/main/resources/data/pages/" + strTableName + "_page" + pageIndex + ".ser");
        page.delete();

        Table tbl = Table.loadTable(strTableName);

        for(int i = pageIndex+1; i<tbl.numOfPages; i++){
            Page p = loadPage(strTableName,i);
            p.savePage(strTableName,i-1);
        }

        tbl.minOfPages.remove(pageIndex);
        tbl.maxOfPages.remove(pageIndex);
        tbl.numOfPages--;
    }

    public static Page loadPage(String strTableName, int pageIndex) {
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main/resources/data/pages/" + strTableName + "_page" + pageIndex + ".ser");
            ObjectInputStream inputStream = new ObjectInputStream(fileInputStream);
            Page page = (Page) inputStream.readObject();
            inputStream.close();
            fileInputStream.close();
            return page;
        }
        catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return null;
    }

    public void insertInPage(Row input, Table tbl, Page p, Integer pageIndex) throws IOException, DBAppException {
        if(rows.contains(input)) {
            System.out.println(input.data.get("id"));
            System.out.print(pageIndex);
            throw new DBAppException("Duplicate record");
        }

        Row x;
        if(rows.size() == maxRows){
            x = rows.remove(maxRows - 1);
            rows.add(input);
            ++pageIndex;
            p.insertInPage(x, tbl, null, pageIndex);
        }
        else
            rows.add(input);

        rows.sort(new Comparator<Row>() {
            @Override
            public int compare(Row o1, Row o2) {
                String type = o1.clusteringKeyValue.getClass().getName();
                switch (type) {
                    case "java.lang.Integer":
                        Integer i1 = (Integer) o1.clusteringKeyValue;
                        Integer i2 = (Integer) o2.clusteringKeyValue;
                        return i1.compareTo(i2);
                    case "java.util.Date":
                        Date d3 = (Date) o1.clusteringKeyValue;
                        Date d4 = (Date) o2.clusteringKeyValue;
                        return d3.compareTo(d4);
                    case "java.lang.String":
                        String s1 = (String) o1.clusteringKeyValue;
                        String s2 = (String) o2.clusteringKeyValue;
                        return s1.compareTo(s2);
                    case "java.lang.Double":
                        Double d1 = (Double) o1.clusteringKeyValue;
                        Double d2 = (Double) o2.clusteringKeyValue;
                        return d1.compareTo(d2);
                    default: return -1;
                }
            }
        });

        min = rows.firstElement().clusteringKeyValue;
        max = rows.lastElement().clusteringKeyValue;

        if(tbl.indexed){
            for(int j = 0; j < tbl.indicesPath.size() ; j++){
            Index i = Index.loadIndex(tbl.name, j);
            RecordReference e = new RecordReference(pageIndex,rows.indexOf(input));
            int[] indices = new int[i.indexedCols.length];
            for(int ci = 0 ; ci < i.indexedCols.length ; ci++){
                Object o = input.data.get(i.indexedCols[ci]);
                Vector v = i.NRanges.get(i.indexedCols[ci]);
                for(int vi = 0 ; vi < v.size() ; vi++){
                    Vector minMax = (Vector) v.get(vi);
                    if (compare(o,minMax.get(0)) >= 0 && compare(o,minMax.get(1)) <= 0) {
                        indices[ci] = vi;
                        break;
                    }
                }
            }
            i.insertIntoNDimensionalArray(i.indexedCols.length,indices,i.NDimensionalArray,e);
            i.saveIndex(tbl.name, j);
        }
        }
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

    public static long convert(String str)
    {
        //String specialChars = "-/*!@#$%^&*()\"{}_[]|\\?/<>,.";
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

    public static String charRemoveAt(String str, int p) {
        return str.substring(0, p) + str.substring(p + 1);
    }

    public void updatePage(Row update, int pageIndex, String tblname) throws DBAppException, IOException {
        Table tbl = Table.loadTable(tblname);
        boolean flag = false;
        if(rows.contains(update)){
            rows.set(rows.indexOf(update),update);
        }
        else if(overflowExists){
            for (int i = 0; i < overflowPages.size(); i++){
                if(overflowPages.get(i).rows.contains(update)){
                    overflowPages.get(i).rows.set(rows.indexOf(update),update);
                    flag = true;
                    break;
                }
            }
            if(!flag)
                throw new DBAppException("Record doesn't exist!");
        }
        else
            throw new DBAppException("Record doesn't exist!");

        if(tbl.indexed){
            for(int j = 0; j < tbl.indicesPath.size() ; j++){
            Index I = Index.loadIndex(tbl.name, j);
            RecordReference e = new RecordReference(pageIndex,rows.indexOf(update));
            int[] indices = new int[I.indexedCols.length];
            for(int ci = 0 ; ci < I.indexedCols.length ; ci++){
                Object o = update.data.get(I.indexedCols[ci]);
                Vector v = I.NRanges.get(I.indexedCols[ci]);
                for(int vi = 0 ; vi < v.size() ; vi++){
                    Vector minMax = (Vector) v.get(vi);
                    if (compare(o,minMax.get(0)) >= 0 && compare(o,minMax.get(1)) <= 0) {
                        indices[ci] = vi;
                        break;
                    }
                }
            }
            I.updateNDimensionalArray(I.indexedCols.length,indices,I.NDimensionalArray,e);
            I.saveIndex(tbl.name, j);
        }
        }
    }

    public boolean deleteFromPageLinearSearch(int index, Row x, Table tbl) throws DBAppException, IOException {
        boolean flag = false;
        for(int i = 0; i<rows.size();i++){
            if(compareRows(rows.get(rows.indexOf(i)),x)){
                rows.remove(i);
                flag = true;
            }
        }
        if(tbl.indexed){
            for(int j = 0; j < tbl.indicesPath.size() ; j++){
            Index i = Index.loadIndex(tbl.name, j);
            RecordReference e = new RecordReference(index,rows.indexOf(x));
            int[] indices = new int[i.indexedCols.length];
            for(int ci = 0 ; ci < i.indexedCols.length ; ci++){
                Object o = x.data.get(i.indexedCols[ci]);
                Vector v = i.NRanges.get(i.indexedCols[ci]);
                for(int vi = 0 ; vi < v.size() ; vi++){
                    Vector minMax = (Vector) v.get(vi);
                    if (compare(o,minMax.get(0)) >= 0 && compare(o,minMax.get(1)) <= 0) {
                        indices[ci] = vi;
                        break;
                    }
                }
            }
            i.deleteFromNDimensionalArray(i.indexedCols.length,indices,i.NDimensionalArray,e);
            i.saveIndex(tbl.name, j);
        }
        }
        return flag;


    }

    public void deleteFromPageBinarySearch(int index, Row x, Table tbl) throws DBAppException, IOException {

        boolean flag = false;
        if(rows.contains(x)){
            if(compareRows(rows.get(rows.indexOf(x)),x)){

                rows.remove(x);

                if(overflowExists) {
                    insertInPage(overflowPages.lastElement().rows.lastElement(), tbl, null, index);
                    overflowPages.lastElement().rows.remove(overflowPages.lastElement().rows.size() - 1);
                    if (overflowPages.lastElement().rows.size() == 0)
                        overflowPages.remove(overflowPages.size() - 1);
                }

                else if(rows.size() == 0)
                    deletePage(tbl.name, index);
                else{
                    rows.sort(new Comparator<Row>() {
                        @Override
                        public int compare(Row o1, Row o2) {
                            String type = o1.clusteringKeyValue.getClass().getName();
                            switch (type) {
                                case "java.lang.Integer":
                                    Integer i1 = (Integer) o1.clusteringKeyValue;
                                    Integer i2 = (Integer) o2.clusteringKeyValue;
                                    return i1.compareTo(i2);
                                case "java.util.Date":
                                    Date d3 = (Date) o1.clusteringKeyValue;
                                    Date d4 = (Date) o2.clusteringKeyValue;
                                    return d3.compareTo(d4);
                                case "java.lang.String":
                                    String s1 = (String) o1.clusteringKeyValue;
                                    String s2 = (String) o2.clusteringKeyValue;
                                    return s1.compareTo(s2);
                                case "java.lang.Double":
                                    Double d1 = (Double) o1.clusteringKeyValue;
                                    Double d2 = (Double) o2.clusteringKeyValue;
                                    return d1.compareTo(d2);
                                default: return -1;
                            }
                        }
                    });

                    min = rows.firstElement().clusteringKeyValue;
                    max = rows.lastElement().clusteringKeyValue;


                    tbl.minOfPages.set(index,min);
                    tbl.maxOfPages.set(index,max);
                    Table.saveTable(tbl,tbl.name);
                }
            }
        }
        else if(overflowExists){
            for (int i = 0; i < overflowPages.size(); i++){
                if(overflowPages.get(i).rows.contains(x)){
                    if(compareRows(overflowPages.get(i).rows.get(overflowPages.get(i).rows.indexOf(x)),x)) {
                        overflowPages.get(i).rows.remove(x);
                        flag = true;
                        if (overflowPages.get(i).rows.size() == 0)
                            overflowPages.remove(i);
                        else {
                            overflowPages.get(i).rows.sort(new Comparator<Row>() {
                                @Override
                                public int compare(Row o1, Row o2) {
                                    String type = o1.clusteringKeyValue.getClass().getName();
                                    switch (type) {
                                        case "java.lang.Integer":
                                            Integer i1 = (Integer) o1.clusteringKeyValue;
                                            Integer i2 = (Integer) o2.clusteringKeyValue;
                                            return i1.compareTo(i2);
                                        case "java.util.Date":
                                            Date d3 = (Date) o1.clusteringKeyValue;
                                            Date d4 = (Date) o2.clusteringKeyValue;
                                            return d3.compareTo(d4);
                                        case "java.lang.String":
                                            String s1 = (String) o1.clusteringKeyValue;
                                            String s2 = (String) o2.clusteringKeyValue;
                                            return s1.compareTo(s2);
                                        case "java.lang.Double":
                                            Double d1 = (Double) o1.clusteringKeyValue;
                                            Double d2 = (Double) o2.clusteringKeyValue;
                                            return d1.compareTo(d2);
                                        default: return -1;
                                    }
                                }
                            });
                            if(overflowPages.get(i).min.equals(min))
                                min = tbl.nextLowestMin;
                            else if(overflowPages.get(i).max.equals(max))
                                max = tbl.nextHighestMax;

                            overflowPages.get(i).min = rows.firstElement().clusteringKeyValue;
                            overflowPages.get(i).max = rows.lastElement().clusteringKeyValue;
                            Table.saveTable(tbl,tbl.name);
                        }
                        break;
                    }
                }
            }
            if(!flag)
                throw new DBAppException("Record doesn't exist!");
        }
        else
            throw new DBAppException("Record doesn't exist!");

        if(tbl.indexed){
            for(int j = 0; j < tbl.indicesPath.size() ; j++){
            Index i = Index.loadIndex(tbl.name, j);
            RecordReference e = new RecordReference(index,rows.indexOf(x));
            int[] indices = new int[i.indexedCols.length];
            for(int ci = 0 ; ci < i.indexedCols.length ; ci++){
                Object o = x.data.get(i.indexedCols[ci]);
                Vector v = i.NRanges.get(i.indexedCols[ci]);
                for(int vi = 0 ; vi < v.size() ; vi++){
                    Vector minMax = (Vector) v.get(vi);
                    if (compare(o,minMax.get(0)) >= 0 && compare(o,minMax.get(1)) <= 0) {
                        indices[ci] = vi;
                        break;
                    }
                }
            }
            i.deleteFromNDimensionalArray(i.indexedCols.length,indices,i.NDimensionalArray,e);
            i.saveIndex(tbl.name, j);
        }
        }

    }

    public boolean compareRows(Row x, Row y) {
        Set<String> dataOfY = y.data.keySet();
        for (String colName : dataOfY) {
            if(!y.data.get(colName).equals(x.data.get(colName)))
                return false;
        }
        return true;
    }
}
