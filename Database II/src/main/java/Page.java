import java.io.*;
import java.util.*;

public class Page implements java.io.Serializable{

    Object min;
    Object max;
    boolean overflowExists;
    int maxRows;
    Vector<Page> overflowPages;
    Vector<Row> rows;

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

    public void insertInPage(Row input, Table tbl, Page p, Integer index) throws IOException, DBAppException {
        if(rows.contains(input))
            throw new DBAppException("Duplicate record");

        Row x;
        if(rows.size() == maxRows){
            x = rows.remove(maxRows - 1);
            rows.add(input);
            ++index;
            p.insertInPage(x, tbl, null, index);
            index--;
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
        savePage(tbl.name, index);
    }

    public void updatePage(Row update, int index, String tblname) throws DBAppException, IOException {
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

        savePage(tblname, index);
    }

    public void deleteFromPageLinearSearch(int index, Row x, Table tbl) throws DBAppException, IOException {
        for(int i = 0; i<rows.size();i++){
            if(compareRows(rows.get(rows.indexOf(i)),x)){
                rows.remove(i);
            }
        }
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
                    savePage(tbl.name, index);


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
                            savePage(tbl.name, index);
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
