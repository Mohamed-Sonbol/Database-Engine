import java.io.*;
import java.util.Date;
import java.util.Vector;

public class Table implements java.io.Serializable{

    String name;
    String clusterKey;
    int numOfPages;
    Vector<Object> minOfPages;
    Vector<Object> maxOfPages;
    Object nextLowestMin;
    Object nextHighestMax;


    public Table (String name, String clusterKey){
        this.name = name;
        this.clusterKey = clusterKey;
        this.numOfPages = 0;
        this.minOfPages = new Vector<Object>();
        this.maxOfPages = new Vector<Object>();
        this.nextLowestMin = "";
        this.nextHighestMax = "";
    }

    public void insertInTable(Row input, String tableName) throws IOException, DBAppException {
        int c = 0;
        Table tbl = loadTable(tableName);
        Object clk = input.clusteringKeyValue;
        int first = 0;
        int last = tbl.numOfPages - 1;
        int mid = (first + last)/2;
        if(tbl.numOfPages == 0){
            Page p = new Page();
            p.insertInPage(input, tbl, null, tbl.numOfPages);
            tbl.minOfPages.add(p.min);
            tbl.maxOfPages.add(p.max);
            ++tbl.numOfPages;
            saveTable(tbl,tableName);
        }
        else {
            while(mid < tbl.numOfPages){
                if(mid == tbl.numOfPages-1){
                    Page p = Page.loadPage(tableName, mid);
                    if (p.rows.size() == p.maxRows) {
                        Page p1 = new Page();
                        p1.insertInPage(input, tbl, null, tbl.numOfPages);
                        ++tbl.numOfPages;
                        tbl.minOfPages.add(p1.min);
                        tbl.maxOfPages.add(p1.max);
                        saveTable(tbl,tableName);
                    }
                    else {
                        p.insertInPage(input, tbl, null, mid);
                        tbl.minOfPages.remove(tbl.numOfPages-1);
                        tbl.minOfPages.add(tbl.numOfPages-1, p.min);
                        tbl.maxOfPages.remove(tbl.numOfPages-1);
                        tbl.maxOfPages.add(tbl.numOfPages-1, p.max);
                        saveTable(tbl,tableName);
                    }
                    break;
                }
                else if(compare(clk, tbl.maxOfPages.get(mid)) >= 1 && compare(clk, tbl.minOfPages.get(mid+1)) <= -1){
                    Page p = Page.loadPage(tableName, mid);
                    Page p1 = Page.loadPage(tableName, mid+1);
                    overflowCheck(p, p1, input, tbl, mid);
                    break;
                }
                else if(compare(clk, tbl.minOfPages.get(mid)) <= -1){
                    if(mid == 0){
                        Page p = Page.loadPage(tableName, mid);
                        Page p1 = Page.loadPage(tableName, mid+1);
                        overflowCheck(p, p1, input, tbl, mid);
                        break;
                    }
                    last = mid - 1;
                    mid = (first+last)/2;
                }
                else if(compare(clk, tbl.maxOfPages.get(mid)) >= 1) {
                    first = mid + 1;
                    mid = (first+last)/ 2;
                }
                else if(compare(clk, tbl.maxOfPages.get(mid)) < 1){
                    Page p = Page.loadPage(tableName, mid);
                    Page p1 = Page.loadPage(tableName, mid+1);
                    overflowCheck(p, p1, input, tbl, mid);
                    break;
                }
                c++;
            }
        }
    }

    public void overflowCheck(Page mid, Page next, Row input, Table tbl, Integer index) throws IOException, DBAppException {
        if(mid.overflowExists) {
            if(mid.overflowPages.lastElement().rows.size() == mid.maxRows){
                Page x = new Page();
                mid.overflowPages.add(x);
            }
            mid.overflowPages.lastElement().insertInPage(input, tbl, null, index);
            if(compare(mid.overflowPages.lastElement().min, mid.min) == -1){
                tbl.nextLowestMin = tbl.minOfPages.get(index);
                mid.min = mid.overflowPages.lastElement().min;
                tbl.minOfPages.set(index,mid.overflowPages.lastElement().min);
            }
            else if(compare(mid.overflowPages.lastElement().max, mid.max) == 1){
                tbl.nextHighestMax = tbl.maxOfPages.get(index);
                mid.max = mid.overflowPages.lastElement().max;
                tbl.maxOfPages.set(index,mid.overflowPages.lastElement().max);
            }
        }
        else if(mid.rows.size() == mid.maxRows && next.rows.size() == mid.maxRows){
            Page x = new Page();
            mid.overflowPages.add(x);
            mid.overflowPages.lastElement().insertInPage(input, tbl, null, index);
            if(compare(mid.overflowPages.lastElement().min, mid.min) == -1){
                mid.min = mid.overflowPages.lastElement().min;
                tbl.minOfPages.set(index,mid.overflowPages.lastElement().min);
            }
            else if(compare(mid.overflowPages.lastElement().max, mid.max) == 1){
                mid.max = mid.overflowPages.lastElement().max;
                tbl.maxOfPages.set(index,mid.overflowPages.lastElement().max);
            }
            mid.overflowExists = true;
        }
        else{
            mid.insertInPage(input, tbl, next, index);
            tbl.minOfPages.set(index, mid.min);
            tbl.maxOfPages.set(index, mid.max);
        }
        saveTable(tbl,tbl.name);
    }
    public void updateTable(Row update, Table tbl) throws DBAppException, IOException {
        Object clk = update.clusteringKeyValue;
        int first = 0;
        int last = tbl.numOfPages - 1;
        int mid = (first + last)/2;
        while(mid < tbl.numOfPages){
            if(compare(clk, tbl.minOfPages.get(0)) <= -1 || compare(clk, tbl.maxOfPages.lastElement()) >= 1){
                throw new DBAppException("This record doesn't exist");
            }
            else if((mid != numOfPages - 1) && compare(clk, tbl.maxOfPages.get(mid)) >= 1 && compare(clk, tbl.minOfPages.get(mid+1)) <= -1){
                throw new DBAppException("This record doesn't exist");
            }
            else if(compare(clk, tbl.minOfPages.get(mid)) <= -1){
                last = mid - 1;
                mid = (first+last)/2;
            }
            else if(compare(clk, tbl.maxOfPages.get(mid)) >= 1) {
                first = mid + 1;
                mid = (first+last)/ 2;
            }
            else if(compare(clk, tbl.maxOfPages.get(mid)) < 1){
                Page p = Page.loadPage(tbl.name, mid);
                p.updatePage(update, mid, tbl.name);
                break;
            }
        }
    }

    public void deleteFromTableBinarySearch(Table tbl, Row x) throws DBAppException, IOException {
            int first = 0;
            int last = tbl.numOfPages - 1;
            int mid = (first + last)/2;
            while(mid < tbl.numOfPages){
                if(compare(x.clusteringKeyValue, tbl.minOfPages.get(0)) <= -1 || compare(x.clusteringKeyValue, tbl.maxOfPages.lastElement()) >= 1){
                    throw new DBAppException("This record doesn't exist");
                }
                if((mid != numOfPages - 1) && compare(x.clusteringKeyValue, tbl.maxOfPages.get(mid)) >= 1 && compare(x.clusteringKeyValue, tbl.minOfPages.get(mid+1)) <= -1){
                    throw new DBAppException("This record doesn't exist");
                }
                else if(compare(x.clusteringKeyValue, tbl.minOfPages.get(mid)) <= -1){
                    last = mid - 1;
                    mid = (first+last)/2;
                }
                else if(compare(x.clusteringKeyValue, tbl.maxOfPages.get(mid)) >= 1) {
                    first = mid + 1;
                    mid = (first+last)/ 2;
                }
                else if(compare(x.clusteringKeyValue, tbl.maxOfPages.get(mid)) < 1){
                    Page p = Page.loadPage(tbl.name, mid);
                    p.deleteFromPageBinarySearch(mid,x,tbl);
                    break;
                }
            }

    }

    public void deleteFromTableLinearSearch(Table tbl, Row x) throws DBAppException, IOException {
        for(int i = 0; i< tbl.numOfPages;i++){
            Page p = Page.loadPage(tbl.name,i);
            p.deleteFromPageLinearSearch(i, x, tbl);
        }
        for(int i = 0; i< tbl.numOfPages;i++){
            Page p = Page.loadPage(tbl.name,i);
            if(p.rows.size() == 0)
                p.deletePage(tbl.name,i);
        }
    }

    public static void saveTable(Table tbl, String tblName) throws IOException {
        try {
            File table = new File("src/main/resources/data/tables/" + tblName +  ".ser");
            if(!table.exists()) {
                table.createNewFile();
            }
            FileOutputStream fileOut = new FileOutputStream(table);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(tbl);
            out.close();
            fileOut.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public static Table loadTable(String tblName) {
        try {
            FileInputStream fileInputStream = new FileInputStream("src/main/resources/data/tables/" + tblName + ".ser");
            ObjectInputStream inputStream = new ObjectInputStream(fileInputStream);
            Table table = (Table) inputStream.readObject();
            inputStream.close();
            fileInputStream.close();
            return table;
        }
        catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return null;
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
                Date d4 = (Date) o2;
                return d3.compareTo(d4);
            case "java.lang.String":
                String s1 = (String) o1;
                String s2 = (String) o2;
                return s1.compareTo(s2);
            case "java.lang.Double":
                Double d1 = (Double) o1;
                Double d2 = (Double) o2;
                return d1.compareTo(d2);
            default: return -1;
        }
    }
}
