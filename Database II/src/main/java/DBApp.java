import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DBApp implements DBAppInterface{

    String metaData = "";

    public void init() {

        metaData += "Table Name, Column Name, Column Type, ClusteringKey, Indexed, min, max\n";

    }

    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax ) throws DBAppException {

        Set<String> colNames = htblColNameType.keySet();
        String ck = "";
        for(String colName: colNames){
            if(colName.equals(strClusteringKeyColumn))
                ck = colName;
            metaData += strTableName + "," + colName + "," + htblColNameType.get(colName) + "," + colName.equals(strClusteringKeyColumn) + ",False," +
                    htblColNameMin.get(colName) + "," + htblColNameMax.get(colName) + "\n";
        }

        try {
            FileWriter csvWriter = new FileWriter("src/main/resources/metadata.csv");
            csvWriter.append(metaData);
            csvWriter.flush();
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Table x = new Table(strTableName,ck);
        try {
            Table.saveTable(x,strTableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {

    }

    public boolean checkTableExists(String tableName){

        String row;
        boolean exists = false;

        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(tableName.equals(data[0])){
                    exists = true;
                    break;
                }
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return exists;

    }

    public boolean checkClusteringKey(String clusterKey, Hashtable<String, Object> htblColNameValue){
        Set<String> colNamesInput = htblColNameValue.keySet();
        for(String colName: colNamesInput){
            if(clusterKey.equals(colName))
                return true;
        }
        return false;
    }

    public String getClusteringKey(String tableName){

        String row = "";
        String key = "";

        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(data[0].equals(tableName) && data[3].equals("true"))
                    key = data[1];
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return key;

    }

    public String getClusteringKeyType(String tableName){

        String row = "";
        String key = "";

        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(data[0].equals(tableName) && data[3].equals("true"))
                    key = data[2];
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return key;

    }

    public boolean checkColumnNames(String strTableName, Hashtable<String, Object> htblColNameValue){
        ArrayList<String> colNames = getColumnNames(strTableName);
        Set<String> colNamesInput = htblColNameValue.keySet();
        for(String colName: colNamesInput){
            if(!colNames.contains(colName))
                return false;
        }
        return true;
    }

    public ArrayList<String> getColumnNames(String tableName){
        String row;
        ArrayList<String> colNames = new ArrayList<String>();
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(tableName.equals(data[0]))
                    colNames.add(data[1]);
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return colNames;
    }

    public boolean checkColumnTypes(String strTableName, Hashtable<String, Object> htblColNameValue){
        Hashtable<String,String> colNameTypes = getColumnTypes(strTableName);
        Set<String> colNameTypesInput = htblColNameValue.keySet();
        Set<String> colNameTypesTable = colNameTypes.keySet();
        for(String colNameTypeInput: colNameTypesInput){
            String typeInput = htblColNameValue.get(colNameTypeInput).getClass().getName();
            for(String colNameTypeTable: colNameTypesTable){
                if(colNameTypeInput.equals(colNameTypeTable)){
                    if(!typeInput.equals(colNameTypes.get(colNameTypeTable)))
                        return false;
                }
            }
        }
        return true;
    }

    public Hashtable getColumnTypes(String tableName){
        String row;
        Hashtable colTypes = new Hashtable();

        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(tableName.equals(data[0]))
                    colTypes.put(data[1],data[2]);
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return colTypes;
    }

    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {

        if(!checkTableExists(strTableName))
            throw new DBAppException("Table does not exist!");

        if(!checkClusteringKey(getClusteringKey(strTableName), htblColNameValue))
            throw new DBAppException("Invalid cluster key!");

        if(!checkColumnNames(strTableName, htblColNameValue))
            throw new DBAppException("Invalid column names!");

        if(!checkColumnTypes(strTableName, htblColNameValue))
            throw new DBAppException("Invalid column types!");

        Set<String> colNames = htblColNameValue.keySet();

        Row x = new Row(htblColNameValue.get(getClusteringKey(strTableName)));
        for (String colName: colNames) {
            x.data.put(colName,htblColNameValue.get(colName));
        }

        Table tbl = Table.loadTable(strTableName);
        try {
            tbl.insertInTable(x,strTableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue ) throws DBAppException {
        if(!checkTableExists(strTableName))
            throw new DBAppException("Table does not exist!");

        if(!checkColumnNames(strTableName, htblColNameValue))
            throw new DBAppException("Invalid column names!");

        if(!checkColumnTypes(strTableName, htblColNameValue))
            throw new DBAppException("Invalid column types!");


        Table x = Table.loadTable(strTableName);
        Set<String> colNames = htblColNameValue.keySet();

        String clusteringKeyType = getClusteringKeyType(strTableName);
        Object clusteringKeyValue;

        switch (clusteringKeyType) {
            case "java.lang.Integer":
                clusteringKeyValue = Integer.parseInt(strClusteringKeyValue);break;
            case "java.util.Date":
                int year = Integer.parseInt(strClusteringKeyValue.trim().substring(0, 4));
                int month = Integer.parseInt(strClusteringKeyValue.trim().substring(5, 7));
                int day = Integer.parseInt(strClusteringKeyValue.trim().substring(8));
                clusteringKeyValue = new Date(year - 1900, month - 1, day);break;
            case "java.lang.String":
                clusteringKeyValue = strClusteringKeyValue;break;
            case "java.lang.Double":
                clusteringKeyValue = Double.parseDouble(strClusteringKeyValue);break;
            default: clusteringKeyValue = null;break;
        }

        Row update = new Row(clusteringKeyValue);
        update.data.put(getClusteringKey(strTableName), strClusteringKeyValue);
        for (String colName: colNames) {
            update.data.put(colName,htblColNameValue.get(colName));
        }
        try {
            x.updateTable(update, x);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Object getClusteringKeyValue(String clusteringKey, Hashtable<String,Object> htblColNameValue){
        ArrayList<String> clusteringKeyValues = new ArrayList<String>();

        Set<String> colNames = htblColNameValue.keySet();
        for(String colName: colNames){
            if(colName.equals(clusteringKey))
                return htblColNameValue.get(colName);
        }

        return null;
    }

    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException {
        if(!checkTableExists(strTableName))
            throw new DBAppException("Table does not exist!");

        if(!checkColumnNames(strTableName, htblColNameValue))
            throw new DBAppException("Invalid column names!");

        if(!checkColumnTypes(strTableName, htblColNameValue))
            throw new DBAppException("Invalid column types!");

        String clusteringKey = getClusteringKey(strTableName);
        Object clusteringKeyValue = getClusteringKeyValue(clusteringKey, htblColNameValue);

        Table tbl = Table.loadTable(strTableName);
        Row x;
        if(clusteringKeyValue == null)
            x = new Row(null);
        else
            x = new Row(clusteringKeyValue);

        Set<String> colNames = htblColNameValue.keySet();
        for (String colName: colNames) {
            x.data.put(colName,htblColNameValue.get(colName));
        }

        if(clusteringKeyValue != null) {
            try {
                tbl.deleteFromTableBinarySearch(tbl, x);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else{
            try {
                tbl.deleteFromTableLinearSearch(tbl, x);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {

        return null;
    }

    public static void main(String[] args) throws IOException, DBAppException, ParseException {
        Page p = Page.loadPage("courses",1);
        System.out.println(p.rows.size());
        System.out.println(p.min);
        System.out.print(p.max);



    }
}
