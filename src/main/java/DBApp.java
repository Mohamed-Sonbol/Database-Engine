import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
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



    public void createIndex(String strTableName, String[] strarrColName) throws DBAppException{
        int N = strarrColName.length;
        Index I = new Index(strTableName, N, strarrColName);
        Hashtable allMin = getColsMin(strTableName);
        Hashtable allMax = getColsMax(strTableName);
        Object[] colsMin = new Object[N];
        Object[] colsMax = new Object[N];
        for(int j=0 ; j<N ; j++){
            colsMin[j] = allMin.get(strarrColName[j]);
            colsMax[j] = allMax.get(strarrColName[j]);
        }
        I.setRanges(strarrColName, colsMin, colsMax);
        try {
            I.fillIndex();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public String getColType(String tblName, String colName){
        String row;
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(tblName.equals(data[0]) && colName.equals(data[1])){
                    return data[2];
                }
            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public Hashtable<String, Object> getColsMin(String tableName){

        String row;
        Hashtable colMin = new Hashtable();

        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(tableName.equals(data[0])){
                    String colType = getColType(tableName, data[1]);
                    Object colValue;
                    switch (colType) {
                        case "java.lang.Integer":
                            colValue = Integer.parseInt(data[5]);break;
                        case "java.util.Date":
                            int year = Integer.parseInt(data[5].trim().substring(0, 4));
                            int month = Integer.parseInt(data[5].trim().substring(5, 7));
                            int day = Integer.parseInt(data[5].trim().substring(8));
                            colValue = new Date(year - 1900, month - 1, day);break;
                        case "java.lang.String":
                            colValue = data[5];break;
                        case "java.lang.Double":
                            colValue = Double.parseDouble(data[5]);break;
                        default: colValue = null;break;
                    }
                    colMin.put(data[1],colValue);
                }

            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return colMin;

    }

    public Hashtable<String, Object> getColsMax(String tableName){

        String row;
        Hashtable colMax = new Hashtable();

        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            csvReader.readLine();
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                if(tableName.equals(data[0])){
                    String colType = getColType(tableName, data[1]);
                    Object colValue;
                    switch (colType) {
                        case "java.lang.Integer":
                            colValue = Integer.parseInt(data[6]);break;
                        case "java.util.Date":
                            int year = Integer.parseInt(data[6].trim().substring(0, 4));
                            int month = Integer.parseInt(data[6].trim().substring(5, 7));
                            int day = Integer.parseInt(data[6].trim().substring(8));
                            colValue = new Date(year - 1900, month - 1, day);break;
                        case "java.lang.String":
                            colValue = data[6];break;
                        case "java.lang.Double":
                            colValue = Double.parseDouble(data[6]);break;
                        default: colValue = null;break;
                    }
                    colMax.put(data[1],colValue);
                }

            }
            csvReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return colMax;

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
        for(String colNameTypeInput: colNameTypesInput){
            String typeInput = htblColNameValue.get(colNameTypeInput).getClass().getName();
            if(!typeInput.equals(colNameTypes.get(colNameTypeInput)))
                return false;
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

    public boolean checkColumnsMin(Hashtable colsMin, Hashtable inputsMin){
        Set<String> inputsMinSet = inputsMin.keySet();

        for(String colName: inputsMinSet){
            if(compare(colsMin.get(colName),(inputsMin.get(colName))) >= 1)
                return false;
        }

        return true;
    }

    public boolean checkColumnsMax(Hashtable colsMax, Hashtable inputsMax){
        Set<String> inputsMinSet = inputsMax.keySet();

        for(String colName: inputsMinSet){
            if(compare(colsMax.get(colName),(inputsMax.get(colName))) <= -1)
                return false;
        }

        return true;
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

        Hashtable colsMin = getColsMin(strTableName);
        Hashtable colsMax = getColsMax(strTableName);

        if(!checkColumnsMin(colsMin, htblColNameValue))
            throw new DBAppException("Value is not within the range!");
        if(!checkColumnsMax(colsMax, htblColNameValue))
            throw new DBAppException("Value is not within the range!");

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

        Hashtable colsMin = getColsMin(strTableName);
        Hashtable colsMax = getColsMax(strTableName);

        if(!checkColumnsMin(colsMin, htblColNameValue))
            throw new DBAppException("Value is not within the range!");
        if(!checkColumnsMax(colsMax, htblColNameValue))
            throw new DBAppException("Value is not within the range!");


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
        Vector<Result> results = new Vector<Result>();
        Result result;
        Table tbl = Table.loadTable(arrSQLTerms[0]._strTableName);

        if (tbl.indicesPath.size() == 0) {
            for (int k = 0; k < arrSQLTerms.length; k++)
                results.add(tbl.normalSearch(arrSQLTerms[k]));
        }
        else {
            boolean and = true;
            boolean equal = true;
            boolean continueQuery = false;
            for(int i = 0; i<strarrOperators.length; i++){
                if(!strarrOperators[i].equals("AND")){
                    and = false;
                    continueQuery = true;
                    break;
                }
            }
            for(int i = 0; i<arrSQLTerms.length;i++){
                if(!arrSQLTerms[i]._strOperator.equals("=")){
                    equal = false;
                    continueQuery = true;
                    break;
                }
            }
            if(and && equal) {
                ArrayList<String> queryColNames = new ArrayList<>();
                for (int i = 0; i < arrSQLTerms.length; i++)
                    queryColNames.add(arrSQLTerms[i]._strColumnName);

                for (int i = 0; i < tbl.indicesPath.size(); i++) {
                    Index index = Index.loadIndex(tbl.name, i);
                    if(index.indexedCols.length == queryColNames.size()) {
                        boolean accepted = true;
                        for (int j = 0; j < index.indexedCols.length; j++) {
                            if(!queryColNames.contains(index.indexedCols[j])) {
                                accepted = false;
                                break;
                            }
                        }
                        if(accepted) {
                            results.add(tbl.indexSearch(arrSQLTerms, index));
                            return results.get(0);
                        }
                        else
                            continueQuery = true;

                    }
                    else
                        continueQuery = true;
                }
            }
            if(continueQuery){
                for(int i = 0; i<arrSQLTerms.length;i++){
                    SQLTerm x = arrSQLTerms[i];
                    for(int j = 0; j<tbl.indicesPath.size();j++){
                        Index index = Index.loadIndex(tbl.name, j);
                        SQLTerm[] s = new SQLTerm[1];
                        s[0] = x;
                        if(index.indexedCols.length == 1 && index.indexedCols[0].equals(x._strColumnName))
                            results.add(tbl.indexSearch(s,index));
                        else
                            results.add(tbl.normalSearch(x));
                    }
                }
            }
        }

        for (int i = 0; i < strarrOperators.length; i++) {
            result = operation(results, strarrOperators[i]);
            results.remove(0);
            results.remove(0);
            results.add(0, result);
        }
        return results.get(0);
    }

    public Result operation(Vector<Result> results, String operator){
        Result result = new Result();
        switch (operator){
            case "AND":
                for(int i = 0; i<results.get(0).resultSet.size();i++){
                    for(int j = 0; j<results.get(1).resultSet.size(); j++){
                        if(results.get(0).resultSet.get(i).equals(results.get(1).resultSet.get(j)))
                            result.resultSet.add(results.get(1).resultSet.get(j));
                    }
                }
                break;
            case "OR":
                for(int i = 0; i<results.get(0).resultSet.size();i++)
                    result.resultSet.add(results.get(0).resultSet.get(i));

                for(int j = 0; j<results.get(1).resultSet.size(); j++){
                    if(!result.resultSet.contains(results.get(1).resultSet.get(j)))
                        result.resultSet.add(results.get(1).resultSet.get(j));
                }
                break;
            case "XOR":
                for(int i = 0; i<results.get(0).resultSet.size();i++)
                    result.resultSet.add(results.get(0).resultSet.get(i));

                for(int j = 0; j<results.get(1).resultSet.size(); j++){
                    if(result.resultSet.contains(results.get(1).resultSet.get(j)))
                        result.resultSet.remove(result.resultSet.indexOf(results.get(1).resultSet.get(j)));
                    else
                        result.resultSet.add(results.get(1).resultSet.get(j));
                }
                break;
            default: return null;
        }
        return result;
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

    public static void main(String[] args) throws IOException, DBAppException, ParseException {


    }
}
