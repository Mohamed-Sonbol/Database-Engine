public class RecordReference implements java.io.Serializable{

    int pageIndex;
    int recordIndex;

    public RecordReference(int pageIndex, int recordIndex){
        this.recordIndex = recordIndex;
        this.pageIndex = pageIndex;
    }

}
