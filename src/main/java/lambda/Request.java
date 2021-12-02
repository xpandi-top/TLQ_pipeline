package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;
    private String bucketname;
    private String filename;
    private int row;
    private int col;
    public String getBucketname() {return bucketname;}
    public String getFilename(){return filename;}
    public int getRow() {return row;}
    public int getCol() {return col;}
    public String getName() {
        return name;
    }
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setBucketname(String bucketname) {this.bucketname=bucketname;}
    public void setFilename(String filename) {this.filename=filename;}
    public void setRow(int row){this.row = row;}
    public void setCol(int col){this.col = col;}
    public Request(String name, String bucketname, String filename, int row, int col){
        this.name = name;
        this.bucketname = bucketname;
        this.filename = filename;
        this.row = row;
        this.col = col;
    }
    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }
}
