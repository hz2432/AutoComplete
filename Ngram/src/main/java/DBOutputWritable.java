import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by haiki on 3/29/17.
 */
public class DBOutputWritable implements DBWritable{
    private String starting_phrase;
    private String following_phrase;
    private int count;

    public DBOutputWritable(String starting_phrase, String following_phrase, int count){
        this.starting_phrase = starting_phrase;
        this.following_phrase = following_phrase;
        this.count = count;
    }

    public void readFields(ResultSet arg0) throws SQLException{
        this.starting_phrase = arg0.getString(1);
        this.following_phrase = arg0.getString(2);
        this.count = arg0.getInt(3);
    }

    public void write(PreparedStatement arg0) throws SQLException{
        arg0.setString(1, starting_phrase);
        arg0.setString(2, following_phrase);
        arg0.setInt(3, count);

    }
}
