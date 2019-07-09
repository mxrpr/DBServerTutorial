package com.mixer.query.sqltokens;

import com.mixer.query.sql.DBEntry;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Select object represents the 'Select keyword' in the SQL query
 */
public class SQLSelect extends SQLToken {

    public SQLSelect(SQLTYPE type) {
        super(type);
    }

    /**
     * Resposibility of this object is:
     * 
     * o coordinate the run of the child elements
     * o provide the necessary data set to the childs elements
     * o collects the results
     */
    @Override
    public DBEntry[] render(DBEntry[] objects) {
        HashSet<DBEntry> result = new HashSet<>();
        this.expression = this.expression.substring(1);
        this.expression = this.expression.substring(0, this.expression.length()-1);
        // not used in this example - later we will use it
        String[] valuesToReturn = this.expression.split(
                ",");
        /*
         * run through all childs, and call the render method.
         */
        for (SQLToken token: this.childs) {
            if (token.type == SQLTYPE.WHERE){
                result.addAll(Arrays.asList(token.render(objects)));

            }else if(token.type == SQLTYPE.AND){
                // the input array is produced by the last child.
                // result.addAll(Arrays.asList(token.render(result.toArray())));
                DBEntry[] _res = token.render(result.toArray(new DBEntry[0]));
                result.clear();
                result.addAll(Arrays.asList(_res));
            }
            else if (token.type == SQLTYPE.OR){
                // input array is the original object array
                result.addAll(Arrays.asList(token.render(objects)));
            }
        }

        return result.toArray(new DBEntry[0]);
    }

}
