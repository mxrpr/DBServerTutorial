package com.mixer.query.sqltokens;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Select object represents the 'Select keyword' in the SQL query
 */
public class SQLDelete extends SQLToken {

    public SQLDelete(SQLTYPE type) {
        super(type);
    }

    /**
     * Resposibility of this object is:
     * 
     * o coordinate the run of the child elements
     * o provide the necessary data set to the childs elements
     * o collects the results
     * 
     * @return The rest of the original results
     */
    @Override
    public Object[] render(Object[] objects) {
        HashSet<Object> result = new HashSet<>();
        /*
         * run through all childs, and call the render method.
         */
        for (SQLToken token: this.childs) {
            if (token.type == SQLTYPE.WHERE){
                result.addAll(Arrays.asList(token.render(objects)));

            }else if(token.type == SQLTYPE.AND){
                // the input array is produced by the last child.
                // result.addAll(Arrays.asList(token.render(result.toArray())));
                Object[] _res = token.render(result.toArray());
                result.clear();
                result.addAll(Arrays.asList(_res));
            }
            else if (token.type == SQLTYPE.OR){
                // input array is the original object array
                result.addAll(Arrays.asList(token.render(objects)));
            }
        }
        // delete the elements from the table
        HashSet<Object> finalResult = new HashSet<>();
        for(Object object : objects) {
            if(!result.contains(object)) {
                finalResult.add(object);
            }
        }
        return finalResult.toArray(new Object[0]);
    }

}
