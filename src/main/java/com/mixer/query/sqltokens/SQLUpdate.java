package com.mixer.query.sqltokens;

import java.util.Arrays;
import java.util.HashSet;
import java.lang.reflect.Field;

/**
 * Select object represents the 'Select keyword' in the SQL query
 */
public class SQLUpdate extends SQLToken {
    private String[] values = null;
    private SQLToken tokenToUse = null;

    public SQLUpdate(SQLTYPE type) {
        super(type);
    }

    @Override
    public void addChild(final SQLToken token) {
        if(token.type == SQLTYPE.VALUES) {
            this.tokenToUse = token;
        }
        else{
            this.childs.add(token);
        }
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
        

        this.expression = this.expression.trim().substring(1).trim();
        this.expression = this.expression.trim().substring(0, this.expression.length()-1).trim();
        //get the new values
        String _tmp =  this.tokenToUse.expression.trim().substring(1);
        _tmp = _tmp.substring(0, _tmp.length()-1);
        this.values = _tmp.split(",");
        String[] fieldNames = this.expression.trim().split(",");

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
        // update the elements from the table
        try{
            for(Object o : result) {
                for(int i=0;i< fieldNames.length;i++){
                    Field field = o.getClass().getDeclaredField(fieldNames[i].trim());
                    field.setAccessible(true);
                    field.set(o, this.values[i].substring(1, this.values[i].length()-1));
                }
            }
        }catch(IllegalArgumentException|NoSuchFieldException|IllegalAccessException iae) {
            // TODO throw new DBexception
            iae.printStackTrace();
        }

        return result.toArray(new Object[0]);
    }

}
