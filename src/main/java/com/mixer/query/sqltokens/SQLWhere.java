package com.mixer.query.sqltokens;

import com.mixer.query.sql.DBEntry;

import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to represents the 'where' SQL keyword
 */
public class SQLWhere extends SQLToken {

    public SQLWhere(SQLTYPE type) {
        super(type);
    }

    /**
     * Returns the operation from the expression
     * 
     * @param exp Expression itself - for example (address='Wien')
     * @return The operator - for example '=' or '>'
     */
    protected String getOperation(final String exp) {
        String retValue = "=";
        if(exp.contains(">"))
        {
            retValue=">";
        }
        // used for regular expression
        else if(exp.contains("~"))
        {
            retValue="~";
        }
        else if(exp.contains("<"))
        {
            retValue="<";
        }
        else if(exp.contains("!="))
        {
            retValue="!=";
        }

        return retValue;
    }
    /**
     * Extract the field name and the value from expression
     * 
     * @param operation Operation used in the expression
     * @return field name and the field value - we could use a Tuple, or Pair here
     */
    private String[] getFieldNameAndValue(final String operation) {
        int index = this.expression.indexOf(operation);
        String fieldName = this.expression.substring(0, index).trim();
        String fieldValue = this.expression.substring(index + 1).trim();

        return new String[]{fieldName, fieldValue.substring(1, fieldValue.length()-1)};
    }

    @Override
    public DBEntry[] render(DBEntry[] dbEntries) {
        Vector<DBEntry> result = new Vector<>();
        this.expression = this.expression.substring(1, this.expression.length()-1);
        // get the operation
        String operation = this.getOperation(this.expression);
        String[] fieldNameAndValue = this.getFieldNameAndValue(operation);

        try {
            for (DBEntry object : dbEntries) {
                DBEntry retValue = this.hasFieldValue(object, fieldNameAndValue[0], fieldNameAndValue[1], operation);

                if (retValue != null) {
                    result.add(retValue);
                }
            }
        }catch(DBException dbe) {
            dbe.printStackTrace();
        }
        return result.toArray(new DBEntry[0]);
    }

    /**
     * We must check whether the object has such a filed which is in the expression
     * 
     * What we have to do when we are implementin this into the DB server is to
     * throw exceptions when we registers syntax or other problems.
     * 
     * @param dbentry Object wich contains the stored object in the database on which the check is performed
     * @param fieldName Name of the field
     * @param fieldValue Value of the field
     * @param operation Operation used in expression
     * @return  The value of the expression (The found object)
     * 
     * @throws DBException
     */
    protected DBEntry hasFieldValue(final DBEntry dbentry,
                                 final String fieldName,
                                 final String fieldValue,
                                 final String operation) throws DBException {
        try {
            Object _fieldValue = dbentry.object.getClass().getDeclaredField(fieldName).get(dbentry.object);

            if(this.handleOperation(_fieldValue, fieldValue, operation)) {
                return dbentry;
            }
            return null;

        }catch(Exception e) {
            e.printStackTrace();
            throw new DBException(e.getMessage());
        }
    }

    /**
     * Checks wheter the condition in the expression is true on a
     * specific object or not
     * 
     * @param originalFieldValue Field value
     * @param toTestValue test value from the expression
     * @param operation What operation we have to use
     * @return the condition from the expression if true or false
     */
    protected boolean handleOperation(final Object originalFieldValue,
                                    final Object toTestValue,
                                    final String operation) {
        switch (operation) {
            case "=":
                return originalFieldValue.equals(toTestValue);
            case "!=":
                return !originalFieldValue.equals(toTestValue);
            case ">":
                return (Double) originalFieldValue > (Double) toTestValue;
            case "<":
                return (Double) originalFieldValue < (Double) toTestValue;
            case "~":{
                String _original = (String) originalFieldValue;
                Pattern r = Pattern.compile((String) toTestValue);
                Matcher m = r.matcher(_original);
                if (m.find()) {
                    return true;
                }
                break;
            }
            default: {
                // we should throw here exception
                System.out.println("Operation is not supported..!!");
                return false;
            }
        }

        return false;
    }

}
