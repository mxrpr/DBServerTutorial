package com.mixer.query.sqltokens;

import com.mixer.exceptions.DBException;
import com.mixer.query.sql.DBEntry;

import java.util.Vector;

/**
 * Represent an SQL token class
 */
public abstract class SQLToken {

    // what type of token is
    public final SQLTYPE type;
    // stores the associated expression like (name='test')
    String expression;
    // store childs of the token object
    final Vector<SQLToken> childs;

    SQLToken(SQLTYPE type) {
        this.type = type;
        this.childs = new Vector<>();
    }

    public SQLToken[] childs() {
        return this.childs.toArray(new SQLToken[childs.size()]);
    }

    public void addChild(final SQLToken token) {
        this.childs.add(token);
    }

    public void setExpression(final String expression) {
        this.expression = expression;
    }

    public String getExpression() {
        return this.expression;
    }

    /**
     * Empty implementation of the render method. This will 'find' the
     * requested objects
     * 
     * @param objects Rows of the table  - array of stored objects
     * @see DBEntry
     *
     * @return DBEntry array
     */
    public DBEntry[] render(DBEntry[] objects) throws DBException {
        return null;
    }
}
