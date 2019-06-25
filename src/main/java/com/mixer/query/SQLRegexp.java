package com.mixer.query;

import com.mixer.query.sqltokens.*;
import com.mixer.query.sql.ResultSet;

import java.util.Stack;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// not
// Select (name, address) where  not (name='a1') ? 
// Delete where (name='VP3')
// Update (name, address) values ('new name') where (name='a1')

public final class SQLRegexp {

    private static SQLRegexp instance = null;

    public SQLRegexp() {

    }

    public static SQLRegexp getInstance() {
        if(instance == null) {
            instance = new SQLRegexp();
        }

        return instance;
    }

//    public static void main(String[] args) {
//        // String to be scanned to find the pattern.select
//        //String line = "Update (name, address) values ('new name','new address') where (name='a1')";
//        String line = "Select (name, address) where (name='a1') or (name='a2')";
//        //String line = "Delete where (address='VP3')";
//
//        SQLRegexp regext = new SQLRegexp();
//
//        // step 1
//        String[] tokens = regext.parseSQL(line);
//        // step 2
//        SQLToken rootToken = regext.buildTree(tokens);
//        // step 3 : generate test data
//        Object[] data = regext.generate();
//        // step 4 run the query
//        ResultSet result = regext.render(rootToken, data);
//
//        // print out the result
//        System.out.println(String.format("=== Object result (count: %d) ===", result.count()));
//        for (Object obj : result) {
//            System.out.println(obj.toString());
//        }
//
//        System.out.println("=== /Object result ====");
//    }

    /**
     * Parse the sql text with the help of the regular expression
     * 
     * @param sqlText SQL string
     * @return  parsed Token strings
     */
    public String[] parseSQL(final String sqlText) {
        String pattern = "(Update|Select|Delete|\\([^(\\)]+\\)|where|and|or|values)";

        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);

        // Now create matcher object.
        Matcher m = r.matcher(sqlText);
        Vector<String> tokens = new Vector<>();
        while (m.find()) {
            String _tmp = m.group(0);
            System.out.println("Found value: " +  _tmp);
            tokens.add(_tmp.trim());
        }

        return tokens.toArray(new String[0]);
    }

    /**
     * Build the AST tree
     * Root element will be the Select object.
     * 
     * @param tokens Array of the parsed tokens
     * @return Returns the root object, which is the Select object
     */
    public SQLToken buildTree(String[] tokens) {
        // stack is used to store the parent element of the tree
        Stack<SQLToken> tokenStack = new Stack<>();

        for(String str : tokens) {
            switch (str) {
                case "Select":
                    SQLSelect select = new SQLSelect(SQLTYPE.SELECT);
                    tokenStack.push(select);
                    break;
                case "Delete":
                    SQLDelete delete = new SQLDelete(SQLTYPE.DELETE);
                    tokenStack.push(delete);
                    break;
                case "Update":
                    SQLUpdate update = new SQLUpdate(SQLTYPE.UPDATE);
                    tokenStack.push(update);
                    break;
                case "where":
                    SQLWhere where = new SQLWhere(SQLTYPE.WHERE);
                    tokenStack.peek().addChild(where);
                    tokenStack.push(where);
                    break;
                case "and": {
                    SQLAND and = new SQLAND(SQLTYPE.AND);
                    tokenStack.peek().addChild(and);
                    tokenStack.push(and);
                    break;
                }
                case "or": {
                    SQLOr and = new SQLOr(SQLTYPE.OR);
                    tokenStack.peek().addChild(and);
                    tokenStack.push(and);
                    break;
                }
                case "values": {
                    SQLValues values = new SQLValues(SQLTYPE.VALUES);
                    tokenStack.peek().addChild(values);
                    tokenStack.push(values);
                    break;
                }
                default:
                    // it must be an expression
                    tokenStack.peek().setExpression(str);
                    if (tokenStack.peek().type == SQLTYPE.VALUES || 
                    tokenStack.peek().type == SQLTYPE.OR 
                    || tokenStack.peek().type == SQLTYPE.AND
                    || tokenStack.peek().type == SQLTYPE.WHERE)
                    //     //remove last element
                       tokenStack.pop();
                    break;
            }
        }
        // root element is the last element on the stack
        SQLToken token = tokenStack.pop();
            SQLToken[] childs = token.childs();
            System.out.println("=== CHILDS ===");
            for (SQLToken child: childs){
                System.out.println(child.type.toString() + " ->" + child.getExpression());
            }
            System.out.println("=== /CHILDS ===");
        return token;
    }

    /**
     * Run the query
     * @param rootToken
     * @param data the 'rows' of the database  - or it can be a content of an index
     * @return array of objects which the query finds
     */
    public ResultSet render(final SQLToken rootToken, Object[] data) {
        Object[] result = rootToken.render(data);
        return new ResultSet(result);
    }
}
