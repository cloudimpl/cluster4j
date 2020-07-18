package com.cloudimpl.cluster4j.test;

//package com.cloudimpl.cloudengine.test;
//
//import java.util.List;
//import net.sf.jsqlparser.JSQLParserException;
//import net.sf.jsqlparser.parser.CCJSqlParserUtil;
//import net.sf.jsqlparser.schema.Column;
//import net.sf.jsqlparser.schema.Table;
//import net.sf.jsqlparser.statement.Statement;
//import net.sf.jsqlparser.statement.select.PlainSelect;
//import net.sf.jsqlparser.statement.select.Select;
//import net.sf.jsqlparser.statement.select.SelectBody;
//import net.sf.jsqlparser.statement.select.SelectItem;
//import net.sf.jsqlparser.statement.select.SetOperationList;
//import net.sf.jsqlparser.util.SelectUtils;
//
//public class SelectSqlParser {
//
//  public static void main(String[] args) {
//
//    System.out.println("Program to parse INSERT sql statement");
//    String selectSQL = "Select id, name, location from Database.UserTable " +
//        "where created_dt >= current_date- 180";
//
//    try {
//      Statement select = (Statement) CCJSqlParserUtil.parse(selectSQL);
//      // Simple Select query parsing
//      System.out.println("Simple single select with where condition\n");
//
//      System.out.println("List of  columns in select query");
//      System.out.println("--------------------------------");
//      List<SelectItem> selectCols = ((PlainSelect) ((Select) select).getSelectBody()).getSelectItems();
//
//      for (SelectItem selectItem : selectCols)
//        System.out.println(selectItem.toString());
//
//      System.out.println("Where condition: " + ((PlainSelect) ((Select) select).getSelectBody()).getWhere().toString());
//
//      SelectUtils.addExpression((Select) select, new Column("newColumnName"));
//      System.out.println("\nModified select with additional column");
//      System.out.println("----------------------------------");
//      System.out.println(select.toString());
//
//      ((Table) ((PlainSelect) ((Select) select).getSelectBody()).getFromItem()).setName("NewSourceTable");
//      ((Table) ((PlainSelect) ((Select) select).getSelectBody()).getFromItem()).setSchemaName("NewSourceTable");
//
//      System.out.println("\nModified select with new table and database");
//      System.out.println("-------------------------------------");
//      System.out.println(select.toString());
//
//      selectSQL = "Select w.id, w.name, w.location from Database.WebLogs w " +
//          "union Select m.id, m.name, m.location from Database.MobileLogs m ";
//
//      Statement newSQL = (Statement) CCJSqlParserUtil.parse(selectSQL);
//      List<SelectBody> selectList = ((SetOperationList) ((Select) newSQL).getSelectBody()).getSelects();
//      System.out.println("\nListing all selects from the query");
//      System.out.println("----------------------------------");
//      for (SelectBody selectBody : selectList)
//        System.out.println(selectBody.toString());
//
//    } catch (JSQLParserException e) {
//      e.printStackTrace();
//    }
//  }
//}
