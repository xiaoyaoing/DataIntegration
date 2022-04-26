package com.DI.flink

import org.apache.flink.connector.jdbc.JdbcStatementBuilder

import java.sql.{PreparedStatement, Types}



class CkSinkBuilder[O] extends JdbcStatementBuilder[O] {
  def accept(ps: PreparedStatement, v: O): Unit = {
    val list = v.getClass.getDeclaredFields.toList
    for (i <- 1 to list.length) {
      list(i - 1).setAccessible(true)
      val maybeValue = list(i - 1).get(v)
      maybeValue match {
        case someString: Some[Any] =>
          someString match {
            case Some(value) =>
              value match {
                case d: Int =>
                  ps.setInt(i, d)
                case d: Double =>
                  ps.setDouble(i, d)
                case d: String =>
                  ps.setString(i, d)
                case _ =>
              }
            case _ =>
          }
        case _ =>
          ps.setNull(i, Types.NULL)
      }
    }
  }
}
