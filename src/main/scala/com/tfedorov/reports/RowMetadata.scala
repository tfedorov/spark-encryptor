package main.scala.com.tfedorov.reports


case class RowMetadata(originalRow: String, emr: Boolean, slittedColumns: List[String]) {

  def isFromThomas: Boolean = !emr

  def isFromEmr: Boolean = emr

  def row2Key: RowMetadata.Key = {
    if (slittedColumns.size < 5)
      return ("", "", "")
    (slittedColumns.head, slittedColumns(1), slittedColumns(2))
  }

  def keyColumns: KeyColumns = {
    val (splitted1, splitted2, splitted3) = row2Key
    if (splitted3.isEmpty)
      return KeyColumns("", "", "", "")
    val c3 = splitted3.substring(0, 7)
    val c4 = splitted3.substring(7)
    KeyColumns(splitted1, splitted2, c3, c4)
  }
}

case class KeyColumns(marketCode: String,
                      callLetter: String,
                      commercialDate: String,
                      commercialTime: String)

object RowMetadata {
  type Key = (String, String, String)
}

