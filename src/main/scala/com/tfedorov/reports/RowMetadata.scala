package main.scala.com.tfedorov.reports


case class RowMetadata(originalRow: String, emr: Boolean, parsed: RowParsedColumns) {

  def isOriginalSource: Boolean = !emr

  def isNewSource: Boolean = emr

}
