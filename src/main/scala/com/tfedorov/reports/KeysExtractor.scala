package main.scala.com.tfedorov.reports

class KeysExtractor(meta: RowMetadata) {

  def keys4Cols: Key4Columns =
    Key4Columns(
      meta.parsed.marketCode,
      meta.parsed.callLetter,
      meta.parsed.commercialDate,
      meta.parsed.commercialTime)
}

case class Key4Columns(marketCode: String,
                       callLetter: String,
                       commercialDate: String,
                       commercialTime: String)

object KeysExtractor {

  implicit def row2KeyExtractor(meta: RowMetadata): KeysExtractor = new KeysExtractor(meta)

  implicit val dateTimeOrdering: Ordering[Key4Columns] = new Ordering[Key4Columns] {
    override def compare(x: Key4Columns, y: Key4Columns): Int = {
      val compareByDate = x.commercialDate.compareTo(y.commercialDate)
      if (compareByDate == 0)
        x.commercialTime.compareTo(y.commercialTime)
      else
        compareByDate
    }
  }
}