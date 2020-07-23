package main.scala.com.tfedorov.reports

import sun.reflect.generics.reflectiveObjects.NotImplementedException

object Splitter extends App {

  def extractOriginal(input: String): RowMetadata = {
    RowMetadata(input, false, splitThomas(input))
  }

  def extractChecked(input: String): RowMetadata = {
    RowMetadata(input, true, splitEMR(input))
  }

  private def splitThomas(line: String): RowParsedColumns = {
    if (line.length < 343)
      throw new NotImplementedException()
    val marketCode = line.substring(0, 2)
    val callLetter = line.substring(2, 6)
    val commercialDate = line.substring(6, 14)
    val commercialTime = line.substring(14, 20)
    val commercialDescription = line.substring(20, 55)
    val showTitle = line.substring(55, 80)
    val showTypeDescription = line.substring(80, 110)
    val dayPart = line.substring(110, 127)
    val ultimateParentDesc = line.substring(127, 162)
    val parentDesc = line.substring(162, 197)
    val brandDesc = line.substring(197, 262)
    val pccDesc = line.substring(264, 292)
    val commercialDuration = line.substring(292, 295)
    val expenditure = line.substring(295, 303)
    val podNumber = line.substring(303, 306)
    val podSequence = line.substring(306, 309)
    val commercialTrafficType = line.substring(309, 310)
    val brandVariantDesc = line.substring(310, 340)
    val maxSequenceWithinPod = line.substring(340)

    RowParsedColumns(marketCode, callLetter, commercialDate, commercialTime,
      commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
      parentDesc, brandDesc, pccDesc, commercialDuration,
      expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
  }


  private def splitEMR(line: String): RowParsedColumns = {

    val marketCode = line.substring(0, 2)
    val callLetter = line.substring(2, 6)
    val commercialDate = line.substring(6, 14)
    val commercialTime = line.substring(14, 20)
    val commercialDescription = line.substring(20, 55)
    val showTitle = line.substring(55, 80)
    val showTypeDescription = line.substring(80, 110)
    val dayPart = line.substring(110, 127)
    val ultimateParentDesc = line.substring(127, 162)
    val parentDesc = line.substring(162, 197)
    val brandDesc = line.substring(197, 262)
    val pccDesc = line.substring(264, 292)
    val commercialDuration = line.substring(292, 295)
    val expenditure = line.substring(295, 303)
    val podNumber = line.substring(303, 306)
    val podSequence = line.substring(306, 309)
    var commercialTrafficType = line.substring(309, 310)
    if (line.length < 337) {
      commercialTrafficType = line.substring(302, 303)
      val brandVariantDesc = line.substring(303, 333)
      val maxSequenceWithinPod = line.substring(333)
      return RowParsedColumns(marketCode, callLetter, commercialDate, commercialTime,
        commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
        parentDesc, brandDesc, pccDesc, commercialDuration,
        expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
    }
    val brandVariantDesc = line.substring(310, 340)
    val maxSequenceWithinPod = line.substring(340)

    RowParsedColumns(marketCode, callLetter, commercialDate, commercialTime,
      commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
      parentDesc, brandDesc, pccDesc, commercialDuration,
      expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
  }

}
