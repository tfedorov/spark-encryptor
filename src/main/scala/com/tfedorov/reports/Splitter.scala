package main.scala.com.tfedorov.reports

import sun.reflect.generics.reflectiveObjects.NotImplementedException

import scala.collection.immutable
import scala.util.matching.Regex

object Splitter extends App {

  def extractThomas(input: String): RowMetadata = {
    extract(input, false)
  }

  def extractEmr(input: String): RowMetadata = {
    extract(input, true)
  }

  private def extract(input: String, source: Boolean): RowMetadata = {
    val space3Regex: Regex = " {3,}".r
    var splitted = space3Regex.split(input).toList
    if (splitted.size >= 5) {
      val space2Regex: Regex = " {2,}".r
      splitted = space2Regex.split(input).toList
    }


    val splitted1st = splitFirstColumn(splitted.head)

    val result: immutable.Seq[String] = splitted1st ++ splitted

    RowMetadata(input, source, result.toList)
  }

  private def splitFirstColumn(line: String): List[String] = {

    val column1 = line.substring(0, 2)
    if (line.length < 9)
      return column1 :: line.substring(2) :: "" :: Nil
    if ("2020".equals(line.substring(6, 10))) {
      val column2 = line.substring(2, 6)
      val column3 = line.substring(6, 20)
      val column4 = line.substring(20)
      return column1 :: column2 :: column3 :: column4 :: Nil
    }
    val column2 = line.substring(2, 5)
    val column3 = line.substring(5, 19)
    val column4 = line.substring(19)
    column1 :: column2 :: column3 :: column4 :: Nil

  }

  private def splitDocStyle(line: String): RowParsed = {
    if (line.length < 343)
      throw new NotImplementedException()
    val marketCode = line.substring(0, 2)
    val callLetter = line.substring(2, 6)
    val commercialDate = line.substring(6, 14)
    val commercialTime = line.substring(14, 20)
    val commercialDescription = line.substring(20, 55)
    val showTitle = line.substring(55, 81)
    val showTypeDescription = line.substring(81, 111)
    val dayPart = line.substring(111, 128)
    val ultimateParentDesc = line.substring(128, 164)
    val parentDesc = line.substring(164, 199)
    val brandDesc = line.substring(199, 264)
    val pccDesc = line.substring(264, 295)
    val commercialDuration = line.substring(295, 298)
    val expenditure = line.substring(298, 305)
    val podNumber = line.substring(306, 309)
    val podSequence = line.substring(309, 312)
    val commercialTrafficType = line.substring(312, 313)
    val brandVariantDesc = line.substring(313, 343)
    val maxSequenceWithinPod = line.substring(343)

    RowParsed(marketCode, callLetter, commercialDate, commercialTime,
      commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
      parentDesc, brandDesc, pccDesc, commercialDuration,
      expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
  }


  private def splitThomas(line: String): RowParsed = {
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
    //HCBEIE20200427073741NO TITLE AVAILABLE                 LOCKER ROOM - R -        SPORTS NEWS                   M-F NOON NEWS    PROMO                              PROMO                              BEIN SPORTS SOCCER TV PGM-CABLE-SPORTS                           TV PGM-CABLE-SPORTS           03000000000003002C                              004
    RowParsed(marketCode, callLetter, commercialDate, commercialTime,
      commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
      parentDesc, brandDesc, pccDesc, commercialDuration,
      expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
  }


  private def splitEMR(line: String): RowParsed = {
    if (line.length < 337)
      throw new NotImplementedException()
    val marketCode = line.substring(0, 2)
    val callLetter = line.substring(2, 6)
    val commercialDate = line.substring(6, 14)
    val commercialTime = line.substring(14, 20)
    val commercialDescription = line.substring(20, 55)
    val showTitle = line.substring(55, 81)
    val showTypeDescription = line.substring(81, 111)
    val dayPart = line.substring(111, 128)
    val ultimateParentDesc = line.substring(128, 164)
    val parentDesc = line.substring(164, 199)
    val brandDesc = line.substring(199, 264)
    val pccDesc = line.substring(264, 295)
    val commercialDuration = line.substring(295, 298)
    val expenditure = line.substring(298, 305)
    val podNumber = line.substring(306, 309)
    val podSequence = line.substring(309, 312)
    val commercialTrafficType = line.substring(312, 313)
    val brandVariantDesc = line.substring(313, 340)
    val maxSequenceWithinPod = line.substring(340)

    RowParsed(marketCode, callLetter, commercialDate, commercialTime,
      commercialDescription, showTitle, showTypeDescription, dayPart, ultimateParentDesc,
      parentDesc, brandDesc, pccDesc, commercialDuration,
      expenditure, podNumber, podSequence, commercialTrafficType, brandVariantDesc, maxSequenceWithinPod)
  }

  //from doc
  splitDocStyle("NSSYN 20160522161048WOMAN WITH SLEDGEHAMMER/CARNIVAL   CLOSER AT                  GENERAL DRAMA                 M-S TOTAL DAY    BAYER AG                            BAYER HEALTHCARE LLC               CLARITIN ALLERGY REMEDY                                          COLD, COUGH & SINUS REMEDIES  01500000000001003BTABS/CAPS                     007")
  //from thomas
  splitThomas("HCBEIE20200427073741NO TITLE AVAILABLE                 LOCKER ROOM - R -        SPORTS NEWS                   M-F NOON NEWS    PROMO                              PROMO                              BEIN SPORTS SOCCER TV PGM-CABLE-SPORTS                           TV PGM-CABLE-SPORTS           03000000000003002C                              004")
  //emr
  splitEMR("NNCOZI20200427060938COVID19/WOMAN & KID/TREES/LAPTOP   MAKE ROOM DAD M-F 6A     SITUATION COMEDY              M-F SUNRISE      CARVANA GROUP LLC                  CARVANA LLC                        CARVANA ONLINE-VEHICLE SALES                                     AUTOS & LIGHT TRUCK - DEALERSH03000128.00001001NLINE-VEHICLE SALES CARVANA.COM004")
  splitEMR("HCCINL20200503182515VEHICLES/ANIMATION/CLOUDS/2 WOMEN  EL QUE BUSCA ENCUENTRA   FEATURE FILM                  M-F EARLY FRINGE UNITED BREAST CANCER FNDN          UNITED BREAST CANCER FNDN          UNITED BREAST CANCER FNDN CHARITABLE ORGN                        MISCELLANEOUS ORGANIZATION ADV060 002002NST CANCER FNDN CHARITABLE ORGN008")

}
