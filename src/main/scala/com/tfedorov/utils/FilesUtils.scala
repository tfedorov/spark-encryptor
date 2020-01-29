package com.tfedorov.utils

import java.io.InputStream

import scala.io.Source

object FilesUtils {

  def manifestMF: String = {
    val manifestPath = getClass().getClassLoader().getResource("META-INF/MANIFEST.MF")
    if (manifestPath == null)
      return "'META-INF/MANIFEST.MF' not found"
    val manifestResource: InputStream = getClass().getClassLoader().getResourceAsStream("META-INF/MANIFEST.MF")
    val manifestText = Source.fromInputStream(manifestResource, "UTF-8").mkString
    s"\n$manifestPath :\n" + manifestText
  }

}
