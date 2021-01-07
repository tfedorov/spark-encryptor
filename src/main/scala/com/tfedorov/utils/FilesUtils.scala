package com.tfedorov.utils

import java.util.jar.{JarFile, Manifest}
import scala.collection.JavaConverters._

object FilesUtils {

  def readAssemblyManifest(): String = {
    readManifest(jarFragment = "assembly").getOrElse("no assembly manifest")
  }

  private def readManifest(jarFragment: String): Option[String] = {
    val manifests = Thread.currentThread.getContextClassLoader.getResources(JarFile.MANIFEST_NAME).asScala.toSeq
    val foundedJars = manifests.filter(_.getPath.contains(jarFragment))
    if (foundedJars.isEmpty)
      return None

    val manifestContent = new StringBuilder()
    foundedJars.foreach { url =>
      val manifest = new Manifest(url.openStream())
      manifestContent.append("File:" + url.getPath + "\n")
      manifest.getMainAttributes.asScala.foreach { atribute =>
        manifestContent.append(atribute._1 + "," + atribute._2 + "\n")
      }
    }
    Some(manifestContent.mkString)
  }
}
