package util

import java.io.File
import java.nio.file.{Files, Path}
import java.util.zip.ZipFile

import akka.actor.Scheduler
import akka.pattern.after
import org.apache.commons.io.FilenameUtils

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

object Helpers {

  implicit class SeqExtension[A](s: Seq[A]) {
    def foldLeftToFuture[B](initial: B)(f: (B, A) => Future[B])(implicit ec: ExecutionContext): Future[B] =
      s.foldLeft(Future(initial))((future, item) => future.flatMap(f(_, item)))

    def mapInSeries[B](f: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] =
      s.foldLeftToFuture(Seq[B]())((seq, item) => f(item).map(seq :+ _))
  }

  def retry[T](f: => Future[T], delay: FiniteDuration = Duration.Zero, retries: Int = 0)(implicit ec: ExecutionContext, s: Scheduler): Future[T] = {
    f recoverWith { case _ => if (retries > 0) after(delay, s)(retry(f, delay, retries - 1)) else after(delay, s)(retry(f, delay)) }
  }

  def unzip(zipFile: File, delete: Boolean = false): Unit = {
    def using[T <: {def close()}, U](resource: T)(block: T => U): U = {
      try {
        block(resource)
      } finally {
        if (resource != null) {
          resource.close()
        }
      }
    }

    val outputPath = Path.of(FilenameUtils.removeExtension(zipFile.getPath))
    using(new ZipFile(zipFile)) {
      zipFile =>
        zipFile.entries.asScala.foreach {
          entry =>
            val path = outputPath.resolve(entry.getName)
            if (entry.isDirectory) {
              Files.createDirectories(path)
            } else {
              Files.createDirectories(path.getParent)
              Files.copy(zipFile.getInputStream(entry), path)
            }
        }
    }
    if (delete) zipFile.delete()
  }
}
