package util

import java.io._

import com.github.tototoshi.csv.{CSVFormat, CSVParser, CSVReader, LineReader, MalformedCSVException, ReaderLineReader}

class QuantlibCSVReader(private val lineReader: LineReader)(implicit format: CSVFormat) extends CSVReader(lineReader) {
  private[this] val parser = new CSVParser(format)

  override def readNext(): Option[List[String]] = {

    @scala.annotation.tailrec
    def parseNext(lineReader: LineReader, leftOver: Option[String] = None): Option[List[String]] = {
      val nextLine = lineReader.readLineWithTerminator()

      nextLine match {
        case null =>
          if (leftOver.isDefined) {
            throw new MalformedCSVException("Malformed Input!: " + leftOver)
          } else None
        case n if n.contains("\"\"") && !n.contains(",\"\"") => parseNext(lineReader)
        case _ =>
          val line = leftOver.getOrElse("") + nextLine.replace("=", "")
          parser.parseLine(line) match {
            case None => parseNext(lineReader, Some(line))
            case result => result
          }
      }
    }

    parseNext(lineReader)
  }
}

object QuantlibCSVReader {
  val DEFAULT_ENCODING = "UTF-8"

  def open(reader: Reader)(implicit format: CSVFormat): CSVReader = new QuantlibCSVReader(new ReaderLineReader(reader))(format)

  def open(file: File)(implicit format: CSVFormat): CSVReader = {
    open(file, this.DEFAULT_ENCODING)(format)
  }

  def open(file: File, encoding: String)(implicit format: CSVFormat): CSVReader = {
    val fin = new FileInputStream(file)
    try {
      open(new InputStreamReader(fin, encoding))(format)
    } catch {
      case e: UnsupportedEncodingException => fin.close(); throw e
    }
  }
}