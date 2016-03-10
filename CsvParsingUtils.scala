import com.opencsv.CSVReader
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.Try

object CsvParsingUtils {
  def convertOptStr(x: String): Option[String] = if (x.isEmpty) None else Some(x)

  val formatter = DateTimeFormat.forPattern("mm/dd/yyyy")
  def convertDT(x: String): DateTime = DateTime.parse(x, formatter)

  def convertOptInt(x: String): Option[Int] = Try(x.toInt).toOption
  def convertOptD(x: String): Option[Double] = Try(x.toDouble).toOption

  def convertExtra0(line: Array[String], index: Int): PymtModelExtra0 = {
    PymtModelExtra0(convertOptStr(line(index)),
      convertOptStr(line(index + 1)),
      convertOptStr(line(index + 2)),
      convertOptStr(line(index + 3)),
      convertOptStr(line(index + 4))
    )
  }
  def convertExtra1(line: Array[String], index: Int): PymtModelExtra1 = {
    PymtModelExtra1 (convertOptStr(line(index)),
      convertOptStr(line(index + 1)),
      convertOptStr(line(index + 2)),
      convertOptStr(line(index + 3)),
      convertOptStr(line(index + 4)),
      line(index + 5).toDouble,
      convertOptStr(line(index + 6)),
      line(index + 7).toDouble,
      convertOptStr(line(index + 8)),
      convertOptStr(line(index + 9)),
      convertOptStr(line(index + 10)),
      convertOptStr(line(index + 11)),
      convertOptStr(line(index + 12)),
      convertOptStr(line(index + 13)),
      convertOptStr(line(index + 14)),
      convertOptStr(line(index + 15)),
      convertOptStr(line(index + 16)),
      convertOptStr(line(index + 17)),
      convertOptStr(line(index + 18)),
      convertOptStr(line(index + 19))
    )
  }

  def convertExtra2(line: Array[String], index: Int): PymtModelExtra2=
    PymtModelExtra2(
      convertOptStr(line(index)),
      convertOptStr(line(index + 1)),
      convertOptStr(line(index + 2)),
      convertOptStr(line(index + 3)),
      convertOptStr(line(index + 4)),
      convertOptStr(line(index + 5)),
      convertOptStr(line(index + 6)),
      convertOptStr(line(index + 7)),
      convertOptStr(line(index + 8)),
      convertOptStr(line(index + 9)),
      convertOptStr(line(index + 10)),
      convertOptStr(line(index + 11)),
      convertOptStr(line(index + 12)),
      convertOptStr(line(index + 13)),
      convertOptStr(line(index + 14)),
      convertOptStr(line(index + 15)),
      convertOptStr(line(index + 16)),
      convertOptStr(line(index + 17)),
      convertOptStr(line(index + 18))
    )

  def make60long(in: Array[String]): Array[String] = {
    if (in == null) return null
    if (in.size >= 60) {
      in
    } else {
      in ++ Array.fill(60 - in.size)("")
    }
  }
}

class PymtReader(reader: CSVReader) extends Iterator[PymtModel] {
  import CsvParsingUtils._

  var curLine: Array[String] = null
  def hasNext: Boolean = {
    curLine = make60long(reader.readNext)
    curLine != null
  }

  def next: PymtModel= {
    PymtModel(
      convertOptStr(curLine(0)),
      convertOptStr(curLine(1)),
      convertOptStr(curLine(2)),
      convertOptStr(curLine(3)),
      convertOptStr(curLine(4)),
      convertOptStr(curLine(5)),
      convertOptStr(curLine(6)),
      convertOptStr(curLine(7)),
      convertOptStr(curLine(8)),
      convertOptStr(curLine(9)),
      convertOptStr(curLine(10)),
      convertOptStr(curLine(11)),  //recipientState
      convertOptStr(curLine(12)),
      convertOptStr(curLine(13)),
      convertOptStr(curLine(14)),
      convertOptStr(curLine(15)),
      convertOptStr(curLine(16)),
      convertOptStr(curLine(17)),
      convertExtra0(curLine, 18),
      convertExtra1(curLine, 23),
      curLine(43),            //recordId
      convertExtra2(curLine, 44)
    )
  }
}

class LineReader(reader: CSVReader) extends Iterator[Array[String]] {
  var curLine: Array[String] = null
  def hasNext: Boolean = {
    curLine = CsvParsingUtils.make60long(reader.readNext)
    curLine != null
  }
  def next = curLine
}
