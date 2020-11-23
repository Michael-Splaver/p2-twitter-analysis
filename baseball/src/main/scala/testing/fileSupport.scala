package testing

import java.io.PrintWriter
import java.time.Instant
import java.time.Instant.ofEpochMilli
import org.json4s.native.JsonMethods.{parse, pretty, render}
import org.json4s.native.Serialization
import org.json4s.{CustomSerializer, DefaultFormats, Formats, JLong}

import scala.io.Source

object fileSupport {

  implicit lazy val json4sFormats: Formats = DefaultFormats.preservingEmptyValues + InstantSerializer

  private def toJson[T <: AnyRef](value: T): String = pretty(render(parse(Serialization.write(value))))

  private def jsonAs[T: Manifest](json: String): T = Serialization.read[T](json)

  private def writeToFile(filename: String, content: String) = new PrintWriter(filename) {
    write(content)
    close()
  }

  def getJson[T <: AnyRef](value: T) = parse(Serialization.write(value))

  def readFromFile(filename: String): String = Source.fromFile(filename).mkString

  def toFileAsJson[T <: AnyRef](filename: String, t: T) = writeToFile(filename, toJson(t))

  def toFile[T <: AnyRef](filename: String, json: String) = writeToFile(filename, json)

  def fromJsonFileAs[T: Manifest](filename: String): T = jsonAs(readFromFile(filename))

}

private object InstantSerializer extends CustomSerializer[Instant](
  format => (
    { case jl: JLong => ofEpochMilli(jl.num) },
    { case inst: Instant => JLong(inst.toEpochMilli) }
  )
)
