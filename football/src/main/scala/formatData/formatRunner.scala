package formatData

import java.io.FileWriter

import scala.io.{BufferedSource, Source}

object formatRunner {
  def formatMain(args: Array[String]): Unit = {
    var openedFile : BufferedSource = null
    val fileWriter = new FileWriter("streamData/tweetstream.json")
    var counter = 1

    fileWriter.write("{results:[")

    while (counter <= 110)
    {
      try {
        openedFile = Source.fromFile("twitterstream/tweetstream-part" + counter)
        val content = openedFile.getLines().mkString(",")
        fileWriter.write(content)
      }
      catch {
        case e => {}
      }
      finally {
        if (openedFile != null)
          openedFile.close

        counter += 1
      }
    }

    fileWriter.write("]}")
    fileWriter.close()
  }
}
