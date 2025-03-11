package k64

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, IOApp, Resource}

import scala.concurrent.duration.DurationInt

final case class Token(value: String) extends AnyVal

object BufferResApp extends IOApp.Simple {

  override def run: IO[Unit] = {
    val res: Resource[IO, Set[Token] => IO[Unit]] =
      Resource.make(IO.pure(theSink _))(_ => IO.unit)

    val buffer    = BufferRes.apply[IO, Token](5, 5.seconds, res)
    val tokenSink = buffer.unsafeRunSync()

    publishTokens(tokenSink) *> tokenSink.release
  }

  private def publishTokens(tokenSink: BufferRes[IO, Token]): IO[Unit] =
    loopWithDelay(20, 500.milliseconds) { i =>
      println(s"Publishing token $i ...")
      tokenSink
        .publish(Token(i.toString))
        .unsafeRunSync()
    }

  private def theSink(tokens: Set[Token]): IO[Unit] = {
    println(s"Read ${tokens.size} elements from buffer ...")
    IO.println(tokens.mkString(">>>", ",", ""))
  }
}
