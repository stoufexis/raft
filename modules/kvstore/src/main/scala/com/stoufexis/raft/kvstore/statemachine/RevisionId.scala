package com.stoufexis.raft.kvstore.statemachine

import cats.data.*
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue}

import com.stoufexis.raft.kvstore.implicits.given
import scodec.bits.*
import io.circe.*

case class RevisionId(revisions: Map[String, Long]) derives CanEqual, scodec.Codec

object RevisionId:
  // Maps to a json string in base64
  given Codec[RevisionId] =
    val enc: Encoder[RevisionId] =
      Encoder[String].contramap(scodec.Codec[RevisionId].encode(_).require.toBase64)

    val dec: Decoder[RevisionId] =
      Decoder[String]
        .emap(BitVector.fromBase64(_).toRight("String was not in base64"))
        .emap(scodec.Codec[RevisionId].decodeValue(_).toEither.left.map(_.message))

    Codec.from(dec, enc)


  given QueryParamDecoder[RevisionId] with
    def decode(value: QueryParameterValue): ValidatedNel[ParseFailure, RevisionId] =
      val v = value.value
      BitVector.fromBase64(v) match
        case Some(bv) =>
          scodec.Codec[RevisionId].decodeValue(bv) match
            case scodec.Attempt.Successful(rid) =>
              Validated.Valid(rid)

            case scodec.Attempt.Failure(cause) =>
              Validated.Invalid(NonEmptyList.of(ParseFailure("Decode failure", v)))

        case None =>
          Validated.Invalid(NonEmptyList.of(ParseFailure("Not a valid base64 string", v)))
