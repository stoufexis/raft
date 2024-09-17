package com.stoufexis.raft.kvstore.statemachine

import cats.data.*
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue}
import scodec.*
import scodec.bits.BitVector

import com.stoufexis.raft.kvstore.implicits.given

case class RevisionId(revisions: Map[String, Long]) derives CanEqual, Codec

object RevisionId:
  given QueryParamDecoder[RevisionId] with
    def decode(value: QueryParameterValue): ValidatedNel[ParseFailure, RevisionId] =
      val v = value.value
      BitVector.fromBase64(v) match
        case Some(bv) =>
          Codec[RevisionId].decodeValue(bv) match
            case Attempt.Successful(rid) =>
              Validated.Valid(rid)

            case Attempt.Failure(cause) =>
              Validated.Invalid(NonEmptyList.of(ParseFailure("Decode failure", v)))

        case None =>
          Validated.Invalid(NonEmptyList.of(ParseFailure("Not a valid base64 string", v)))
