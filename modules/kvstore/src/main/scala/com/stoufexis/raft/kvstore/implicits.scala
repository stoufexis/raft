package com.stoufexis.raft.kvstore

import cats.*
import cats.data.*
import cats.effect.*
import doobie.util.{Get, Put}
import io.circe.*
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue, EntityDecoder, EntityEncoder}
import org.http4s.circe.*
import scodec.bits.BitVector

import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.*

object implicits:
  given Contravariant[Encoder] with
    def contramap[A, B](fa: Encoder[A])(f: B => A): Encoder[B] = fa.contramap(f)

  given Functor[Decoder] with
    def map[A, B](fa: Decoder[A])(f: A => B): Decoder[B] = fa.map(f)

  given Put[Index]   = Index.deriveContravariant
  given Get[Index]   = Index.deriveFunctor
  given Codec[Index] = Codec.from(Index.deriveFunctor, Index.deriveContravariant)

  given Put[Term]   = Term.deriveContravariant
  given Get[Term]   = Term.deriveFunctor
  given Codec[Term] = Codec.from(Term.deriveFunctor, Term.deriveContravariant)

  given Put[CommandId]   = CommandId.deriveContravariant
  given Get[CommandId]   = CommandId.deriveFunctor
  given Codec[CommandId] = Codec.from(CommandId.deriveFunctor, CommandId.deriveContravariant)

  given Codec[NodeId] = Codec.derived

  given [A: Codec]: Codec[Command[A]] = Codec.derived

  given [K: scodec.Codec, V: scodec.Codec]: scodec.Codec[Map[K, V]] with
    val codec: scodec.Codec[(K, V)] = summon

    def encode(value: Map[K, V]): scodec.Attempt[BitVector] =
      codec.encodeAll(value)

    def decode(bits: BitVector): scodec.Attempt[scodec.DecodeResult[Map[K, V]]] =
      codec.collect(bits, None).map(_.map(_.toMap))

    def sizeBound: scodec.SizeBound = scodec.SizeBound.unknown

    override def toString = s"set($codec)"

  given [A](using codec: scodec.Codec[A]): scodec.Codec[Set[A]] with
    def encode(value: Set[A]): scodec.Attempt[BitVector] =
      codec.encodeAll(value)

    def decode(bits: BitVector): scodec.Attempt[scodec.DecodeResult[Set[A]]] =
      codec.collect(bits, None)

    def sizeBound: scodec.SizeBound = scodec.SizeBound.unknown

    override def toString = s"set($codec)"

  given [A](using codec: scodec.Codec[A]): scodec.Codec[Seq[A]] with
    def encode(value: Seq[A]): scodec.Attempt[BitVector] =
      codec.encodeAll(value)

    def decode(bits: BitVector): scodec.Attempt[scodec.DecodeResult[Seq[A]]] =
      codec.collect(bits, None)

    def sizeBound: scodec.SizeBound = scodec.SizeBound.unknown

    override def toString = s"set($codec)"

  given QueryParamDecoder[CommandId] with
    def decode(value: QueryParameterValue): ValidatedNel[ParseFailure, CommandId] =
      Validated.Valid(CommandId(value.value))

  given [A: Codec]: Codec[AppendEntries[A]] = Codec.derived
  given Codec[AppendResponse] = Codec.derived
  given Codec[RequestVote]    = Codec.derived
  given Codec[VoteResponse]   = Codec.derived

  given [F[_]: Concurrent, A: Decoder]: EntityDecoder[F, A] = jsonOf
  given [F[_]: Concurrent, A: Encoder]: EntityEncoder[F, A] = jsonEncoderOf
