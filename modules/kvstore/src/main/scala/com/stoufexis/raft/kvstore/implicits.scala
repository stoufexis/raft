package com.stoufexis.raft.kvstore

import cats.*
import doobie.util.*
import scodec.*
import scodec.Codec.{given_Codec_Long, given_Codec_String}
import scodec.bits.BitVector

import com.stoufexis.raft.kvstore.statemachine.KvCommand
import com.stoufexis.raft.model.*
import com.stoufexis.raft.rpc.AppendEntries
import com.stoufexis.raft.rpc.AppendResponse
import com.stoufexis.raft.rpc.RequestVote
import com.stoufexis.raft.rpc.VoteResponse

object implicits:
  given Contravariant[Encoder] with
    def contramap[A, B](fa: Encoder[A])(f: B => A): Encoder[B] = fa.contramap(f)

  given Functor[Decoder] with
    def map[A, B](fa: Decoder[A])(f: A => B): Decoder[B] = fa.map(f)

  given Put[Index]   = Index.deriveContravariant
  given Get[Index]   = Index.deriveFunctor
  given Codec[Index] = Codec(Index.deriveContravariant, Index.deriveFunctor)

  given Put[Term]   = Term.deriveContravariant
  given Get[Term]   = Term.deriveFunctor
  given Codec[Term] = Codec(Term.deriveContravariant, Term.deriveFunctor)

  given Put[CommandId]   = CommandId.deriveContravariant
  given Get[CommandId]   = CommandId.deriveFunctor
  given Codec[CommandId] = Codec(CommandId.deriveContravariant, CommandId.deriveFunctor)

  given Put[NodeId]   = NodeId.deriveContravariant
  given Get[NodeId]   = NodeId.deriveFunctor
  given Codec[NodeId] = Codec(NodeId.deriveContravariant, NodeId.deriveFunctor)

  given Codec[Nothing] =
    Codec(_ => throw RuntimeException("unreachable"), a => Attempt.Failure(Err("Decoding to nothing")))

  given [A: Codec]: Codec[Command[A]] = Codec.derived

  given [K: Codec, V: Codec]: Codec[Map[K, V]] with
    val codec: Codec[(K, V)] = summon

    def encode(value: Map[K, V]): Attempt[BitVector] =
      codec.encodeAll(value)

    def decode(bits: BitVector): Attempt[DecodeResult[Map[K, V]]] =
      codec.collect(bits, None).map(_.map(_.toMap))

    def sizeBound: SizeBound = SizeBound.unknown

    override def toString = s"set($codec)"

  given [A](using codec: Codec[A]): Codec[Set[A]] with
    def encode(value: Set[A]): Attempt[BitVector] =
      codec.encodeAll(value)

    def decode(bits: BitVector): Attempt[DecodeResult[Set[A]]] =
      codec.collect(bits, None)

    def sizeBound: SizeBound = SizeBound.unknown

    override def toString = s"set($codec)"

  given [A](using codec: Codec[A]): Codec[Seq[A]] with
    def encode(value: Seq[A]): Attempt[BitVector] =
      codec.encodeAll(value)

    def decode(bits: BitVector): Attempt[DecodeResult[Seq[A]]] =
      codec.collect(bits, None)

    def sizeBound: SizeBound = SizeBound.unknown

    override def toString = s"set($codec)"

  given [A: Codec]: Codec[AppendEntries[A]] = Codec.derived
  given Codec[AppendResponse] = Codec.derived
  given Codec[RequestVote]    = Codec.derived
  given Codec[VoteResponse]   = Codec.derived
