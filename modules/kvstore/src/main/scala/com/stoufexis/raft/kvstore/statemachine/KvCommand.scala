package com.stoufexis.raft.kvstore.statemachine

import scodec.Codec

import com.stoufexis.raft.kvstore.implicits.given

enum KvCommand derives CanEqual, Codec:
  case Get(keys: Set[String])
  case Update(sets: Map[String, Option[String]])
  case TransactionUpdate(revisions: RevisionId, sets: Map[String, Option[String]])
