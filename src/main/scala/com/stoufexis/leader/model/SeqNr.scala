package com.stoufexis.leader.model

opaque type SeqNr = Int

object SeqNr:
  given CanEqual[SeqNr, SeqNr] = CanEqual.derived
