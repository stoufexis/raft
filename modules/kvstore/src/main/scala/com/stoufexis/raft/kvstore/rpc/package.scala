package com.stoufexis.raft.kvstore.rpc

case object EmptyValueReceived extends RuntimeException("Received empty value")
