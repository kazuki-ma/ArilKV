-------------------------- MODULE PipelineStresserTimeout --------------------------
EXTENDS Naturals, Sequences

\* Code-derived model target:
\*   connection_handler::handle_connection read/parse/execute/write loop
\*   + request_lifecycle::handle_set / handle_get behavior
\*
\* Test target:
\*   tests/unit/other.tcl :: "PIPELINING stresser (also a regression for the old epoll bug)"
\*
\* Test structure:
\* 1) client sends N inline pairs: "SET key:i v" then "GET key:i"
\* 2) one flush
\* 3) for i in [0, N):
\*      gets line      ; ignores SET reply content
\*      gets countLine ; expects "$<len>" for GET
\*      read count
\*      read CRLF
\*
\* In Tcl, if countLine is "$-1", then read(-1) blocks until EOF -> timeout/hang.

CONSTANTS PipelinePairs, MaxSteps, StoreCapacity

Phases == {"SENDING", "READING", "DONE", "HUNG"}
ReadPhases == {"EXPECT_SET_LINE", "EXPECT_GET_LEN", "EXPECT_GET_VALUE"}
Ops == {"SET", "GET"}
RespKinds == {"OK", "ERR", "BULK", "NULL"}

NoPendingKey == PipelinePairs
NullKey == PipelinePairs + 1
Keys == 0..(PipelinePairs - 1)
PendingKeys == Keys \cup {NoPendingKey}
RespKeys == Keys \cup {NullKey}

Cmd == [op: Ops, key: Keys]
Resp == [kind: RespKinds, key: RespKeys]

VARIABLES
    phase,
    nextSend,
    clientBuffered,
    socketToServer,
    receiveBuffer,
    db,
    storeFreeSlots,
    responsesBuffer,
    socketToClient,
    readIndex,
    readPhase,
    pendingBulkKey,
    stepsRemaining

Vars ==
    <<phase, nextSend, clientBuffered, socketToServer, receiveBuffer, db, storeFreeSlots,
      responsesBuffer, socketToClient, readIndex, readPhase, pendingBulkKey, stepsRemaining>>

TypeOK ==
    /\ phase \in Phases
    /\ nextSend \in 0..PipelinePairs
    /\ clientBuffered \in Seq(Cmd)
    /\ socketToServer \in Seq(Cmd)
    /\ receiveBuffer \in Seq(Cmd)
    /\ db \subseteq Keys
    /\ storeFreeSlots \in 0..StoreCapacity
    /\ responsesBuffer \in Seq(Resp)
    /\ socketToClient \in Seq(Resp)
    /\ readIndex \in 0..PipelinePairs
    /\ readPhase \in ReadPhases
    /\ pendingBulkKey \in PendingKeys
    /\ stepsRemaining \in 0..MaxSteps

DoneReadsAllPairs ==
    phase = "DONE" =>
        /\ readIndex = PipelinePairs
        /\ readPhase = "EXPECT_SET_LINE"
        /\ pendingBulkKey = NoPendingKey

NotHung ==
    phase # "HUNG"

Init ==
    /\ phase = "SENDING"
    /\ nextSend = 0
    /\ clientBuffered = <<>>
    /\ socketToServer = <<>>
    /\ receiveBuffer = <<>>
    /\ db = {}
    /\ storeFreeSlots = StoreCapacity
    /\ responsesBuffer = <<>>
    /\ socketToClient = <<>>
    /\ readIndex = 0
    /\ readPhase = "EXPECT_SET_LINE"
    /\ pendingBulkKey = NoPendingKey
    /\ stepsRemaining = MaxSteps

ClientBufferSetGetPair ==
    /\ stepsRemaining > 0
    /\ phase = "SENDING"
    /\ nextSend < PipelinePairs
    /\ clientBuffered' =
        Append(
            Append(clientBuffered, [op |-> "SET", key |-> nextSend]),
            [op |-> "GET", key |-> nextSend]
        )
    /\ nextSend' = nextSend + 1
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, socketToServer, receiveBuffer, db, storeFreeSlots, responsesBuffer,
                  socketToClient, readIndex, readPhase, pendingBulkKey>>

ClientFlushBuffered ==
    /\ stepsRemaining > 0
    /\ phase = "SENDING"
    /\ nextSend = PipelinePairs
    /\ phase' = "READING"
    /\ socketToServer' = socketToServer \o clientBuffered
    /\ clientBuffered' = <<>>
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<nextSend, receiveBuffer, db, storeFreeSlots, responsesBuffer, socketToClient,
                  readIndex, readPhase, pendingBulkKey>>

ServerReadAndDrainAvailable ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ Len(socketToServer) > 0
    /\ \E readCount \in 1..Len(socketToServer):
        /\ receiveBuffer' = receiveBuffer \o SubSeq(socketToServer, 1, readCount)
        /\ socketToServer' = SubSeq(socketToServer, readCount + 1, Len(socketToServer))
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, nextSend, clientBuffered, db, storeFreeSlots, responsesBuffer,
                  socketToClient, readIndex, readPhase, pendingBulkKey>>

ServerProcessSetApply ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ Len(receiveBuffer) > 0
    /\ Head(receiveBuffer).op = "SET"
    /\ storeFreeSlots > 0
    /\ LET key == Head(receiveBuffer).key
       IN /\ receiveBuffer' = Tail(receiveBuffer)
          /\ db' = db \cup {key}
          /\ storeFreeSlots' = storeFreeSlots - 1
          /\ responsesBuffer' =
                Append(responsesBuffer, [kind |-> "OK", key |-> key])
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, nextSend, clientBuffered, socketToServer, socketToClient, readIndex,
                  readPhase, pendingBulkKey>>

ServerProcessSetRejectedCapacity ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ Len(receiveBuffer) > 0
    /\ Head(receiveBuffer).op = "SET"
    /\ storeFreeSlots = 0
    /\ LET key == Head(receiveBuffer).key
       IN /\ receiveBuffer' = Tail(receiveBuffer)
          /\ db' = db
          /\ storeFreeSlots' = storeFreeSlots
          /\ responsesBuffer' =
                Append(responsesBuffer, [kind |-> "ERR", key |-> key])
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, nextSend, clientBuffered, socketToServer, socketToClient, readIndex,
                  readPhase, pendingBulkKey>>

ServerProcessGet ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ Len(receiveBuffer) > 0
    /\ Head(receiveBuffer).op = "GET"
    /\ LET key == Head(receiveBuffer).key
       IN /\ receiveBuffer' = Tail(receiveBuffer)
          /\ db' = db
          /\ storeFreeSlots' = storeFreeSlots
          /\ responsesBuffer' =
                IF key \in db
                    THEN Append(responsesBuffer, [kind |-> "BULK", key |-> key])
                    ELSE Append(responsesBuffer, [kind |-> "NULL", key |-> NullKey])
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, nextSend, clientBuffered, socketToServer, socketToClient, readIndex,
                  readPhase, pendingBulkKey>>

ServerProcessOne ==
    \/ ServerProcessSetApply
    \/ ServerProcessSetRejectedCapacity
    \/ ServerProcessGet

ServerFlushResponses ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ Len(receiveBuffer) = 0
    /\ Len(responsesBuffer) > 0
    /\ socketToClient' = socketToClient \o responsesBuffer
    /\ responsesBuffer' = <<>>
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, nextSend, clientBuffered, socketToServer, receiveBuffer, db,
                  storeFreeSlots, readIndex, readPhase, pendingBulkKey>>

ClientReadSetLine ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ readIndex < PipelinePairs
    /\ readPhase = "EXPECT_SET_LINE"
    /\ Len(socketToClient) > 0
    /\ socketToClient' = Tail(socketToClient)
    /\ readPhase' = "EXPECT_GET_LEN"
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, nextSend, clientBuffered, socketToServer, receiveBuffer, db,
                  storeFreeSlots, responsesBuffer, readIndex, pendingBulkKey>>

ClientReadGetLenBulk ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ readIndex < PipelinePairs
    /\ readPhase = "EXPECT_GET_LEN"
    /\ Len(socketToClient) > 0
    /\ Head(socketToClient).kind = "BULK"
    /\ socketToClient' = Tail(socketToClient)
    /\ readPhase' = "EXPECT_GET_VALUE"
    /\ pendingBulkKey' = Head(socketToClient).key
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<phase, nextSend, clientBuffered, socketToServer, receiveBuffer, db,
                  storeFreeSlots, responsesBuffer, readIndex>>

ClientReadLenNullBulkHang ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ readIndex < PipelinePairs
    /\ readPhase = "EXPECT_GET_LEN"
    /\ Len(socketToClient) > 0
    /\ Head(socketToClient).kind = "NULL"
    /\ socketToClient' = Tail(socketToClient)
    /\ phase' = "HUNG"
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<nextSend, clientBuffered, socketToServer, receiveBuffer, db, storeFreeSlots,
                  responsesBuffer, readIndex, readPhase, pendingBulkKey>>

ClientReadLenUnexpectedHang ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ readIndex < PipelinePairs
    /\ readPhase = "EXPECT_GET_LEN"
    /\ Len(socketToClient) > 0
    /\ ~(Head(socketToClient).kind \in {"BULK", "NULL"})
    /\ socketToClient' = Tail(socketToClient)
    /\ phase' = "HUNG"
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<nextSend, clientBuffered, socketToServer, receiveBuffer, db, storeFreeSlots,
                  responsesBuffer, readIndex, readPhase, pendingBulkKey>>

ClientReadValue ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ readIndex < PipelinePairs
    /\ readPhase = "EXPECT_GET_VALUE"
    /\ pendingBulkKey = readIndex
    /\ readIndex' = readIndex + 1
    /\ readPhase' = "EXPECT_SET_LINE"
    /\ pendingBulkKey' = NoPendingKey
    /\ phase' = IF readIndex' = PipelinePairs THEN "DONE" ELSE "READING"
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<nextSend, clientBuffered, socketToServer, receiveBuffer, db, storeFreeSlots,
                  responsesBuffer, socketToClient>>

ClientReadValueUnexpectedHang ==
    /\ stepsRemaining > 0
    /\ phase = "READING"
    /\ readIndex < PipelinePairs
    /\ readPhase = "EXPECT_GET_VALUE"
    /\ pendingBulkKey # readIndex
    /\ phase' = "HUNG"
    /\ stepsRemaining' = stepsRemaining - 1
    /\ UNCHANGED <<nextSend, clientBuffered, socketToServer, receiveBuffer, db, storeFreeSlots,
                  responsesBuffer, socketToClient, readIndex, readPhase, pendingBulkKey>>

StepBudgetExhaustedHang ==
    /\ phase \in {"SENDING", "READING"}
    /\ stepsRemaining = 0
    /\ phase' = "HUNG"
    /\ UNCHANGED <<nextSend, clientBuffered, socketToServer, receiveBuffer, db, storeFreeSlots,
                  responsesBuffer, socketToClient, readIndex, readPhase, pendingBulkKey,
                  stepsRemaining>>

TerminalStutter ==
    /\ phase \in {"DONE", "HUNG"}
    /\ UNCHANGED Vars

Next ==
    \/ ClientBufferSetGetPair
    \/ ClientFlushBuffered
    \/ ServerReadAndDrainAvailable
    \/ ServerProcessOne
    \/ ServerFlushResponses
    \/ ClientReadSetLine
    \/ ClientReadGetLenBulk
    \/ ClientReadLenNullBulkHang
    \/ ClientReadLenUnexpectedHang
    \/ ClientReadValue
    \/ ClientReadValueUnexpectedHang
    \/ StepBudgetExhaustedHang
    \/ TerminalStutter

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
