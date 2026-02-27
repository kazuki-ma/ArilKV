-------------------------- MODULE MultiExecBlockingTimeout --------------------------
EXTENDS Naturals, Sequences

\* Model target:
\*   tests/unit/multi.tcl :: "Blocking commands ignores the timeout" (line 725)
\*
\* Test script (modeled as-is, in order):
\* 1) XGROUP CREATE s{t} g $ MKSTREAM
\* 2) MULTI
\* 3) queue:
\*    BLPOP/BRPOP/BRPOPLPUSH/BLMOVE/BZPOPMIN/BZPOPMAX/XREAD/XREADGROUP
\* 4) EXEC -> expects 8 empty replies and no blocking wait

CONSTANTS MaxSteps, IgnoreBlockingTimeoutInExec

ScriptPhases == {"START", "GROUP_READY", "MULTI_OPEN", "QUEUED", "EXEC_RUNNING", "DONE", "HUNG"}
BlockingOps == {"BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BZPOPMIN", "BZPOPMAX", "XREAD", "XREADGROUP"}
ReplyKinds == {"EMPTY"}

ExpectedQueue ==
    <<"BLPOP", "BRPOP", "BRPOPLPUSH", "BLMOVE", "BZPOPMIN", "BZPOPMAX", "XREAD", "XREADGROUP">>

VARIABLES
    phase,
    inMulti,
    queue,
    execIndex,
    replies,
    streamExists,
    groupExists,
    emptyListHasData,
    emptyList1HasData,
    emptyZsetHasMember,
    streamHasEntry,
    stepsRemaining

Vars ==
    <<phase, inMulti, queue, execIndex, replies, streamExists, groupExists, emptyListHasData,
      emptyList1HasData, emptyZsetHasMember, streamHasEntry, stepsRemaining>>

IsPrefix(prefix, full) ==
    /\ Len(prefix) <= Len(full)
    /\ \A i \in 1..Len(prefix): prefix[i] = full[i]

WouldBlock(cmd) ==
    \/ (cmd \in {"BLPOP", "BRPOP"} /\ ~emptyListHasData)
    \/ (cmd \in {"BRPOPLPUSH", "BLMOVE"} /\ ~emptyList1HasData)
    \/ (cmd \in {"BZPOPMIN", "BZPOPMAX"} /\ ~emptyZsetHasMember)
    \/ (cmd = "XREAD" /\ ~streamHasEntry)
    \/ (cmd = "XREADGROUP" /\ ~streamHasEntry)

TypeOK ==
    /\ phase \in ScriptPhases
    /\ inMulti \in BOOLEAN
    /\ queue \in Seq(BlockingOps)
    /\ execIndex \in 1..(Len(ExpectedQueue) + 1)
    /\ replies \in Seq(ReplyKinds)
    /\ streamExists \in BOOLEAN
    /\ groupExists \in BOOLEAN
    /\ emptyListHasData \in BOOLEAN
    /\ emptyList1HasData \in BOOLEAN
    /\ emptyZsetHasMember \in BOOLEAN
    /\ streamHasEntry \in BOOLEAN
    /\ stepsRemaining \in 0..MaxSteps
    /\ IgnoreBlockingTimeoutInExec \in BOOLEAN

QueueIsScriptPrefix ==
    IsPrefix(queue, ExpectedQueue)

DoneMatchesExpectedTestResult ==
    phase = "DONE" =>
        /\ Len(queue) = Len(ExpectedQueue)
        /\ Len(replies) = Len(ExpectedQueue)
        /\ \A i \in 1..Len(replies): replies[i] = "EMPTY"

NotHung ==
    phase # "HUNG"

Init ==
    /\ phase = "START"
    /\ inMulti = FALSE
    /\ queue = <<>>
    /\ execIndex = 1
    /\ replies = <<>>
    /\ streamExists = FALSE
    /\ groupExists = FALSE
    /\ emptyListHasData = FALSE
    /\ emptyList1HasData = FALSE
    /\ emptyZsetHasMember = FALSE
    /\ streamHasEntry = FALSE
    /\ stepsRemaining = MaxSteps

CmdXgroupCreate ==
    /\ stepsRemaining > 0
    /\ phase = "START"
    /\ ~streamExists
    /\ phase' = "GROUP_READY"
    /\ streamExists' = TRUE
    /\ groupExists' = TRUE
    /\ UNCHANGED <<inMulti, queue, execIndex, replies, emptyListHasData, emptyList1HasData,
                  emptyZsetHasMember, streamHasEntry>>
    /\ stepsRemaining' = stepsRemaining - 1

CmdMulti ==
    /\ stepsRemaining > 0
    /\ phase = "GROUP_READY"
    /\ ~inMulti
    /\ phase' = "MULTI_OPEN"
    /\ inMulti' = TRUE
    /\ UNCHANGED <<queue, execIndex, replies, streamExists, groupExists, emptyListHasData,
                  emptyList1HasData, emptyZsetHasMember, streamHasEntry>>
    /\ stepsRemaining' = stepsRemaining - 1

QueueCmd(cmd, expectedLen) ==
    /\ stepsRemaining > 0
    /\ phase = IF expectedLen = 0 THEN "MULTI_OPEN" ELSE "QUEUED"
    /\ inMulti
    /\ Len(queue) = expectedLen
    /\ queue' = Append(queue, cmd)
    /\ phase' = "QUEUED"
    /\ UNCHANGED <<inMulti, execIndex, replies, streamExists, groupExists, emptyListHasData,
                  emptyList1HasData, emptyZsetHasMember, streamHasEntry>>
    /\ stepsRemaining' = stepsRemaining - 1

QueueBlpop ==
    QueueCmd("BLPOP", 0)

QueueBrpop ==
    QueueCmd("BRPOP", 1)

QueueBrpoplpush ==
    QueueCmd("BRPOPLPUSH", 2)

QueueBlmove ==
    QueueCmd("BLMOVE", 3)

QueueBzpopmin ==
    QueueCmd("BZPOPMIN", 4)

QueueBzpopmax ==
    QueueCmd("BZPOPMAX", 5)

QueueXread ==
    QueueCmd("XREAD", 6)

QueueXreadgroup ==
    /\ groupExists
    /\ QueueCmd("XREADGROUP", 7)

CmdExecBegin ==
    /\ stepsRemaining > 0
    /\ phase = "QUEUED"
    /\ inMulti
    /\ Len(queue) = Len(ExpectedQueue)
    /\ phase' = "EXEC_RUNNING"
    /\ inMulti' = FALSE
    /\ UNCHANGED <<queue, execIndex, replies, streamExists, groupExists, emptyListHasData,
                  emptyList1HasData, emptyZsetHasMember, streamHasEntry>>
    /\ stepsRemaining' = stepsRemaining - 1

ExecProcessOne ==
    /\ stepsRemaining > 0
    /\ phase = "EXEC_RUNNING"
    /\ execIndex <= Len(queue)
    /\ LET cmd == queue[execIndex]
           blocks == WouldBlock(cmd)
       IN /\ IF blocks /\ ~IgnoreBlockingTimeoutInExec
              THEN /\ phase' = "HUNG"
                   /\ execIndex' = execIndex
                   /\ replies' = replies
              ELSE /\ replies' = Append(replies, "EMPTY")
                   /\ execIndex' = execIndex + 1
                   /\ phase' = IF execIndex = Len(queue) THEN "DONE" ELSE "EXEC_RUNNING"
    /\ UNCHANGED <<inMulti, queue, streamExists, groupExists, emptyListHasData, emptyList1HasData,
                  emptyZsetHasMember, streamHasEntry>>
    /\ stepsRemaining' = stepsRemaining - 1

DoneStutter ==
    /\ (phase \in {"DONE", "HUNG"} \/ stepsRemaining = 0)
    /\ UNCHANGED Vars

Next ==
    \/ CmdXgroupCreate
    \/ CmdMulti
    \/ QueueBlpop
    \/ QueueBrpop
    \/ QueueBrpoplpush
    \/ QueueBlmove
    \/ QueueBzpopmin
    \/ QueueBzpopmax
    \/ QueueXread
    \/ QueueXreadgroup
    \/ CmdExecBegin
    \/ ExecProcessOne
    \/ DoneStutter

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
