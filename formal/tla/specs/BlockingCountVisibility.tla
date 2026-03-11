---------------------------- MODULE BlockingCountVisibility ----------------------------
EXTENDS Naturals

\* Model target:
\* `tests/unit/type/list.tcl` "Linked LMOVEs"
\*
\* Runtime shape:
\* - waiter-1 blocks on `BLMOVE list1 -> list2`
\* - waiter-2 blocks on `BLMOVE list2 -> list3`
\* - external test waits for `blocked_clients == 2`
\* - producer issues `RPUSH list1 foo`
\* - producer ACK waits only while a ready blocking waiter is visible
\*
\* Bug mechanism modeled here:
\* - `blocked_clients` is incremented before wait-queue registration becomes visible
\* - external observer sees `blocked_clients == 2` and pushes early
\* - producer ACK sees no ready waiter and is exposed before waiter-1 is registered

CONSTANTS
    MaxSteps,
    AdvertiseAfterRegistration

WaiterStates == {"NEW", "COUNTED", "BLOCKED", "DONE"}

VARIABLES
    waiter1,
    waiter2,
    blockedCount,
    list1HasFoo,
    list2HasFoo,
    list3HasFoo,
    pushed,
    producerAcked,
    observed,
    observedList1HasFoo,
    observedList2HasFoo,
    observedList3HasFoo,
    stepsRemaining

Vars ==
    <<waiter1, waiter2, blockedCount, list1HasFoo, list2HasFoo, list3HasFoo, pushed,
      producerAcked, observed, observedList1HasFoo, observedList2HasFoo, observedList3HasFoo,
      stepsRemaining>>

BoolToNat(v) == IF v THEN 1 ELSE 0

TokenCount ==
    BoolToNat(list1HasFoo) + BoolToNat(list2HasFoo) + BoolToNat(list3HasFoo)

ReadyWaiterVisible ==
    \/ /\ waiter1 = "BLOCKED"
       /\ list1HasFoo
    \/ /\ waiter2 = "BLOCKED"
       /\ list2HasFoo

TypeOK ==
    /\ waiter1 \in WaiterStates
    /\ waiter2 \in WaiterStates
    /\ blockedCount \in 0..2
    /\ list1HasFoo \in BOOLEAN
    /\ list2HasFoo \in BOOLEAN
    /\ list3HasFoo \in BOOLEAN
    /\ pushed \in BOOLEAN
    /\ producerAcked \in BOOLEAN
    /\ observed \in BOOLEAN
    /\ observedList1HasFoo \in BOOLEAN
    /\ observedList2HasFoo \in BOOLEAN
    /\ observedList3HasFoo \in BOOLEAN
    /\ stepsRemaining \in 0..MaxSteps
    /\ AdvertiseAfterRegistration \in BOOLEAN

TokenConservation ==
    IF pushed
        THEN TokenCount = 1
        ELSE TokenCount = 0

ObservedNoIntermediateResidue ==
    observed =>
        /\ ~observedList1HasFoo
        /\ ~observedList2HasFoo
        /\ observedList3HasFoo

BlockedCountMatchesVisibleQueues ==
    blockedCount =
        BoolToNat(waiter1 \in {"COUNTED", "BLOCKED"}) + BoolToNat(waiter2 \in {"COUNTED", "BLOCKED"})

Init ==
    /\ waiter1 = "NEW"
    /\ waiter2 = "NEW"
    /\ blockedCount = 0
    /\ list1HasFoo = FALSE
    /\ list2HasFoo = FALSE
    /\ list3HasFoo = FALSE
    /\ pushed = FALSE
    /\ producerAcked = FALSE
    /\ observed = FALSE
    /\ observedList1HasFoo = FALSE
    /\ observedList2HasFoo = FALSE
    /\ observedList3HasFoo = FALSE
    /\ stepsRemaining = MaxSteps

AdvertiseBlockedClientAfterRegistration(which) ==
    /\ stepsRemaining > 0
    /\ AdvertiseAfterRegistration
    /\ which \in {1, 2}
    /\ IF which = 1
        THEN waiter1 = "NEW"
        ELSE waiter2 = "NEW"
    /\ IF which = 1
        THEN waiter1' = "BLOCKED"
        ELSE waiter1' = waiter1
    /\ IF which = 2
        THEN waiter2' = "BLOCKED"
        ELSE waiter2' = waiter2
    /\ blockedCount' = blockedCount + 1
    /\ UNCHANGED
        <<list1HasFoo, list2HasFoo, list3HasFoo, pushed, producerAcked, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

CountBlockedClientBeforeRegistration(which) ==
    /\ stepsRemaining > 0
    /\ ~AdvertiseAfterRegistration
    /\ which \in {1, 2}
    /\ IF which = 1
        THEN waiter1 = "NEW"
        ELSE waiter2 = "NEW"
    /\ IF which = 1
        THEN waiter1' = "COUNTED"
        ELSE waiter1' = waiter1
    /\ IF which = 2
        THEN waiter2' = "COUNTED"
        ELSE waiter2' = waiter2
    /\ blockedCount' = blockedCount + 1
    /\ UNCHANGED
        <<list1HasFoo, list2HasFoo, list3HasFoo, pushed, producerAcked, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

RegisterBlockingWait(which) ==
    /\ stepsRemaining > 0
    /\ which \in {1, 2}
    /\ IF which = 1
        THEN waiter1 = "COUNTED"
        ELSE waiter2 = "COUNTED"
    /\ IF which = 1
        THEN waiter1' = "BLOCKED"
        ELSE waiter1' = waiter1
    /\ IF which = 2
        THEN waiter2' = "BLOCKED"
        ELSE waiter2' = waiter2
    /\ UNCHANGED
        <<blockedCount, list1HasFoo, list2HasFoo, list3HasFoo, pushed, producerAcked, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

ProducerRpushList1 ==
    /\ stepsRemaining > 0
    /\ ~pushed
    /\ blockedCount = 2
    /\ pushed' = TRUE
    /\ list1HasFoo' = TRUE
    /\ UNCHANGED
        <<waiter1, waiter2, blockedCount, list2HasFoo, list3HasFoo, producerAcked, observed,
          observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

WakeWaiter1Blmove ==
    /\ stepsRemaining > 0
    /\ waiter1 = "BLOCKED"
    /\ list1HasFoo
    /\ waiter1' = "DONE"
    /\ blockedCount' = blockedCount - 1
    /\ list1HasFoo' = FALSE
    /\ list2HasFoo' = TRUE
    /\ UNCHANGED
        <<waiter2, list3HasFoo, pushed, producerAcked, observed, observedList1HasFoo,
          observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

WakeWaiter2Blmove ==
    /\ stepsRemaining > 0
    /\ waiter2 = "BLOCKED"
    /\ list2HasFoo
    /\ waiter2' = "DONE"
    /\ blockedCount' = blockedCount - 1
    /\ list2HasFoo' = FALSE
    /\ list3HasFoo' = TRUE
    /\ UNCHANGED
        <<waiter1, list1HasFoo, pushed, producerAcked, observed, observedList1HasFoo,
          observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

ProducerAck ==
    /\ stepsRemaining > 0
    /\ pushed
    /\ ~producerAcked
    /\ ~ReadyWaiterVisible
    /\ producerAcked' = TRUE
    /\ UNCHANGED
        <<waiter1, waiter2, blockedCount, list1HasFoo, list2HasFoo, list3HasFoo, pushed,
          observed, observedList1HasFoo, observedList2HasFoo, observedList3HasFoo>>
    /\ stepsRemaining' = stepsRemaining - 1

ObservePostProducerAck ==
    /\ producerAcked
    /\ ~observed
    /\ observed' = TRUE
    /\ observedList1HasFoo' = list1HasFoo
    /\ observedList2HasFoo' = list2HasFoo
    /\ observedList3HasFoo' = list3HasFoo
    /\ UNCHANGED
        <<waiter1, waiter2, blockedCount, list1HasFoo, list2HasFoo, list3HasFoo, pushed,
          producerAcked>>
    /\ stepsRemaining' = IF stepsRemaining > 0 THEN stepsRemaining - 1 ELSE 0

DoneStutter ==
    /\ (stepsRemaining = 0 \/ observed)
    /\ UNCHANGED Vars

Next ==
    \/ \E which \in {1, 2}: AdvertiseBlockedClientAfterRegistration(which)
    \/ \E which \in {1, 2}: CountBlockedClientBeforeRegistration(which)
    \/ \E which \in {1, 2}: RegisterBlockingWait(which)
    \/ ProducerRpushList1
    \/ WakeWaiter1Blmove
    \/ WakeWaiter2Blmove
    \/ ProducerAck
    \/ ObservePostProducerAck
    \/ DoneStutter

Spec ==
    Init /\ [][Next]_Vars

THEOREM Spec => []TypeOK

================================================================================
