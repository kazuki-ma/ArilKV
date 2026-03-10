------------------------------- MODULE ScriptActiveExpireFreeze -------------------------------
EXTENDS Naturals, TLC

CONSTANT PauseActiveExpireWhenScriptRunning

VARIABLES phase, scriptRunning, wallNow, frozenNow,
          key2Present, key3Present, key2Deadline, key3Deadline,
          expireAttempted

Phases == {
    "Idle",
    "RunningBeforeFirstRestore",
    "RunningAfterFirstRestore",
    "RunningAfterSleep",
    "RunningAfterSecondRestore",
    "Done"
}

Init ==
    /\ phase = "Idle"
    /\ scriptRunning = FALSE
    /\ wallNow = 0
    /\ frozenNow = 0
    /\ key2Present = FALSE
    /\ key3Present = FALSE
    /\ key2Deadline = 0
    /\ key3Deadline = 0
    /\ expireAttempted = FALSE

BeginScriptCriticalSection ==
    /\ phase = "Idle"
    /\ phase' = "RunningBeforeFirstRestore"
    /\ scriptRunning' = TRUE
    /\ frozenNow' = wallNow
    /\ UNCHANGED << wallNow, key2Present, key3Present, key2Deadline, key3Deadline, expireAttempted >>

RestoreFirst ==
    /\ phase = "RunningBeforeFirstRestore"
    /\ phase' = "RunningAfterFirstRestore"
    /\ key2Present' = TRUE
    /\ key2Deadline' = frozenNow + 1
    /\ UNCHANGED << scriptRunning, wallNow, frozenNow, key3Present, key3Deadline, expireAttempted >>

SleepScript ==
    /\ phase = "RunningAfterFirstRestore"
    /\ phase' = "RunningAfterSleep"
    /\ wallNow' = wallNow + 2
    /\ UNCHANGED << scriptRunning, frozenNow, key2Present, key3Present, key2Deadline, key3Deadline, expireAttempted >>

SkipActiveExpireWhileScriptRunning ==
    /\ phase = "RunningAfterSleep"
    /\ scriptRunning
    /\ PauseActiveExpireWhenScriptRunning
    /\ ~expireAttempted
    /\ expireAttempted' = TRUE
    /\ UNCHANGED << phase, scriptRunning, wallNow, frozenNow, key2Present, key3Present, key2Deadline, key3Deadline >>

ActiveExpireWhileScriptRunning ==
    /\ phase = "RunningAfterSleep"
    /\ scriptRunning
    /\ ~PauseActiveExpireWhenScriptRunning
    /\ ~expireAttempted
    /\ key2Present
    /\ wallNow >= key2Deadline
    /\ key2Present' = FALSE
    /\ expireAttempted' = TRUE
    /\ UNCHANGED << phase, scriptRunning, wallNow, frozenNow, key3Present, key2Deadline, key3Deadline >>

RestoreSecond ==
    /\ phase = "RunningAfterSleep"
    /\ expireAttempted
    /\ phase' = "RunningAfterSecondRestore"
    /\ key3Present' = TRUE
    /\ key3Deadline' = frozenNow + 1
    /\ UNCHANGED << scriptRunning, wallNow, frozenNow, key2Present, key2Deadline, expireAttempted >>

EndScriptCriticalSection ==
    /\ phase = "RunningAfterSecondRestore"
    /\ phase' = "Done"
    /\ scriptRunning' = FALSE
    /\ UNCHANGED << wallNow, frozenNow, key2Present, key3Present, key2Deadline, key3Deadline, expireAttempted >>

Next ==
    \/ BeginScriptCriticalSection
    \/ RestoreFirst
    \/ SleepScript
    \/ SkipActiveExpireWhileScriptRunning
    \/ ActiveExpireWhileScriptRunning
    \/ RestoreSecond
    \/ EndScriptCriticalSection

FrozenRestoreInvariant ==
    phase = "RunningAfterSecondRestore" =>
        /\ key2Present
        /\ key3Present
        /\ key2Deadline = key3Deadline

Spec == Init /\ [][Next]_<< phase, scriptRunning, wallNow, frozenNow, key2Present, key3Present, key2Deadline, key3Deadline, expireAttempted >>

=============================================================================
