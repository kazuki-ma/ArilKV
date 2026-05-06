//! Debug/test-only concurrency diagnostics and deterministic sync points.

use std::ops::Deref;
use std::ops::DerefMut;
use std::sync::LockResult;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::time::Duration;

#[cfg(test)]
pub static SYNC_TEST_MUTEX: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockClass {
    Store,
    ObjectStore,
    Expirations,
    KeyRegistry,
    ObjectKeyRegistry,
}

impl LockClass {
    #[cfg(any(debug_assertions, test))]
    #[inline]
    fn order(self) -> u8 {
        match self {
            Self::Store => 10,
            Self::ObjectStore => 20,
            Self::Expirations => 30,
            Self::KeyRegistry => 40,
            Self::ObjectKeyRegistry => 50,
        }
    }
}

pub struct OrderedMutex<T> {
    inner: Mutex<T>,
    class: LockClass,
    name: &'static str,
}

impl<T> OrderedMutex<T> {
    pub fn new(value: T, class: LockClass, name: &'static str) -> Self {
        Self {
            inner: Mutex::new(value),
            class,
            name,
        }
    }

    pub fn lock(&self) -> LockResult<OrderedMutexGuard<'_, T>> {
        before_lock(self.class, self.name);
        match self.inner.lock() {
            Ok(guard) => {
                lock_acquired(self.class, self.name);
                Ok(OrderedMutexGuard::new(guard, self.class, self.name))
            }
            Err(error) => {
                lock_acquired(self.class, self.name);
                let guard = OrderedMutexGuard::new(error.into_inner(), self.class, self.name);
                Err(PoisonError::new(guard))
            }
        }
    }
}

pub struct OrderedMutexGuard<'a, T> {
    guard: MutexGuard<'a, T>,
    class: LockClass,
    name: &'static str,
}

impl<'a, T> OrderedMutexGuard<'a, T> {
    fn new(guard: MutexGuard<'a, T>, class: LockClass, name: &'static str) -> Self {
        Self { guard, class, name }
    }
}

impl<T> Drop for OrderedMutexGuard<'_, T> {
    fn drop(&mut self) {
        lock_released(self.class, self.name);
    }
}

impl<T> Deref for OrderedMutexGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T> DerefMut for OrderedMutexGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

#[cfg(any(debug_assertions, test))]
thread_local! {
    static HELD_LOCKS: std::cell::RefCell<Vec<(LockClass, &'static str)>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[inline]
fn before_lock(class: LockClass, name: &'static str) {
    #[cfg(any(debug_assertions, test))]
    HELD_LOCKS.with(|held| {
        let held = held.borrow();
        if let Some((previous_class, previous_name)) = held
            .iter()
            .find(|(held_class, _)| held_class.order() > class.order())
        {
            panic!(
                "lock order violation: acquiring {name} ({class:?}) while holding \
                 {previous_name} ({previous_class:?})"
            );
        }
    });
    #[cfg(not(any(debug_assertions, test)))]
    let _ = (class, name);
}

#[inline]
fn lock_acquired(class: LockClass, name: &'static str) {
    #[cfg(any(debug_assertions, test))]
    HELD_LOCKS.with(|held| {
        held.borrow_mut().push((class, name));
    });
    #[cfg(not(any(debug_assertions, test)))]
    let _ = (class, name);
}

#[inline]
fn lock_released(class: LockClass, name: &'static str) {
    #[cfg(any(debug_assertions, test))]
    HELD_LOCKS.with(|held| {
        let mut held = held.borrow_mut();
        let (released_class, released_name) = held
            .pop()
            .expect("lock tracker underflow while releasing mutex");
        assert_eq!(
            (released_class, released_name),
            (class, name),
            "lock tracker mismatch on release"
        );
    });
    #[cfg(not(any(debug_assertions, test)))]
    let _ = (class, name);
}

#[cfg(any(debug_assertions, test))]
#[derive(Default)]
struct SyncPointState {
    hits: usize,
    blocked: bool,
}

#[cfg(any(debug_assertions, test))]
struct SyncPoint {
    state: Mutex<SyncPointState>,
    condvar: std::sync::Condvar,
}

#[cfg(any(debug_assertions, test))]
impl Default for SyncPoint {
    fn default() -> Self {
        Self {
            state: Mutex::new(SyncPointState::default()),
            condvar: std::sync::Condvar::new(),
        }
    }
}

#[cfg(any(debug_assertions, test))]
fn all_sync_points()
-> &'static Mutex<std::collections::HashMap<&'static str, std::sync::Arc<SyncPoint>>> {
    static POINTS: std::sync::OnceLock<
        Mutex<std::collections::HashMap<&'static str, std::sync::Arc<SyncPoint>>>,
    > = std::sync::OnceLock::new();
    POINTS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

#[cfg(any(debug_assertions, test))]
fn get_sync_point(name: &'static str) -> std::sync::Arc<SyncPoint> {
    let mut points = all_sync_points()
        .lock()
        .expect("sync-point registry mutex poisoned");
    points
        .entry(name)
        .or_insert_with(|| std::sync::Arc::new(SyncPoint::default()))
        .clone()
}

pub fn hit_sync_point(name: &'static str) {
    #[cfg(any(debug_assertions, test))]
    {
        let point = get_sync_point(name);
        let mut state = point.state.lock().expect("sync-point mutex poisoned");
        state.hits += 1;
        point.condvar.notify_all();
        while state.blocked {
            state = point
                .condvar
                .wait(state)
                .expect("sync-point mutex poisoned while waiting");
        }
    }
    #[cfg(not(any(debug_assertions, test)))]
    let _ = name;
}

pub fn block_sync_point(name: &'static str) {
    #[cfg(any(debug_assertions, test))]
    {
        let point = get_sync_point(name);
        let mut state = point.state.lock().expect("sync-point mutex poisoned");
        state.blocked = true;
    }
    #[cfg(not(any(debug_assertions, test)))]
    let _ = name;
}

pub fn unblock_sync_point(name: &'static str) {
    #[cfg(any(debug_assertions, test))]
    {
        let point = get_sync_point(name);
        let mut state = point.state.lock().expect("sync-point mutex poisoned");
        state.blocked = false;
        point.condvar.notify_all();
    }
    #[cfg(not(any(debug_assertions, test)))]
    let _ = name;
}

pub fn wait_for_sync_point_hits(
    name: &'static str,
    expected_hits: usize,
    timeout: Duration,
) -> bool {
    #[cfg(any(debug_assertions, test))]
    {
        let point = get_sync_point(name);
        let deadline = std::time::Instant::now() + timeout;
        let mut state = point.state.lock().expect("sync-point mutex poisoned");
        while state.hits < expected_hits {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, timeout_result) = point
                .condvar
                .wait_timeout(state, remaining)
                .expect("sync-point mutex poisoned while waiting");
            state = next_state;
            if timeout_result.timed_out() && state.hits < expected_hits {
                return false;
            }
        }
        true
    }
    #[cfg(not(any(debug_assertions, test)))]
    {
        let _ = (name, expected_hits, timeout);
        true
    }
}

pub fn reset_sync_points() {
    #[cfg(any(debug_assertions, test))]
    {
        let mut points = all_sync_points()
            .lock()
            .expect("sync-point registry mutex poisoned");
        points.clear();
    }
}

#[macro_export]
macro_rules! debug_sync_point {
    ($name:expr) => {{
        $crate::debug_concurrency::hit_sync_point($name);
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn ordered_mutex_accepts_declared_order() {
        let store = OrderedMutex::new(0usize, LockClass::Store, "store");
        let expirations = OrderedMutex::new(0usize, LockClass::Expirations, "expirations");
        let _guard_a = store.lock().unwrap();
        let _guard_b = expirations.lock().unwrap();
    }

    #[test]
    #[should_panic(expected = "lock order violation")]
    fn ordered_mutex_panics_on_inverted_order() {
        let store = OrderedMutex::new(0usize, LockClass::Store, "store");
        let expirations = OrderedMutex::new(0usize, LockClass::Expirations, "expirations");
        let _guard_a = expirations.lock().unwrap();
        let _guard_b = store.lock().unwrap();
    }

    #[test]
    fn sync_point_can_block_and_release_thread() {
        let _test_guard = SYNC_TEST_MUTEX.lock().unwrap();
        reset_sync_points();
        block_sync_point("test.point");
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            hit_sync_point("test.point");
            done_tx.send(()).unwrap();
        });

        assert!(wait_for_sync_point_hits(
            "test.point",
            1,
            Duration::from_secs(1)
        ));
        assert!(done_rx.recv_timeout(Duration::from_millis(50)).is_err());
        let mut released = false;
        for _ in 0..20 {
            unblock_sync_point("test.point");
            if done_rx.recv_timeout(Duration::from_millis(250)).is_ok() {
                released = true;
                break;
            }
        }
        assert!(
            released,
            "sync point remained blocked after repeated unblocks"
        );
        worker.join().unwrap();
        reset_sync_points();
    }
}
