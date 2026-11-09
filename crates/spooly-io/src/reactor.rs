//! The reactor: a `mio::Poll` behind Spooly's `IoDriver` hook
//! (ASYNC_IO_PLAN.md IO1, §5.3).
//!
//! Each registered socket has a [`ScheduledIo`]: its cached readiness and,
//! per direction, the waker of the task waiting on it. `mio` is
//! edge-triggered, so readiness is cached until an operation returns
//! `WouldBlock`, and only then cleared. An operation tries its system call
//! while the readiness bit is set, and registers its waker only once it
//! would block. Everything here runs on the runtime's thread, so no event
//! can be delivered between a `WouldBlock` and the clear that follows it.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use mio::event::{Event, Source};
use mio::{Events, Interest, Registry, Token};
use spooly::driver::IoDriver;

/// The token of the `mio::Waker` that interrupts a `wait`.
const WAKER: Token = Token(usize::MAX);

/// The most events one I/O phase delivers; the rest wait for the next step.
const EVENTS_CAPACITY: usize = 1024;

const NO_DRIVER: &str = "spooly-io: this runtime has no I/O driver; run it with \
     `spooly_io::run` or `spooly_io::block_on`, or build it with \
     `spooly::Runtime::builder().io_driver(spooly_io::Driver::new()?)`";

/// Spooly's I/O driver: a reactor on the OS's readiness API (epoll, kqueue,
/// or Windows' AFD), through [`mio`](https://docs.rs/mio).
///
/// [`run`](crate::run) and [`block_on`](crate::block_on) install one for
/// you. To combine it with other runtime options (a
/// [`MockClock`](spooly::MockClock), say), install it yourself:
///
/// ```
/// let rt = spooly::Runtime::builder()
///     .io_driver(spooly_io::Driver::new()?)
///     .build();
/// let listener = rt.block_on(spooly_io::net::TcpListener::bind("127.0.0.1:0"))?;
/// assert!(listener.local_addr()?.port() != 0);
/// # Ok::<(), std::io::Error>(())
/// ```
///
/// The socket types of [`net`](crate::net) register with the current
/// runtime's driver when they are created, so they must be created inside a
/// runtime that has one, and used on that runtime only.
pub struct Driver {
    poll: RefCell<mio::Poll>,
    registry: Registry,
    events: RefCell<Events>,
    /// `wait` saw I/O that `poll_ready` hasn't delivered yet.
    pending: Cell<bool>,
    /// Every registered socket, by token. Tokens are never reused, so a
    /// stale event for a closed socket finds nothing.
    sources: RefCell<HashMap<usize, Rc<ScheduledIo>>>,
    next_token: Cell<usize>,
    waker: Arc<mio::Waker>,
}

impl Driver {
    /// Creates a driver: a new OS polling instance.
    ///
    /// # Errors
    ///
    /// If the OS can't create one (for example, the process is out of file
    /// descriptors).
    pub fn new() -> io::Result<Driver> {
        let poll = mio::Poll::new()?;
        let registry = poll.registry().try_clone()?;
        let waker = Arc::new(mio::Waker::new(&registry, WAKER)?);
        Ok(Driver {
            poll: RefCell::new(poll),
            registry,
            events: RefCell::new(Events::with_capacity(EVENTS_CAPACITY)),
            pending: Cell::new(false),
            sources: RefCell::new(HashMap::new()),
            next_token: Cell::new(0),
            waker,
        })
    }

    /// The current runtime's driver.
    ///
    /// # Panics
    ///
    /// Outside a runtime, or if the runtime has no `Driver`.
    pub(crate) fn current() -> Rc<Driver> {
        spooly::driver::current_io_driver::<Driver>().expect(NO_DRIVER)
    }

    fn register(
        self: &Rc<Self>,
        source: &mut impl Source,
        interest: Interest,
    ) -> io::Result<Registration> {
        let token = self.next_token.get();
        self.next_token.set(token + 1);
        self.registry.register(source, Token(token), interest)?;
        let io = Rc::new(ScheduledIo::default());
        self.sources.borrow_mut().insert(token, Rc::clone(&io));
        Ok(Registration {
            driver: Rc::clone(self),
            token,
            io,
        })
    }

    /// Polls the OS. Returns `true` if any socket became ready.
    fn poll_os(&self, timeout: Option<Duration>) -> bool {
        let mut events = self.events.borrow_mut();
        match self.poll.borrow_mut().poll(&mut events, timeout) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => events.clear(),
            Err(e) => panic!("spooly-io: polling for I/O failed: {e}"),
        }
        events.iter().any(|event| event.token() != WAKER)
    }

    /// Records the readiness in `events`, and wakes the tasks waiting for
    /// it. Returns `true` if it woke any.
    fn dispatch(&self) -> bool {
        let mut wakers = Vec::new();
        {
            let events = self.events.borrow();
            let sources = self.sources.borrow();
            for event in events.iter() {
                if let Some(io) = sources.get(&event.token().0) {
                    io.set_ready(event, &mut wakers);
                }
            }
        }
        // Wake outside the borrows (each wake queues a microtask).
        let woke = !wakers.is_empty();
        wakers.into_iter().for_each(Waker::wake);
        woke
    }
}

impl IoDriver for Driver {
    fn poll_ready(&self) -> bool {
        if !self.pending.replace(false) {
            // Nothing recorded by `wait`: poll without blocking, unless no
            // socket is registered (then skip the system call).
            if self.sources.borrow().is_empty() || !self.poll_os(Some(Duration::ZERO)) {
                return false;
            }
        }
        self.dispatch()
    }

    fn wait(&self, timeout: Option<Duration>) -> bool {
        if self.pending.get() {
            return true; // Don't overwrite events not yet delivered.
        }
        let ready = self.poll_os(timeout);
        self.pending.set(ready);
        ready
    }

    fn unparker(&self) -> Arc<dyn Fn() + Send + Sync> {
        let waker = Arc::clone(&self.waker);
        Arc::new(move || {
            // Fails only if the OS does; the wait then just ends at its
            // timeout, as with a lost thread unpark.
            let _ = waker.wake();
        })
    }

    fn is_active(&self) -> bool {
        self.sources.borrow().values().any(|io| io.is_waited_on())
    }
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Driver")
            .field("sources", &self.sources.borrow().len())
            .finish_non_exhaustive()
    }
}

/// Which half of a socket an operation uses. Each has its own readiness
/// bit and waker slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Read,
    Write,
}

impl Direction {
    fn bit(self) -> u8 {
        match self {
            Direction::Read => READABLE,
            Direction::Write => WRITABLE,
        }
    }
}

const READABLE: u8 = 0b01;
const WRITABLE: u8 = 0b10;
/// The OS reported an error on the socket (on Windows, a failed connect).
/// Sticky: nothing clears it.
const ERROR: u8 = 0b100;

/// The reactor's state for one socket.
struct ScheduledIo {
    /// Cached readiness bits. A new socket starts out ready in both
    /// directions, so its first operation just tries the system call.
    readiness: Cell<u8>,
    /// The task waiting to read, and the one waiting to write. One each: a
    /// second task waiting on the same direction replaces the first.
    reader: RefCell<Option<Waker>>,
    writer: RefCell<Option<Waker>>,
    /// Waits on this socket don't keep `run` alive.
    unref: Cell<bool>,
}

impl Default for ScheduledIo {
    fn default() -> Self {
        ScheduledIo {
            readiness: Cell::new(READABLE | WRITABLE),
            reader: RefCell::new(None),
            writer: RefCell::new(None),
            unref: Cell::new(false),
        }
    }
}

impl ScheduledIo {
    fn slot(&self, dir: Direction) -> &RefCell<Option<Waker>> {
        match dir {
            Direction::Read => &self.reader,
            Direction::Write => &self.writer,
        }
    }

    fn set_ready(&self, event: &Event, wakers: &mut Vec<Waker>) {
        let mut ready = 0;
        // Closed and errored sockets are "ready": the next operation
        // returns the end of the stream or the error.
        if event.is_readable() || event.is_read_closed() || event.is_error() {
            ready |= READABLE;
        }
        if event.is_writable() || event.is_write_closed() || event.is_error() {
            ready |= WRITABLE;
        }
        if event.is_error() {
            ready |= ERROR;
        }
        self.readiness.set(self.readiness.get() | ready);
        for dir in [Direction::Read, Direction::Write] {
            if ready & dir.bit() != 0 {
                wakers.extend(self.slot(dir).take());
            }
        }
    }

    fn is_ready(&self, dir: Direction) -> bool {
        self.readiness.get() & dir.bit() != 0
    }

    fn clear_ready(&self, dir: Direction) {
        self.readiness.set(self.readiness.get() & !dir.bit());
    }

    fn set_waker(&self, dir: Direction, waker: &Waker) {
        let mut slot = self.slot(dir).borrow_mut();
        match &mut *slot {
            Some(old) if old.will_wake(waker) => {}
            _ => *slot = Some(waker.clone()),
        }
    }

    fn clear_waker(&self, dir: Direction) {
        self.slot(dir).take();
    }

    fn is_waited_on(&self) -> bool {
        !self.unref.get() && (self.reader.borrow().is_some() || self.writer.borrow().is_some())
    }
}

/// A socket's registration with the driver. Dropping it forgets the socket
/// (the owner deregisters the socket itself first; see [`PollEvented`]).
struct Registration {
    driver: Rc<Driver>,
    token: usize,
    io: Rc<ScheduledIo>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.driver.sources.borrow_mut().remove(&self.token);
    }
}

/// A `mio` socket registered with the current runtime's driver: the base of
/// every socket type in [`net`](crate::net).
pub(crate) struct PollEvented<S: Source> {
    /// Always `Some`, except after `into_inner`.
    source: Option<S>,
    registration: Registration,
}

impl<S: Source> PollEvented<S> {
    /// Registers `source` with the current runtime's driver.
    ///
    /// # Panics
    ///
    /// Outside a runtime, or if the runtime has no `Driver`.
    pub(crate) fn new(mut source: S, interest: Interest) -> io::Result<Self> {
        let registration = Driver::current().register(&mut source, interest)?;
        Ok(PollEvented {
            source: Some(source),
            registration,
        })
    }

    pub(crate) fn get_ref(&self) -> &S {
        self.source.as_ref().expect("registered source")
    }

    /// Deregisters the source and returns it.
    pub(crate) fn into_inner(mut self) -> S {
        let mut source = self.source.take().expect("registered source");
        let _ = self.registration.driver.registry.deregister(&mut source);
        source
    }

    /// Runs `op` until it doesn't return `WouldBlock`, while `dir` is
    /// ready; registers the task's waker and returns `Pending` otherwise.
    pub(crate) fn poll_io<R>(
        &self,
        cx: &mut Context<'_>,
        dir: Direction,
        mut op: impl FnMut(&S) -> io::Result<R>,
    ) -> Poll<io::Result<R>> {
        let io = &self.registration.io;
        loop {
            if !io.is_ready(dir) {
                io.set_waker(dir, cx.waker());
                return Poll::Pending;
            }
            match op(self.get_ref()) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => io.clear_ready(dir),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                result => {
                    io.clear_waker(dir);
                    return Poll::Ready(result);
                }
            }
        }
    }

    /// Ready once `dir` is (or may be) ready. Doesn't clear readiness.
    pub(crate) fn poll_ready(&self, cx: &mut Context<'_>, dir: Direction) -> Poll<()> {
        let io = &self.registration.io;
        if io.is_ready(dir) {
            io.clear_waker(dir);
            Poll::Ready(())
        } else {
            io.set_waker(dir, cx.waker());
            Poll::Pending
        }
    }

    /// Runs `op` once; clears `dir`'s readiness if it would block.
    pub(crate) fn try_io<R>(
        &self,
        dir: Direction,
        op: impl FnOnce(&S) -> io::Result<R>,
    ) -> io::Result<R> {
        let result = op(self.get_ref());
        if matches!(&result, Err(e) if e.kind() == io::ErrorKind::WouldBlock) {
            self.registration.io.clear_ready(dir);
        }
        result
    }

    /// Marks `dir` not ready, after an operation found it wasn't (when the
    /// caller learned that some other way than `WouldBlock`).
    pub(crate) fn clear_ready(&self, dir: Direction) {
        self.registration.io.clear_ready(dir);
    }

    /// Whether the OS has reported an error on the socket.
    pub(crate) fn saw_error(&self) -> bool {
        self.registration.io.readiness.get() & ERROR != 0
    }

    pub(crate) fn unref(&self) {
        self.registration.io.unref.set(true);
    }
}

/// Byte-stream operations, for sockets whose `&S` implements `Read` and
/// `Write` (TCP and Unix streams).
impl<S: Source> PollEvented<S>
where
    for<'a> &'a S: io::Read + io::Write,
{
    pub(crate) fn poll_read(
        &self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_io(cx, Direction::Read, |mut s| io::Read::read(&mut s, buf))
    }

    pub(crate) fn poll_read_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_io(cx, Direction::Read, |mut s| {
            io::Read::read_vectored(&mut s, bufs)
        })
    }

    pub(crate) fn poll_write(&self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        self.poll_io(cx, Direction::Write, |mut s| io::Write::write(&mut s, buf))
    }

    pub(crate) fn poll_write_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_io(cx, Direction::Write, |mut s| {
            io::Write::write_vectored(&mut s, bufs)
        })
    }
}

impl<S: Source> Drop for PollEvented<S> {
    fn drop(&mut self) {
        if let Some(mut source) = self.source.take() {
            let _ = self.registration.driver.registry.deregister(&mut source);
        }
    }
}

impl<S: Source + fmt::Debug> fmt::Debug for PollEvented<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}
