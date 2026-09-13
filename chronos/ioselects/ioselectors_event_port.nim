#
#            Nim's Runtime Library
#        (c) Copyright 2016 Eugene Kabanov
#
#    See the file "copying.txt", included in this
#    distribution, for details about the copyright.
#
# This module implements illumos event ports. Selector operations (including
# signal registration and cleanup) belong to the dispatcher's thread. Only
# SelectEvent handles may be shared with other threads. Signals are reported as
# Event.Signal; dispatcher/future capability guards must separately enable their
# delivery. Native timer, process and vnode registrations are not implemented.

import std/[deques, tables]
import stew/base10

{.push raises: [].}

# Read-side shutdown must wake both callbacks, even when the send buffer is
# full. Unlike POLLHUP, POLLRDHUP is an explicitly requested poll event.
var POLLRDHUP {.importc, header: "<poll.h>".}: cint

type
  PortRegistration = object
    generation: uint
    events: cint
    armed: bool

  RearmEntry = object
    fd: int32
    generation: uint

  SignalMaskOwner = object
    users: int
    wasBlocked: bool

  SelectorImpl[T] = object
    portFd: cint
    fds: Table[int32, SelectorKey[T]]
    registrations: Table[int32, PortRegistration]
    generation: uint
    rearm: Deque[RearmEntry]
    queueEvents: seq[PortEvent]
    sigFd: cint
    signalMask: Sigset
    signals: Table[cint, int32]
    virtualHoles: Deque[int32]
    virtualId: int32

  Selector*[T] = ref SelectorImpl[T]

  SelectEventImpl = object
    rfd: cint
    wfd: cint

  SelectEvent* = ptr SelectEventImpl
  # Shared allocation permits triggering this handle from another thread.

# Preserve pre-existing masks, including when several selectors on the same
# thread watch the same signal. Other threads must block watched signals too
# if process-directed signals are to be consumed exclusively by signalfd.
var signalOwners {.threadvar.}: Table[cint, SignalMaskOwner]

proc changeSignalMask(how: cint, mask: var Sigset,
                      previous: var Sigset): SelectResult[void] =
  when compileOption("threads"):
    let res = pthread_sigmask(how, mask, previous)
    # pthread_sigmask returns an error number, not -1/errno.
    if res != 0:
      return err(OSErrorCode(res))
  else:
    if sigprocmask(how, mask, previous) == -1:
      return err(osLastError())
  ok()

proc retainSignal(sig: cint): SelectResult[void] =
  signalOwners.withValue(sig, owner):
    inc(owner[].users)
    return ok()
  var mask, previous: Sigset
  if sigemptyset(mask) == -1 or sigaddset(mask, sig) == -1:
    return err(osLastError())
  ? changeSignalMask(SIG_BLOCK, mask, previous)
  signalOwners[sig] = SignalMaskOwner(
    users: 1, wasBlocked: sigismember(previous, sig) == 1)
  ok()

proc releaseSignal(sig: cint): SelectResult[void] =
  signalOwners.withValue(sig, owner):
    if owner[].users > 1:
      dec(owner[].users)
      return ok()
    if not owner[].wasBlocked:
      var mask, previous: Sigset
      if sigemptyset(mask) == -1 or sigaddset(mask, sig) == -1:
        return err(osLastError())
      ? changeSignalMask(SIG_UNBLOCK, mask, previous)
  do:
    raiseAssert "Signal is not owned by this thread's selectors"
  signalOwners.del(sig)
  ok()

proc getVirtualId[T](s: Selector[T]): SelectResult[int32] =
  if s.virtualHoles.len > 0:
    ok(s.virtualHoles.popLast())
  elif s.virtualId == low(int32):
    err(EMFILE)
  else:
    dec(s.virtualId)
    ok(s.virtualId)

proc toString(key: int32|cint|SocketHandle|int): string =
  let fd = int32(key)
  if fd == InvalidIdent:
    "InvalidIdent"
  elif fd < 0:
    "V" & Base10.toString(uint32(-int64(fd)))
  else:
    Base10.toString(uint32(fd))

template checkKey[T](s: Selector[T], key: int32): bool =
  s.fds.contains(key)

template getKey[T](s: Selector[T], key: int32): SelectorKey[T] =
  let pkey = s.fds.getOrDefault(key, SelectorKey[T](ident: InvalidIdent))
  doAssert(pkey.ident != InvalidIdent,
           "Descriptor [" & key.toString() & "] is not registered in the selector!")
  pkey

proc freeKey[T](s: Selector[T], key: int32) =
  s.fds.del(key)
  if key < 0:
    s.virtualHoles.addFirst(key)

proc nextGeneration[T](s: Selector[T]): uint =
  # Cookies are integers, never pointers into GC-managed/movable tables.
  # Do not wrap: old events must never match a reused descriptor.
  doAssert(s.generation < high(uint), "Selector generation counter exhausted")
  inc(s.generation)
  s.generation

proc toPortEvents(events: set[Event]): cint =
  if Event.Read in events or Event.User in events:
    result = result or POLLIN
  if Event.Write in events:
    result = result or POLLOUT
  if result != 0:
    result = result or POLLRDHUP

proc associate[T](s: Selector[T], fd: int32,
                  registration: PortRegistration): SelectResult[void] =
  if handleEintr(port_associate(s.portFd, PORT_SOURCE_FD, uint(fd),
                               registration.events,
                               cast[pointer](registration.generation))) == -1:
    return err(osLastError())
  ok()

proc dissociate[T](s: Selector[T], fd: int32): SelectResult[void] =
  if handleEintr(port_dissociate(s.portFd, PORT_SOURCE_FD, uint(fd))) == -1:
    let errorCode = osLastError()
    # Delivery automatically removes an FD association.
    if errorCode != ENOENT:
      return err(errorCode)
  ok()

proc addDescriptor[T](s: Selector[T], fd: int32,
                      events: cint): SelectResult[void] =
  doAssert(not s.registrations.contains(fd),
           "Descriptor [" & fd.toString() & "] is already registered!")
  let registration = PortRegistration(
    generation: s.nextGeneration(), events: events, armed: events != 0)
  if events != 0:
    ? s.associate(fd, registration)
  s.registrations[fd] = registration
  ok()

proc rearmDescriptors[T](s: Selector[T]): SelectResult[void] =
  while s.rearm.len > 0:
    let entry = s.rearm.peekFirst()
    s.registrations.withValue(entry.fd, registration):
      if registration[].generation == entry.generation and
          not registration[].armed and registration[].events != 0:
        # Keep this entry queued on failure, so callers can retry or unregister.
        ? s.associate(entry.fd, registration[])
        registration[].armed = true
    discard s.rearm.popFirst()
  ok()

proc new*(t: typedesc[Selector], T: typedesc): SelectResult[Selector[T]] =
  var mask: Sigset
  if sigemptyset(mask) == -1:
    return err(osLastError())
  let fd = port_create()
  if fd == -1:
    return err(osLastError())
  let flags = setDescriptorInheritance(fd, false)
  if flags.isErr():
    discard closeFd(fd)
    return err(flags.error())
  ok(Selector[T](
    portFd: fd, sigFd: -1, signalMask: mask,
    fds: initTable[int32, SelectorKey[T]](chronosInitialSize),
    registrations: initTable[int32, PortRegistration](chronosInitialSize),
    rearm: initDeque[RearmEntry](),
    queueEvents: newSeq[PortEvent](chronosInitialSize),
    signals: initTable[cint, int32](),
    virtualId: -1, virtualHoles: initDeque[int32]()
  ))

proc close2*[T](s: Selector[T]): SelectResult[void] =
  if s.portFd == -1:
    return err(EBADF)
  var errorCode = OSErrorCode(0)
  if closeFd(s.portFd) == -1:
    errorCode = osLastError()
  s.portFd = -1
  if s.sigFd != -1:
    if closeFd(s.sigFd) == -1 and errorCode == OSErrorCode(0):
      errorCode = osLastError()
    s.sigFd = -1
  for sig in s.signals.keys:
    let res = releaseSignal(sig)
    if res.isErr() and errorCode == OSErrorCode(0):
      errorCode = res.error()
  s.signals.clear()
  s.fds.clear()
  s.registrations.clear()
  s.rearm.clear()
  s.queueEvents.setLen(0)
  s.virtualHoles.clear()
  s.virtualId = -1
  if errorCode != OSErrorCode(0): err(errorCode) else: ok()

proc new*(t: typedesc[SelectEvent]): SelectResult[SelectEvent] =
  var fds: array[2, cint]
  if pipe2(fds, O_NONBLOCK or O_CLOEXEC) == -1:
    return err(osLastError())
  let event = cast[SelectEvent](allocShared0(sizeof(SelectEventImpl)))
  event.rfd = fds[0]
  event.wfd = fds[1]
  ok(event)

proc trigger2*(event: SelectEvent): SelectResult[void] =
  var data = 1'u64
  let res = handleEintr(osdefs.write(event.wfd, addr data, sizeof(data)))
  if res == -1:
    err(osLastError())
  elif res != sizeof(data):
    err(EINVAL)
  else:
    ok()

proc close2*(event: SelectEvent): SelectResult[void] =
  let (rfd, wfd) = (event.rfd, event.wfd)
  deallocShared(cast[pointer](event))
  var errorCode = OSErrorCode(0)
  if closeFd(rfd) == -1:
    errorCode = osLastError()
  if closeFd(wfd) == -1 and errorCode == OSErrorCode(0):
    errorCode = osLastError()
  if errorCode != OSErrorCode(0): err(errorCode) else: ok()

proc registerHandle2*[T](s: Selector[T], fd: cint, events: set[Event],
                         data: T): SelectResult[void] =
  if s.portFd == -1 or fd < 0:
    return err(EBADF)
  doAssert(events <= {Event.Read, Event.Write}, "Unsupported descriptor events")
  doAssert(not s.checkKey(fd),
           "Descriptor [" & fd.toString() & "] is already registered!")
  ? s.addDescriptor(fd, toPortEvents(events))
  s.fds[fd] = SelectorKey[T](ident: fd, events: events, data: data)
  ok()

proc updateHandle2*[T](s: Selector[T], fd: cint,
                       events: set[Event]): SelectResult[void] =
  doAssert(events <= {Event.Read, Event.Write}, "Unsupported descriptor events")
  s.fds.withValue(int32(fd), key):
    doAssert(key[].events <= {Event.Read, Event.Write},
             "Descriptor [" & fd.toString() & "] could not be updated!")
    if key[].events != events:
      let registration = PortRegistration(
        generation: s.nextGeneration(), events: toPortEvents(events),
        armed: events != {})
      if events == {}:
        ? s.dissociate(fd)
      else:
        ? s.associate(fd, registration)
      s.registrations[fd] = registration
      key[].events = events
  do:
    raiseAssert "Descriptor [" & fd.toString() & "] is not registered!"
  ok()

proc registerEvent2*[T](s: Selector[T], event: SelectEvent,
                        data: T): SelectResult[cint] =
  doAssert(not event.isNil)
  ? s.registerHandle2(event.rfd, {Event.Read}, data)
  s.fds.withValue(int32(event.rfd), key):
    key[].events = {Event.User}
  ok(event.rfd)

proc registerSignal*[T](s: Selector[T], signal: int,
                        data: T): SelectResult[cint] =
  if s.portFd == -1:
    return err(EBADF)
  if signal <= 0 or signal > int(high(cint)) or
      signal == int(SIGKILL) or signal == int(SIGSTOP):
    return err(EINVAL)
  let sig = cint(signal)
  doAssert(not s.signals.contains(sig), "Signal is already registered!")
  var mask = s.signalMask
  if sigaddset(mask, sig) == -1:
    return err(osLastError())
  let ident = ? s.getVirtualId()
  let blocked = retainSignal(sig)
  if blocked.isErr():
    s.virtualHoles.addFirst(ident)
    return err(blocked.error())
  let fd = signalfd(s.sigFd, mask, SFD_NONBLOCK or SFD_CLOEXEC)
  if fd == -1:
    let errorCode = osLastError()
    discard releaseSignal(sig)
    s.virtualHoles.addFirst(ident)
    return err(errorCode)
  if s.sigFd == -1:
    let res = s.addDescriptor(fd, POLLIN)
    if res.isErr():
      discard closeFd(fd)
      discard releaseSignal(sig)
      s.virtualHoles.addFirst(ident)
      return err(res.error())
    s.sigFd = fd
  s.signalMask = mask
  s.signals[sig] = ident
  s.fds[ident] = SelectorKey[T](ident: ident, events: {Event.Signal},
                               param: signal, data: data)
  ok(cint(ident))

proc registerTimer*[T](s: Selector[T], timeout: int, oneshot: bool,
                       data: T): SelectResult[cint] =
  # Chronos's dispatcher timers are implemented in userspace instead.
  err(ENOTSUP)

proc registerProcess*[T](s: Selector[T], pid: int,
                         data: T): SelectResult[cint] =
  err(ENOTSUP)

proc registerVnode2*[T](s: Selector[T], fd: cint, events: set[Event],
                        data: T): SelectResult[cint] =
  err(ENOTSUP)

proc unregister2*[T](s: Selector[T], fd: cint): SelectResult[void] =
  let key = s.getKey(int32(fd))
  if Event.Signal in key.events:
    let sig = cint(key.param)
    var mask = s.signalMask
    if sigdelset(mask, sig) == -1:
      return err(osLastError())
    if signalfd(s.sigFd, mask, SFD_NONBLOCK or SFD_CLOEXEC) == -1:
      return err(osLastError())
    let released = releaseSignal(sig)
    if released.isErr():
      discard signalfd(s.sigFd, s.signalMask, SFD_NONBLOCK or SFD_CLOEXEC)
      return err(released.error())
    s.signalMask = mask
    s.signals.del(sig)
    s.freeKey(fd)
    if s.signals.len == 0:
      # Closing also removes any queued port notification. Generation cookies
      # invalidate deferred rearming if the descriptor number is reused.
      let sigFd = s.sigFd
      s.sigFd = -1
      s.registrations.del(sigFd)
      if closeFd(sigFd) == -1:
        return err(osLastError())
  else:
    if key.events != {}:
      ? s.dissociate(fd)
    s.registrations.del(fd)
    s.freeKey(fd)
  ok()

proc unregister2*[T](s: Selector[T], event: SelectEvent): SelectResult[void] =
  s.unregister2(event.rfd)

proc prepareKey[T](s: Selector[T], event: PortEvent,
                   fd: int32): SelectResult[Opt[ReadyKey]] =
  if fd == s.sigFd:
    # Read only one record per port event. Further records remain readable and
    # will be delivered after rearming, even with a one-element output buffer.
    var info: SignalFdInfo
    let res = handleEintr(osdefs.read(fd, addr info, sizeof(info)))
    if res == -1:
      let errorCode = osLastError()
      if errorCode == EAGAIN:
        return ok(Opt.none(ReadyKey))
      return err(errorCode)
    if res != sizeof(info):
      return err(EIO)
    s.signals.withValue(cint(info.ssi_signo), ident):
      return ok(Opt.some(ReadyKey(fd: int(ident[]), events: {Event.Signal})))
    return ok(Opt.none(ReadyKey))

  let key = s.getKey(fd)
  var ready = ReadyKey(fd: fd)
  if (event.portev_events and (POLLERR or POLLHUP or POLLNVAL or POLLRDHUP)) != 0:
    ready.events.incl(Event.Error)
  if (event.portev_events and POLLIN) != 0:
    if Event.User in key.events:
      var data: uint64
      let res = handleEintr(osdefs.read(fd, addr data, sizeof(data)))
      if res == -1 and osLastError() == EAGAIN:
        if ready.events == {}:
          return ok(Opt.none(ReadyKey))
      elif res != sizeof(data):
        ready.events.incl(Event.Error)
      ready.events.incl(Event.User)
    elif Event.Read in key.events:
      ready.events.incl(Event.Read)
  if (event.portev_events and POLLOUT) != 0 and Event.Write in key.events:
    ready.events.incl(Event.Write)
  if ready.events == {}:
    ok(Opt.none(ReadyKey))
  else:
    ok(Opt.some(ready))

proc remainingTimeout(start: Timespec, timeout: int): SelectResult[Timespec] =
  var now: Timespec
  if clock_gettime(CLOCK_MONOTONIC, now) == -1:
    return err(osLastError())
  var
    seconds = int64(timeout div 1000) - (int64(now.tv_sec) - int64(start.tv_sec))
    nanos = int64(timeout mod 1000) * 1_000_000 -
              (int64(now.tv_nsec) - int64(start.tv_nsec))
  if nanos < 0:
    dec(seconds)
    nanos += 1_000_000_000
  elif nanos >= 1_000_000_000:
    inc(seconds)
    nanos -= 1_000_000_000
  if seconds < 0:
    ok(Timespec())
  else:
    ok(Timespec(tv_sec: Time(seconds), tv_nsec: clong(nanos)))

proc selectInto2*[T](s: Selector[T], timeout: int,
                     readyKeys: var openArray[ReadyKey]): SelectResult[int] =
  verifySelectParams(timeout, -1, high(int))
  if s.portFd == -1:
    return err(EBADF)
  if readyKeys.len == 0:
    return ok(0)
  doAssert(uint(readyKeys.len) <= uint(high(cuint)), "Too many output slots")
  var start: Timespec
  if timeout > 0 and clock_gettime(CLOCK_MONOTONIC, start) == -1:
    return err(osLastError())
  ? s.rearmDescriptors()
  if readyKeys.len > s.queueEvents.len:
    s.queueEvents.setLen(readyKeys.len)

  var count: cuint
  while true:
    var tv: Timespec
    if timeout > 0:
      tv = ? remainingTimeout(start, timeout)
    let timeoutPtr = if timeout == -1: nil else: addr tv
    count = 1 # Wait for at least one event, not for the whole output buffer.
    # illumos may leave count unchanged when interrupted before any delivery.
    # A source sentinel distinguishes that case from a genuine partial batch.
    s.queueEvents[0].portev_source = 0
    let res = port_getn(s.portFd, addr s.queueEvents[0], cuint(readyKeys.len),
                        addr count, timeoutPtr)
    if s.queueEvents[0].portev_source == 0:
      count = 0
    if res == -1:
      let errorCode = osLastError()
      if errorCode != EINTR and errorCode != ETIME:
        return err(errorCode)
      # Both timeout and interruption may accompany a partial batch.
      if count == 0 and errorCode == EINTR:
        if timeout == 0 or
            (timeout > 0 and int64(tv.tv_sec) == 0 and tv.tv_nsec == 0):
          return ok(0)
        continue
    break

  # Mark the entire retrieved batch disarmed before decoding it: even if a
  # read fails, every delivered descriptor must be rearmed on the next call.
  for i in 0 ..< int(count):
    let event = s.queueEvents[i]
    if event.portev_source != cushort(PORT_SOURCE_FD) or
        event.portev_object > uint(high(int32)):
      continue
    let fd = int32(event.portev_object)
    s.registrations.withValue(fd, registration):
      if registration[].generation == cast[uint](event.portev_user):
        registration[].armed = false
        s.rearm.addLast(RearmEntry(fd: fd, generation: registration[].generation))

  var n = 0
  for i in 0 ..< int(count):
    let event = s.queueEvents[i]
    if event.portev_source != cushort(PORT_SOURCE_FD) or
        event.portev_object > uint(high(int32)):
      continue
    let fd = int32(event.portev_object)
    s.registrations.withValue(fd, registration):
      if registration[].generation == cast[uint](event.portev_user) and
          registration[].events != 0:
        let ready = ? s.prepareKey(event, fd)
        if ready.isSome():
          readyKeys[n] = ready.get()
          inc(n)
  ok(n)

proc select2*[T](s: Selector[T], timeout: int): SelectResult[seq[ReadyKey]] =
  var ready = newSeq[ReadyKey](chronosEventsCount)
  let count = ? s.selectInto2(timeout, ready)
  ready.setLen(count)
  ok(ready)

proc newSelector*[T](): owned(Selector[T]) {.raises: [IOSelectorsException].} =
  let res = Selector.new(T)
  if res.isErr(): raiseIOSelectorsError(res.error())
  res.get()

proc newSelectEvent*(): SelectEvent {.raises: [IOSelectorsException].} =
  let res = SelectEvent.new()
  if res.isErr(): raiseIOSelectorsError(res.error())
  res.get()

proc trigger*(event: SelectEvent) {.raises: [IOSelectorsException].} =
  let res = event.trigger2()
  if res.isErr(): raiseIOSelectorsError(res.error())

proc close*(event: SelectEvent) {.raises: [IOSelectorsException].} =
  let res = event.close2()
  if res.isErr(): raiseIOSelectorsError(res.error())

proc registerHandle*[T](s: Selector[T], fd: cint|SocketHandle,
                        events: set[Event], data: T) {.
                        raises: [IOSelectorsException].} =
  let res = s.registerHandle2(cint(fd), events, data)
  if res.isErr(): raiseIOSelectorsError(res.error())

proc updateHandle*[T](s: Selector[T], fd: cint|SocketHandle,
                      events: set[Event]) {.raises: [IOSelectorsException].} =
  let res = s.updateHandle2(cint(fd), events)
  if res.isErr(): raiseIOSelectorsError(res.error())

proc registerEvent*[T](s: Selector[T], event: SelectEvent, data: T) {.
                       raises: [IOSelectorsException].} =
  let res = s.registerEvent2(event, data)
  if res.isErr(): raiseIOSelectorsError(res.error())

proc registerVnode*[T](s: Selector[T], fd: cint, events: set[Event], data: T) {.
                       raises: [IOSelectorsException].} =
  let res = s.registerVnode2(fd, events, data)
  if res.isErr(): raiseIOSelectorsError(res.error())

proc unregister*[T](s: Selector[T], event: SelectEvent) {.
                    raises: [IOSelectorsException].} =
  let res = s.unregister2(event)
  if res.isErr(): raiseIOSelectorsError(res.error())

proc unregister*[T](s: Selector[T], fd: cint|SocketHandle) {.
                    raises: [IOSelectorsException].} =
  let res = s.unregister2(cint(fd))
  if res.isErr(): raiseIOSelectorsError(res.error())

proc selectInto*[T](s: Selector[T], timeout: int,
                    readyKeys: var openArray[ReadyKey]): int {.
                    raises: [IOSelectorsException].} =
  let res = s.selectInto2(timeout, readyKeys)
  if res.isErr(): raiseIOSelectorsError(res.error())
  res.get()

proc select*[T](s: Selector[T], timeout: int): seq[ReadyKey] {.
                raises: [IOSelectorsException].} =
  let res = s.select2(timeout)
  if res.isErr(): raiseIOSelectorsError(res.error())
  res.get()

proc close*[T](s: Selector[T]) {.raises: [IOSelectorsException].} =
  let res = s.close2()
  if res.isErr(): raiseIOSelectorsError(res.error())

proc contains*[T](s: Selector[T], fd: SocketHandle|cint): bool {.inline.} =
  s.checkKey(int32(fd))

proc setData*[T](s: Selector[T], fd: SocketHandle|cint, data: T): bool =
  s.fds.withValue(int32(fd), key):
    key[].data = data
    return true
  do:
    return false

template withData*[T](s: Selector[T], fd: SocketHandle|cint, value,
                     body: untyped) =
  s.fds.withValue(int32(fd), key):
    var value = addr(key[].data)
    body

template withData*[T](s: Selector[T], fd: SocketHandle|cint, value, body1,
                     body2: untyped) =
  s.fds.withValue(int32(fd), key):
    var value = addr(key[].data)
    body1
  do:
    body2

proc getFd*[T](s: Selector[T]): cint = s.portFd

{.pop.}
