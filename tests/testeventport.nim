#                Chronos Test Suite
#            (c) Copyright 2026-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

# Standalone selector tests, independent of the dispatcher's capability guards.
# nim c -r tests/testeventport.nim (requires results and stew on Nim's path)
when defined(solaris):
  {.passl: "-lsocket".}
  import std/[unittest, monotimes, times, sets]
  import ../chronos/[selectors2, osdefs, osutils]
  from std/posix import dup2, Sigaction, sigaction, ualarm, Useconds,
                        Timer, Itimerspec, timer_gettime
  from std/os import sleep
  when compileOption("threads"):
    from std/posix import pthread_self, pthread_kill

    proc delayedTrigger(event: SelectEvent) {.thread.} =
      sleep(20)
      doAssert event.trigger2().isOk()

  var interruptions {.volatile.}: cint
  proc interruptHandler(sig: cint) {.noconv, raises: [], stackTrace: off.} =
    inc(interruptions)
    # Bound the test even if a regression restarts the timeout on each EINTR.
    if interruptions >= 50:
      discard ualarm(Useconds(0), Useconds(0))

  proc makePipe(): array[2, cint] =
    doAssert pipe2(result, O_NONBLOCK or O_CLOEXEC) == 0

  proc closePipe(fds: array[2, cint]) =
    discard closeFd(fds[0])
    discard closeFd(fds[1])

  proc put(fd: cint) =
    var data = 'x'
    doAssert osdefs.write(fd, addr data, sizeof(data)) == sizeof(data)

  proc take(fd: cint) =
    var data: char
    doAssert osdefs.read(fd, addr data, sizeof(data)) == sizeof(data)

  proc setMask(how: cint, mask, previous: var Sigset) =
    when compileOption("threads"):
      doAssert pthread_sigmask(how, mask, previous) == 0
    else:
      doAssert sigprocmask(how, mask, previous) == 0

  proc isBlocked(sig: cint): bool =
    var mask, previous: Sigset
    doAssert sigemptyset(mask) == 0
    setMask(SIG_BLOCK, mask, previous)
    sigismember(previous, sig) == 1

  proc sendSignal(sig: cint) =
    when compileOption("threads"):
      doAssert pthread_kill(pthread_self(), sig) == 0
    else:
      doAssert kill(getpid(), sig) == 0

  suite "Event-port selector":
    test "Creation, close-on-exec, empty buffers and ownership":
      let s = Selector.new(int).get()
      let fds = makePipe()
      defer: closePipe(fds)
      check (fcntl(s.getFd(), F_GETFD) and FD_CLOEXEC) != 0
      require s.registerHandle2(fds[0], {}, 12).isOk()
      var empty: array[0, ReadyKey]
      check s.selectInto2(-1, empty).get() == 0
      check s.select2(0).get().len == 0
      check s.close2().isOk()
      check s.getFd() == -1
      check s.close2().error() == EBADF
      check s.select2(0).error() == EBADF
      # Selectors own neither registered pipes nor registered user events.
      put(fds[1])
      take(fds[0])

    test "Persistent read readiness and data access":
      let s = newSelector[int]()
      defer: s.close()
      let fds = makePipe()
      defer: closePipe(fds)
      s.registerHandle(fds[0], {Event.Read}, 12)
      check s.contains(fds[0])
      check s.setData(fds[0], 34)
      s.withData(fds[0], data):
        check data[] == 34
      s.withData(fds[0], data):
        data[] = 56
      do:
        check false
      put(fds[1])
      for i in 0 ..< 3:
        # Same-mask updates must not prevent the deferred rearm.
        s.updateHandle(fds[0], {Event.Read})
        let ready = s.select(1000)
        require ready.len == 1
        check ready[0] == ReadyKey(fd: fds[0], events: {Event.Read})
      take(fds[0])
      check s.select(0).len == 0
      put(fds[1])
      check s.select(1000).len == 1
      s.unregister(fds[0])
      check not s.contains(fds[0])
      check not s.setData(fds[0], 1)
      check s.select(0).len == 0

    test "Disable and reenable after delivery and while queued":
      let s = newSelector[int]()
      defer: s.close()
      let fds = makePipe()
      defer: closePipe(fds)
      s.registerHandle(fds[0], {}, 0)
      put(fds[1])
      check s.select(0).len == 0
      for i in 0 ..< 3:
        s.updateHandle(fds[0], {Event.Read})
        check s.select(1000).len == 1
        s.updateHandle(fds[0], {})
        check s.select(0).len == 0
      s.updateHandle(fds[0], {Event.Read})
      s.updateHandle(fds[0], {})
      check s.select(0).len == 0
      s.unregister(fds[0])

    test "Combined read/write readiness and interest changes":
      let s = newSelector[int]()
      defer: s.close()
      var sockets: array[2, cint]
      require socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0
      defer: closePipe(sockets)
      s.registerHandle(SocketHandle(sockets[0]), {Event.Read, Event.Write}, 0)
      check s.select(1000)[0].events == {Event.Write}
      put(sockets[1])
      let ready = s.select(1000)
      require ready.len == 1
      check ready[0].events == {Event.Read, Event.Write}
      s.updateHandle(SocketHandle(sockets[0]), {Event.Read})
      check s.select(1000)[0].events == {Event.Read}
      s.updateHandle(sockets[0], {Event.Write})
      check s.select(1000)[0].events == {Event.Write}
      s.unregister(SocketHandle(sockets[0]))

    test "Descriptor reuse cannot resurrect a deferred registration":
      let s = newSelector[int]()
      defer: s.close()
      let oldPipe = makePipe()
      let newPipe = makePipe()
      defer: closePipe(newPipe)
      s.registerHandle(oldPipe[0], {Event.Read}, 1)
      put(oldPipe[1])
      require s.select(1000).len == 1
      s.unregister(oldPipe[0])
      closePipe(oldPipe)
      require dup2(newPipe[0], oldPipe[0]) == oldPipe[0]
      defer: discard closeFd(oldPipe[0])
      s.registerHandle(oldPipe[0], {}, 2)
      put(newPipe[1])
      check s.select(0).len == 0
      s.updateHandle(oldPipe[0], {Event.Read})
      require s.select(1000).len == 1
      s.withData(oldPipe[0], data):
        check data[] == 2
      s.unregister(oldPipe[0])

    test "Registration failure rolls back the descriptor table":
      let s = newSelector[int]()
      defer: s.close()
      let fds = makePipe()
      closePipe(fds)
      check s.registerHandle2(fds[0], {Event.Read}, 0).isErr()
      check not s.contains(fds[0])
      check s.registerHandle2(-1, {}, 0).error() == EBADF
      check s.select(0).len == 0

    test "Hangup reports an error":
      let s = newSelector[int]()
      defer: s.close()
      let fds = makePipe()
      defer: discard closeFd(fds[0])
      s.registerHandle(fds[0], {Event.Read}, 0)
      require closeFd(fds[1]) == 0
      let ready = s.select(1000)
      require ready.len == 1
      check Event.Error in ready[0].events
      s.unregister(fds[0])

    test "Read-side shutdown reports an error even with a full send buffer":
      let s = newSelector[int]()
      defer: s.close()
      var sockets: array[2, cint]
      require socketpair(AF_UNIX, SOCK_STREAM or SOCK_NONBLOCK, 0, sockets) == 0
      defer: closePipe(sockets)
      var buffer: array[65536, byte]
      while osdefs.write(sockets[0], addr buffer[0], buffer.len) > 0:
        discard
      require osLastError() == EAGAIN
      require shutdown(SocketHandle(sockets[1]), SHUT_WR) == 0
      s.registerHandle(sockets[0], {Event.Read, Event.Write}, 0)
      var events: set[Event]
      for i in 0 ..< 20:
        let ready = s.select(1000)
        require ready.len == 1
        events = events + ready[0].events
        if Event.Error in events:
          break
        # A read notification can be queued before the shutdown notification.
        sleep(1)
      check Event.Read in events
      check Event.Error in events
      s.unregister(sockets[0])

    test "Batches larger than initial capacity and small output buffers":
      let s = newSelector[int]()
      defer: s.close()
      var pipes: seq[array[2, cint]]
      defer:
        for fds in pipes:
          closePipe(fds)
      for i in 0 ..< 80:
        let fds = makePipe()
        pipes.add(fds)
        s.registerHandle(fds[0], {Event.Read}, i)
        put(fds[1])
      var
        seen: HashSet[int]
        ready: array[7, ReadyKey]
      while seen.len < pipes.len:
        let count = s.selectInto(1000, ready)
        require count > 0
        for i in 0 ..< count:
          check ready[i].fd notin seen
          seen.incl(ready[i].fd)
          s.unregister(cint(ready[i].fd))
      check seen.len == pipes.len
      check s.select(0).len == 0
      for fds in pipes:
        s.registerHandle(fds[0], {Event.Read}, 0)
      var large: array[100, ReadyKey]
      check s.selectInto(1000, large) == pipes.len

    test "User events preserve repeated triggers and survive unregister":
      let s = newSelector[int]()
      defer: s.close()
      let event = newSelectEvent()
      defer: event.close()
      let fd = s.registerEvent2(event, 42).get()
      check s.contains(fd)
      event.trigger()
      event.trigger()
      for i in 0 ..< 2:
        let ready = s.select(1000)
        require ready.len == 1
        check ready[0] == ReadyKey(fd: fd, events: {Event.User})
      check s.select(0).len == 0
      s.unregister(event)
      event.trigger()
      check s.select(0).len == 0
      s.registerEvent(event, 43)
      require s.select(1000).len == 1
      s.unregister(event)

    test "Finite timeout without any registered descriptors":
      let s = newSelector[int]()
      defer: s.close()
      let start = getMonoTime()
      check s.select(30).len == 0
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed >= 20
      check elapsed < 1000

    test "EINTR does not restart a finite timeout or produce phantom events":
      let s = newSelector[int]()
      defer: s.close()
      var
        action, oldAction: Sigaction
        mask, previous, ignored: Sigset
      require sigemptyset(action.sa_mask) == 0
      action.sa_handler = interruptHandler
      require sigaction(SIGALRM, action, oldAction) == 0
      defer: discard sigaction(SIGALRM, oldAction, action)
      require sigemptyset(mask) == 0
      require sigaddset(mask, SIGALRM) == 0
      setMask(SIG_UNBLOCK, mask, previous)
      defer: setMask(SIG_SETMASK, previous, ignored)
      interruptions = 0
      discard ualarm(Useconds(5000), Useconds(5000))
      defer: discard ualarm(Useconds(0), Useconds(0))
      let start = getMonoTime()
      let ready = s.select(40)
      let elapsed = (getMonoTime() - start).inMilliseconds
      discard ualarm(Useconds(0), Useconds(0))
      check ready.len == 0
      check interruptions > 0
      check elapsed >= 30
      check elapsed < 180

    when compileOption("threads"):
      test "User event wakes an infinite wait from another thread":
        let s = newSelector[int]()
        defer: s.close()
        let event = newSelectEvent()
        defer: event.close()
        s.registerEvent(event, 0)
        for i in 0 ..< 3:
          var thread: Thread[SelectEvent]
          createThread(thread, delayedTrigger, event)
          let ready = s.select(-1)
          joinThread(thread)
          require ready.len == 1
          check ready[0].events == {Event.User}
        s.unregister(event)

    test "One-shot timer reports completion only once":
      let s = newSelector[int]()
      defer: s.close()
      let start = getMonoTime()
      let timer = s.registerTimer2(25, true, 12).get()
      check timer < -1
      check s.contains(timer)
      check s.setData(timer, 34)
      let ready = s.select(1000)
      require ready.len == 1
      check ready[0] == ReadyKey(fd: timer,
                                events: {Event.Timer, Event.Oneshot, Event.Finished})
      check (getMonoTime() - start).inMilliseconds >= 20
      s.withData(timer, data):
        check data[] == 34
      check s.select(40).len == 0
      s.unregister(timer)
      check not s.contains(timer)

    test "Subsecond periodic timer requires no FD rearming":
      let s = newSelector[int]()
      defer: s.close()
      let timer = s.registerTimer(5, false, 0).get()
      for i in 0 ..< 5:
        let ready = s.select(1000)
        require ready.len == 1
        check ready[0] == ReadyKey(fd: timer, events: {Event.Timer})
      # Several expirations may coalesce; their count is not a poll event mask.
      sleep(50)
      let ready = s.select(1000)
      require ready.len == 1
      check ready[0] == ReadyKey(fd: timer, events: {Event.Timer})
      s.unregister(timer)
      check s.select(20).len == 0

    test "Periodic intervals retain both seconds and fractional milliseconds":
      let s = newSelector[int]()
      defer: s.close()
      let timer = s.registerTimer(1005, false, 0).get()
      var
        event: PortEvent
        count = 1.cuint
        timeout = Timespec(tv_sec: osdefs.Time(2), tv_nsec: 0)
        remaining: Itimerspec
      require port_getn(s.getFd(), addr event, 1, addr count, addr timeout) == 0
      require count == 1
      require timer_gettime(Timer(event.portev_object), remaining) == 0
      check int64(remaining.it_interval.tv_sec) == 1
      check remaining.it_interval.tv_nsec == 5_000_000
      s.unregister(timer)

    test "Timer cancellation before expiry and after expiry is queued":
      let s = newSelector[int]()
      defer: s.close()
      for queued in [false, true]:
        let timer = s.registerTimer(if queued: 1 else: 5000, true, 0).get()
        if queued:
          sleep(20)
        s.unregister(timer)
        # Reusing a virtual ID must not deliver an obsolete timer event.
        let replacement = s.registerTimer(5000, true, 1).get()
        check replacement == timer
        check s.select(20).len == 0
        s.unregister(replacement)

    test "Timers and descriptors share bounded result buffers":
      let s = newSelector[int]()
      defer: s.close()
      let fds = makePipe()
      defer: closePipe(fds)
      var expected = initHashSet[int]()
      for i in 0 ..< 8:
        expected.incl(int(s.registerTimer(2, true, i).get()))
      s.registerHandle(fds[0], {Event.Read}, 0)
      expected.incl(int(fds[0]))
      put(fds[1])
      sleep(20)
      var ready: array[2, ReadyKey]
      while expected.len > 0:
        let count = s.selectInto(1000, ready)
        require count > 0
        for i in 0 ..< count:
          check ready[i].fd in expected
          expected.excl(ready[i].fd)
          if ready[i].fd == int(fds[0]):
            check ready[i].events == {Event.Read}
          else:
            check ready[i].events == {Event.Timer, Event.Oneshot, Event.Finished}
          s.unregister(cint(ready[i].fd))
      check s.select(0).len == 0

    test "Unregister and close delete native timers, including expired ones":
      for oneshot in [false, true]:
        for closeSelector in [false, true]:
          let s = newSelector[int]()
          let timer = s.registerTimer(2, oneshot, 0).get()
          # Retrieve the native event directly to inspect the OS timer's ID.
          var
            event: PortEvent
            count = 1.cuint
            timeout = Timespec(tv_sec: osdefs.Time(1), tv_nsec: 0)
            remaining: Itimerspec
          require port_getn(s.getFd(), addr event, 1, addr count, addr timeout) == 0
          require count == 1
          let nativeId = Timer(event.portev_object)
          check timer_gettime(nativeId, remaining) == 0
          if closeSelector:
            s.close()
          else:
            s.unregister(timer)
          check timer_gettime(nativeId, remaining) == -1
          check osLastError() == EINVAL
          if not closeSelector:
            s.close()

    test "Invalid timer intervals and registration failure leave no key":
      let s = newSelector[int]()
      check s.registerTimer(0, true, 0).error() == EINVAL
      check s.registerTimer(-1, false, 0).error() == EINVAL
      check not s.contains(-2.cint)
      require closeFd(s.getFd()) == 0 # Inject a native timer_create failure.
      check s.registerTimer(1, true, 0).isErr()
      check not s.contains(-2.cint)
      check s.close2().error() == EBADF
      check s.registerTimer(1, true, 0).error() == EBADF

    test "Deferred features return ENOTSUP":
      let s = newSelector[int]()
      defer: s.close()
      check s.registerProcess(1, 0).error() == ENOTSUP
      check s.registerVnode2(0, {}, 0).error() == ENOTSUP

    test "Signals share a signalfd and work with a one-element buffer":
      let s = newSelector[int]()
      defer: s.close()
      let
        wasBlocked1 = isBlocked(SIGUSR1)
        wasBlocked2 = isBlocked(SIGUSR2)
        first = s.registerSignal(int(SIGUSR1), 1).get()
        second = s.registerSignal(int(SIGUSR2), 2).get()
      check first < -1 and second < -1 and first != second
      check isBlocked(SIGUSR1) and isBlocked(SIGUSR2)
      sendSignal(SIGUSR1)
      sendSignal(SIGUSR2)
      var
        ready: array[1, ReadyKey]
        seen: HashSet[int]
      for i in 0 ..< 2:
        require s.selectInto(1000, ready) == 1
        check ready[0].events == {Event.Signal}
        seen.incl(ready[0].fd)
      check seen == toHashSet([int(first), int(second)])
      s.unregister(first)
      check isBlocked(SIGUSR1) == wasBlocked1
      for i in 0 ..< 3:
        sendSignal(SIGUSR2)
        require s.selectInto(1000, ready) == 1
        check ready[0] == ReadyKey(fd: second, events: {Event.Signal})
      s.unregister(second)
      check isBlocked(SIGUSR2) == wasBlocked2
      check s.select(0).len == 0
      # Recreate the signalfd while an obsolete rearm entry is still possible.
      let again = s.registerSignal(int(SIGUSR1), 3).get()
      sendSignal(SIGUSR1)
      require s.selectInto(1000, ready) == 1
      check ready[0].fd == again
      s.unregister(again)
      check s.registerSignal(int(SIGKILL), 0).error() == EINVAL
      check s.registerSignal(0, 0).error() == EINVAL

    test "Signal mask ownership across selectors and close":
      var mask, previous, ignored: Sigset
      require sigemptyset(mask) == 0
      require sigaddset(mask, SIGUSR1) == 0
      setMask(SIG_UNBLOCK, mask, previous)
      defer: setMask(SIG_SETMASK, previous, ignored)
      let a = newSelector[int]()
      let b = newSelector[int]()
      discard a.registerSignal(int(SIGUSR1), 0).get()
      discard b.registerSignal(int(SIGUSR1), 0).get()
      a.close()
      check isBlocked(SIGUSR1)
      b.close()
      check not isBlocked(SIGUSR1)
      setMask(SIG_BLOCK, mask, ignored)
      let c = newSelector[int]()
      discard c.registerSignal(int(SIGUSR1), 0).get()
      c.close()
      check isBlocked(SIGUSR1)
