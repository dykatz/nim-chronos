#                Chronos Test Suite
#            (c) Copyright 2026-Present
#         Status Research & Development GmbH
#
#              Licensed under either of
#  Apache License, version 2.0, (LICENSE-APACHEv2)
#              MIT license (LICENSE-MIT)

# Standalone smoke test: nim c -r tests/testillumosbindings.nim
# Use only stdlib and osdefs so the bindings can be validated without the
# dispatcher or Chronos's external test dependencies.
when defined(solaris):
  import std/unittest
  import ../chronos/osdefs

  when compileOption("threads"):
    from std/posix import pthread_self, pthread_kill

  proc setSignalMask(how: cint, mask, previous: var Sigset): cint =
    when compileOption("threads"):
      pthread_sigmask(how, mask, previous)
    else:
      sigprocmask(how, mask, previous)

  proc sendSignal(sig: cint): cint =
    when compileOption("threads"):
      # Target this thread, whose mask we control, rather than another thread
      # that may have been started by the surrounding test suite.
      pthread_kill(pthread_self(), sig)
    else:
      kill(getpid(), sig)

  suite "illumos OS bindings":
    test "Event and signal record sizes":
      check:
        sizeof(PortEvent) == (if sizeof(pointer) == 8: 24 else: 16)
        sizeof(SignalFdInfo) == 128

    test "Descriptor association, delivery, rearming and dissociation":
      let port = port_create()
      require port >= 0
      defer: discard close(port)

      var fds: array[2, cint]
      require pipe(fds) == 0
      defer:
        discard close(fds[0])
        discard close(fds[1])

      var cookie = 42
      require port_associate(port, PORT_SOURCE_FD, uint(fds[0]),
                             POLLIN, nil) == 0
      # Associating an existing descriptor updates its mask and cookie.
      require port_associate(port, PORT_SOURCE_FD, uint(fds[0]),
                             POLLIN, addr cookie) == 0
      var data = 'x'
      require write(fds[1], addr data, sizeof(data)) == sizeof(data)

      var events: array[2, PortEvent]
      for i in 0 ..< 2:
        var
          count = 1.cuint
          timeout = Timespec(tv_sec: Time(1), tv_nsec: 0)
        require port_getn(port, addr events[0], cuint(events.len),
                          addr count, addr timeout) == 0
        require count == 1
        check:
          events[0].portev_source == cushort(PORT_SOURCE_FD)
          events[0].portev_object == uint(fds[0])
          events[0].portev_user == addr cookie
          (events[0].portev_events and POLLIN) != 0

        # Retrieval removes the association, even though the pipe still has
        # unread data. Explicitly rearm it for another notification.
        check port_dissociate(port, PORT_SOURCE_FD, uint(fds[0])) == -1
        check osLastError() == ENOENT
        require port_associate(port, PORT_SOURCE_FD, uint(fds[0]),
                               POLLIN, addr cookie) == 0

      require port_dissociate(port, PORT_SOURCE_FD, uint(fds[0])) == 0
      var
        count = 1.cuint
        timeout: Timespec
      # A zero timeout can return success with zero events on illumos.
      let res = port_getn(port, addr events[0], cuint(events.len),
                          addr count, addr timeout)
      let errorCode = osLastError()
      check:
        res == 0 or (res == -1 and errorCode == ETIME)
        count == 0

      count = 1
      timeout.tv_nsec = 1_000_000
      check port_getn(port, addr events[0], cuint(events.len),
                      addr count, addr timeout) == -1
      check:
        osLastError() == ETIME
        count == 0

    test "signalfd mask updates and event-port delivery":
      var mask, previous, ignored: Sigset
      require sigemptyset(mask) == 0
      require sigaddset(mask, SIGUSR1) == 0
      require sigaddset(mask, SIGUSR2) == 0
      require setSignalMask(SIG_BLOCK, mask, previous) == 0
      defer: discard setSignalMask(SIG_SETMASK, previous, ignored)

      let fd = signalfd(-1, mask, SFD_NONBLOCK or SFD_CLOEXEC)
      require fd >= 0
      defer:
        # Consume any pending test signals before restoring the signal mask,
        # including when an assertion above failed.
        var info: SignalFdInfo
        while read(fd, addr info, sizeof(info)) > 0:
          discard
        discard close(fd)

      check:
        (fcntl(fd, F_GETFD) and FD_CLOEXEC) != 0
        (fcntl(fd, F_GETFL) and O_NONBLOCK) != 0

      let port = port_create()
      require port >= 0
      defer: discard close(port)

      var info: SignalFdInfo
      check read(fd, addr info, sizeof(info)) == -1
      check osLastError() == EAGAIN

      for sig in [SIGUSR1, SIGUSR2]:
        require sigemptyset(mask) == 0
        require sigaddset(mask, SIGUSR1) == 0
        require sigaddset(mask, SIGUSR2) == 0
        let other = if sig == SIGUSR1: SIGUSR2 else: SIGUSR1
        require sigdelset(mask, other) == 0
        check sigismember(mask, other) == 0
        require signalfd(fd, mask, SFD_NONBLOCK or SFD_CLOEXEC) == fd
        require port_associate(port, PORT_SOURCE_FD, uint(fd),
                               POLLIN, nil) == 0
        require sendSignal(sig) == 0

        var
          event: PortEvent
          count = 1.cuint
          timeout = Timespec(tv_sec: Time(1), tv_nsec: 0)
        require port_getn(port, addr event, 1, addr count, addr timeout) == 0
        require count == 1
        check:
          event.portev_source == cushort(PORT_SOURCE_FD)
          event.portev_object == uint(fd)
          (event.portev_events and POLLIN) != 0
        require read(fd, addr info, sizeof(info)) == sizeof(info)
        check info.ssi_signo == uint32(sig)
        check read(fd, addr info, sizeof(info)) == -1
        check osLastError() == EAGAIN
