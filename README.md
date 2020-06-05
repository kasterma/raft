
    _   ________       __ _
    | | / /| ___ \     / _| |
    | |/ / | |_/ /__ _| |_| |_
    |    \ |    // _` |  _| __|
    | |\  \| |\ \ (_| | | | |_
    \_| \_/\_| \_\__,_|_|  \__|

            what is it?
                a shitty cheese product company ....
                      nope, Kastermans' Raft implementation

Goals for the week:

- learn the details of raft
  check
- implement a well tested version in python
  it is well testable, was not quite the time to get enough test cases
  in to say it really is well tested
- learn some more python while doing this
  not a lot, but some
- doing lots of interesting testing around this
  check; but many more to do


Things that worked well:

- having the states with step function which the server loops.  This
  made it possible to do single stepping in testing.

- all communication through two Queue's.  This made it possible to put
  in, and read out messages in testing.  I.e. can avoid the background
  threads that handle the sockets in many tests.

- the ability to turn on and off different bits of logging.  Allowed
  me to have a contant tail on a log file, and seeing the bits I was
  interested in pass by.

- pytest and elpy combination; write a test that fails.  Go to the
  code that should make it pass and run recompile to only rerun that
  test.


Things that could have gone better:

- I am currently happy with the state machine design, but getting to
  it was more a random walk than a careful process.  I think that
  means it could have gone terrible as well.

- my emacs-fu is still lacking (same would be true for other
  environments), but there were a whole bunch of occasions where the
  flow broke b/c doing something was too hard.

- naming convention (camel_case, or UnderscoreSeparated and _private
  functions of _testinfrastructure functions) really messy.

- __getattr__ on State really made the code more readable (for now
  when it is fresh in mind), but it probably indicates a design defect
  that it is needed.

- start using the TLA+ spec earlier.  Was very helpful in
  understanding.

- code is messy mostly b/c of time pressure.  But with better
  organisation probably could have gone faster.


TODO:

- there are bugs to fix; one, with out of order AE messages a
  commited entry can be removed.  Need to write testcase and fix.

- Only near the end we looked at the TLA spec, and it is very helpful
  in understanding.  Implementation should move closer to it.

- the persistence of RaftServer has not been tested, and in fact
  clearly doesn't work.  The persisted attributes need to be saved on
  every change.  Maybe make them properties that call persist() on any
  change.  (for the log it has been tested)

- the set of types of tests seem to provide good coverage of the
  things you want tested, but with pressure to get some large part of
  the algorithm working didn't work too hard on getting all cases
  tested.  Code coverage could help with getting all cases.

- many lots start with state.server_str to show which server is doing
  the logging.  This info can be provided in thread local vars, but
  then how to get it into the log statement?  Could make all log
  statements shorter.

- cluster membership changes and log compaction are two important
  features we didn't try implementing at all yet.  Will be a nice test
  of the setup to see how pleasant it is to add.

- a more complete chaos monkey.  We will test some leader election
  scenarios, but if we have the whole thing running we want to really
  mess with it in different scenarios and see it react.

- run the whole setup in e.g. minikube so that it (at least to the
  algo looks like) doesn't fully run on localhost.  Volumes for
  persistence.

- add prometheus metrics; then in the minikube setup can run grafana
  next to it and see the performance of the algorithm.  Then
  load/speed test.

- once have membership changes, should be able to deal with a server
  permanently dying.  Create a new server with new id, and move there.
  Run through such a scenario.

- replace the hand rolled sockets by gRPC.

- replace threading by async.