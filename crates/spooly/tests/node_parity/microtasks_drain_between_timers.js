// Twin of `microtasks_drain_between_timers` in tests/node_parity.rs.
async function main() {
  setTimeout(() => {
    console.log("timer 1");
    queueMicrotask(() => {
      console.log("microtask from timer 1");
      queueMicrotask(() => console.log("nested microtask"));
    });
  }, 0);
  setTimeout(() => console.log("timer 2"), 0);
}
main();
