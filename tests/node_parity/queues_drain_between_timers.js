// Twin of `queues_drain_between_timers` in tests/node_parity.rs.
async function main() {
  setTimeout(() => {
    console.log("timer 1");
    queueMicrotask(() => console.log("microtask from timer 1"));
    process.nextTick(() => console.log("tick from timer 1"));
  }, 0);
  setTimeout(() => console.log("timer 2"), 0);
}
main();
