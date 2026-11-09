// Twin of `spawn_from_timer` in tests/node_parity.rs.
async function main() {
  setTimeout(() => {
    console.log("timer 1");
    queueMicrotask(async () => console.log("task from timer 1"));
  }, 0);
  setTimeout(() => console.log("timer 2"), 0);
}
main();
