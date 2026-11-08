// Twin of `timeout_microtask_tick_timeout` in tests/node_parity.rs.
async function main() {
  setTimeout(() => {
    console.log("A");
    queueMicrotask(() => {
      console.log("M");
      process.nextTick(() => {
        console.log("T");
        setTimeout(() => console.log("B"), 0);
      });
    });
  }, 0);
  setTimeout(() => console.log("C"), 0);
}
main();
