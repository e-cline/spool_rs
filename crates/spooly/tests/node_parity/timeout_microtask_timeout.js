// Twin of `timeout_microtask_timeout` in tests/node_parity.rs.
async function main() {
  setTimeout(() => {
    console.log("A");
    queueMicrotask(() => {
      console.log("M");
      setTimeout(() => console.log("B"), 0);
    });
  }, 0);
  setTimeout(() => console.log("C"), 0);
}
main();
