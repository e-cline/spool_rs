// Twin of `sync_microtask_timeout` in tests/node_parity.rs.
async function main() {
  setTimeout(() => console.log("timeout"), 0);
  queueMicrotask(() => console.log("microtask"));
  console.log("sync");
}
main();
