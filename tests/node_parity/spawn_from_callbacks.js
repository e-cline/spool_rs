// Twin of `spawn_from_callbacks` in tests/node_parity.rs.
async function main() {
  queueMicrotask(() => queueMicrotask(async () => console.log("task from microtask")));
  queueMicrotask(async () => console.log("task from main"));
}
main();
