// Twin of `sleep_between_timeouts` in tests/node_parity.rs.
async function main() {
  setTimeout(() => console.log("A"), 0);
  const s = new Promise((r) => setTimeout(r, 0));
  setTimeout(() => console.log("B"), 0);
  await s;
  console.log("main woke");
  setTimeout(() => console.log("C"), 0);
}
main();
