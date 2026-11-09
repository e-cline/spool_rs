// Twin of `plan_example` in tests/node_parity.rs.
function deferred() {
  let resolve;
  const promise = new Promise((r) => (resolve = r));
  return { promise, resolve };
}

async function main() {
  setTimeout(() => console.log("timeout 40ms"), 40);
  queueMicrotask(() => console.log("microtask"));
  console.log("sync");
  const h = deferred();
  queueMicrotask(async () => {
    await new Promise((r) => setTimeout(r, 10));
    h.resolve(42);
  });
  console.log(`joined: ${await h.promise}`);
}
main();
