// Twin of `join_chain` in tests/node_parity.rs.
//
// A Spooly `JoinHandle` maps to a deferred promise that the task resolves as
// its last action; awaiting the handle is `await deferred.promise`.
function deferred() {
  let resolve;
  const promise = new Promise((r) => (resolve = r));
  return { promise, resolve };
}

async function main() {
  queueMicrotask(async () => {
    for (let i = 0; i < 6; i++) {
      console.log(`s${i}`);
      await null;
    }
  });
  const a = deferred();
  queueMicrotask(async () => {
    console.log("A start");
    const b = deferred();
    queueMicrotask(async () => {
      console.log("B start");
      const c = deferred();
      queueMicrotask(async () => {
        console.log("C");
        c.resolve(3);
      });
      const v = await c.promise;
      console.log("B end");
      b.resolve(v + 2);
    });
    const v = await b.promise;
    console.log("A end");
    a.resolve(v + 1);
  });
  const v = await a.promise;
  console.log(`main end ${v}`);
}
main();
