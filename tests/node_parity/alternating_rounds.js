// Twin of `alternating_rounds` in tests/node_parity.rs.
function m(depth) {
  queueMicrotask(() => {
    console.log(`m${depth}`);
    if (depth < 3) {
      k(depth + 1);
      queueMicrotask(() => console.log(`m${depth}b`));
    }
  });
}

function k(depth) {
  process.nextTick(() => {
    console.log(`t${depth}`);
    if (depth < 3) {
      m(depth + 1);
      process.nextTick(() => console.log(`t${depth}b`));
    }
  });
}

async function main() {
  m(0);
  k(0);
}
main();
