// Twin of `self_rearming_timer` in tests/node_parity.rs.
function rearm(n) {
  setTimeout(() => {
    console.log(`a${n}`);
    if (n < 3) rearm(n + 1);
  }, 0);
}

async function main() {
  rearm(1);
  setTimeout(() => console.log("b"), 0);
}
main();
